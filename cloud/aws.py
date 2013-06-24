from boto import ec2
from boto.ec2 import elb
from boto.exception import BotoServerError
from fabric.api import env, execute, run, sudo
from fabric.colors import green
from fabric.contrib.console import confirm
from fabric.contrib.files import append,sed
from . import ip_address, pretty_instance, show
from .. import debug, error, info, warn
from ..config import verify_env_contains_keys
import os
import random
import tempfile
import time

def is_ec2():
    return "provider" in env and env.provider == "ec2"

def aws_config():
    "See fabulous.config"
    if not is_ec2():
        return None
    if verify_env_contains_keys('aws_access_key_id','aws_secret_access_key','aws_ec2_region'):
        if "aws_ec2_ssh_key" in env and env.key_filename == None:
            error("EC2 SSH key name specified (%s) but no path to key file provided with -i parameter. Either provide both or neither (in which case a temporary one will be generated)." % env.aws_ec2_ssh_key)
            return False
        elif ("aws_ec2_ssh_key" not in env) and env.key_filename != None:
            error("EC2 SSH key name not not specified but path to key file provided with -i parameter. Either provide both or neither (in which case a temporary one will be generated).")
            return False

        if len(filter(lambda x: x, [_is_elasticip_specified_(), _is_secondary_ip_specified_(), _is_elb_specified_()])) > 1:
            error("Cannot specify more than one of: Elastic IP, VPC Secondary IP, Elastic Load Balancer.")
            return False
        elif _is_elasticip_specified_():
            debug("Using Elastic IP " + env.elastic_ip)
        elif _is_secondary_ip_specified_():
            if 'secondary_ip_cidr_prefix_size' in env:
                debug("Using VPC Secondary IP " + env.secondary_ip)
            else:
                error("When using a VPC Secondary IP, secondary_ip_cidr_prefix_size must be specified as well.")
                return False
        elif _is_elb_specified_():
            debug("Using Elastic Load Balancer " + env.aws_elb_name)

        env.aws_ec2_security_groups = [env.aws_ec2_security_group] if 'aws_ec2_security_group' in env else None
        env.aws_ec2_security_group_ids = [env.aws_ec2_security_group_id] if 'aws_ec2_security_group_id' in env else None
        env.user=env['ec2_ami_user'] # Force SSH via the configured user for our AMI rather than local user identified by $USER
        env.provider_instance_function = _ec2_instances_
        env.provider_decommission_function = _decommission_ec2_nodes_
        env.provider_provision_function = _provision_ec2_nodes_
        env.provider_virtual_ip_is_specified_function = _is_virtual_ip_specified_
        env.provider_virtual_ip_membership_function = _get_virtual_ip_node_
        env.provider_virtual_ip_assign_function = _assign_virtual_ip_
        env.provider_load_balancer_is_specified_function = _is_elb_specified_
        env.provider_load_balancer_membership_function = _enumerate_elb_members_
        env.provider_load_balancer_add_nodes_function = _assign_to_elb_
        env.provider_load_balancer_remove_nodes_function = _unassign_from_elb_
        # By default, assume /etc/hosts needs munging if in VPC
        munge_by_default = 'aws_ec2_subnet_id' in env
        if ('aws_ec2_munge_etc_hosts' in env and env.aws_ec2_munge_etc_hosts) or munge_by_default:
            env.provider_post_provision_hook = _munge_etc_hosts_
        debug("AWS access configured. EC2 SSH as %s using key %s" % (env.user, env.key_filename[0] if env.key_filename else "<to be created>"))
        return True
    else:
        return False

def connect(region = None):
    """Return a boto EC2Connection using credentials specified in env. Connects to env.aws_ec2_region unless otherwise specified.
    Use directly for AWS-specific tweaking not supported by other fabulous functionality."""
    region = region or env.aws_ec2_region
    return ec2.connect_to_region(region, aws_access_key_id=env.aws_access_key_id, aws_secret_access_key=env.aws_secret_access_key)

def connect_elb(region = None):
    """Return a boto ELBConnection using credentials specified in env.  Connects to env.aws_ec2_region unless otherwise specified.
    Use directly for AWS-specific tweaking not supported by other fabulous functionality."""
    region = region or env.aws_ec2_region
    return elb.connect_to_region(region, aws_access_key_id=env.aws_access_key_id, aws_secret_access_key=env.aws_secret_access_key)

def _ec2_instances_by_id():
    "Use the EC2 API to get a list of all machines, return dict keyed by id."
    reservations = connect().get_all_instances()
    instances = {}
    for reservation in reservations:
        for instance in reservation.instances:
            instances[instance.id] = instance
    return instances

# Adapted from https://github.com/garethr/cloth/blob/master/src/cloth/utils.py
def _ec2_instances_():
    "Use the EC2 API to get a list of all machines"
    reservations = connect().get_all_instances()
    instances = []
    for reservation in reservations:
        instances += reservation.instances
    return instances


def create_ec2_key_pair():
    env.aws_ec2_ssh_key = "log_parse_%d_%d" % (int(time.time()), int(1000*random.random()))
    key_pair = connect().create_key_pair(env.aws_ec2_ssh_key)
    # Caution: key file left dangling around on disk unless delete_ec2_key_pair is called later on.
    env.key_filename = os.path.join(tempfile.mkdtemp(), env.aws_ec2_ssh_key + ".pem")
    key_pair.save(os.path.dirname(env.key_filename))
    info("Created temporary EC2 key pair '%s' in '%s'" % (env.aws_ec2_ssh_key, env.key_filename))


def _provision_ec2_nodes_(num, next_id):
    "Provision and return num nodes, after verifying that they are running."

    if not "aws_ec2_ssh_key" in env:
        create_ec2_key_pair()

    new_reservations = connect().run_instances(
        env.ec2_ami,
        min_count=num,
        max_count=num,
        key_name=env.aws_ec2_ssh_key,
        instance_type=env.ec2_instance_type,
        security_groups=env.aws_ec2_security_groups,
        security_group_ids=env.aws_ec2_security_group_ids,
        subnet_id=env.aws_ec2_subnet_id if 'aws_ec2_subnet_id' in env else None)
    new_nodes = new_reservations.instances
    info("Provisioning node(s) %s" % ", ".join([node.id for node in new_nodes]))
    new_nodes_with_ids = zip(new_nodes,range(next_id, len(new_nodes)+next_id))

    return map(lambda node_and_id: _wait_for_ec2_provisioning_(node_and_id[0], env.platform, env.role, str(node_and_id[1])), new_nodes_with_ids)

def _wait_for_ec2_provisioning_(new_node, platform, role, identifier):
    """Waits for instance to come online, applies name to it (using Cloth naming convention)"""
    if env.provisioning_timeout:
       timeout_secs = env.provisioning_timeout
    else:
        debug('Default provisioning timeout of 180s will be used for provisioning; set provisioning_timeout in the fabricrc file for a longer or shorter timeout.')
        timeout_secs = 180

    timeout = time.time() + timeout_secs
    while (new_node.state != 'running'):
        if time.time() > timeout:
            raise RuntimeError("Timeout waiting for %s to be provisioned." % (pretty_instance(new_node)))
        debug("Waiting for %s to come online. Currently '%s'" % (new_node.id, new_node.state))
        time.sleep(5)
        new_node.update()

    new_node.add_tag('Name', "%s-%s-%s" % (platform, role, identifier))
    new_node.update()
    info("%s is provisioned." % pretty_instance(new_node))
    print(green("ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i %s %s@%s" % (env.key_filename[0], env.user, ip_address(new_node))))
    return new_node

def _munge_etc_hosts_():
    """Add hostname as name for 127.0.0.1 to /etc/hosts.
    Ubuntu AMIs inside VPC annoyingly log 'unable to resolve host ip-www-xxx-yyy-zzz'
    on every sudo invocation; fix by adding configured hostname to /etc/hosts."""
    execute(_munge_etc_hosts_delegate_)

def _munge_etc_hosts_delegate_():
    hostname = run("hostname").strip()
    sed('/etc/hosts', '127.0.0.1 localhost', '127.0.0.1 localhost %s' % hostname, use_sudo=True)

def _decommission_ec2_nodes_():
    node_ids = {}
    for node in env.nodes:
        node_ids[node.id] = node
    ok = True
    try:
        for elb in connect_elb().get_all_load_balancers():
            for instance_info in elb.instances:
                if instance_info.id in node_ids:
                    warn("%s is one of %d instances behind Elastic Load Balancer %s" % (node_ids[instance_info.id], len(elb.instances), elb.name))
                    ok = False
        if not ok:
            error("Decommissioning aborted because one or more nodes were behind load balancers.")
    except BotoServerError, e:
        warn("Unable to query AWS for Elastic Load Balancer information: %s" % e.error_message)
        show(env.nodes)
        ok = confirm("Cannot guarantee that none of the nodes are behind an ELB. Continue?", default=False)

    if ok:
        # instance-store backed hosts cannot be stopped, only terminated.
        connect().terminate_instances([node.id for node in env.nodes])

def delete_ec2_key_pair():
    connect().delete_key_pair(env.aws_ec2_ssh_key)
    # Trash the whole temp directory.
    for root, dirs, files in os.walk(os.path.dirname(env.key_filename)):
        for f in files:
            os.remove(os.path.join(root, f))
    os.removedirs(os.path.dirname(env.key_filename))
    info("Deleted temporary EC2 key pair '%s'" % (env.aws_ec2_ssh_key))


def _is_elasticip_specified_():
    return "elastic_ip" in env

# TODO: rename to private _assign_to_elb_ and update clients to go through cloud.__init__'s virtual_ip_assign() function
def assign_elastic_ip(node = None, elastic_ip=None):
    """Assigns elastic IP address to node.
    :param: node to assign elastic IP to or None for env.nodes[0]

    :type: str
    :param: Elastic IP address to assign or None for env.elastic_ip
    """
    node = node or env.nodes[0]
    elastic_ip = elastic_ip or env.elastic_ip
    if elastic_ip == ip_address(node):
        debug("ElasticIP %s already assigned to %s" % (elastic_ip, pretty_instance(node)))
    else:
        info("Assigning ElasticIP %s to %s" % (elastic_ip, pretty_instance(node)))
        connect().associate_address(node.id, elastic_ip)

def _get_elastic_ip_node_():
    """Looks through all nodes to find which, if any, holds the ElasticIP."""
    all_instances = _ec2_instances_()
    for instance in all_instances:
        if ip_address(instance) == env.elastic_ip:
            return instance
    return None

def _is_secondary_ip_specified_():
    return "secondary_ip" in env

def _assign_secondary_ip_():
    """Assigns secondary IP address to node's first interface.
    :param: node to assign secondary IP to or None for env.nodes[0]
    """
    interface_idx = 0
    node = env.nodes[0]
    cidr='%s/%s' % (env.secondary_ip,env.secondary_ip_cidr_prefix_size)

    if (_get_secondary_ip_node_().id == node.id):
        debug("VPC Secondary IP %s already assigned to %s" % (cidr, pretty_instance(node)))
    else:
        info("Assigning VPC Secondary IP %s to %s" % (cidr, pretty_instance(node)))
        connect().assign_private_ip_addresses(node.interfaces[interface_idx].id, env.secondary_ip, allow_reassignment=True)
        # Notify opsys that it has a new address (This seems to only happen automatically with Elastic IPs). Write to /etc to make persistent.
        has_address = run('ip addr | grep %s' % cidr, quiet=True)
        if not has_address:
            sudo('ip addr add %s dev eth0' % cidr)
            append('/etc/network/interfaces','up ip addr add %s dev eth%d' % (cidr,interface_idx),use_sudo=True)

def _get_secondary_ip_node_():
    """Looks through all nodes to find which, if any, holds the Secondary IP."""
    all_instances = _ec2_instances_()
    for instance in all_instances:
        for interface in instance.interfaces:
            for address in interface.private_ip_addresses:
                if address.private_ip_address == env.secondary_ip and not address.primary:
                    return instance
    return None

def _is_virtual_ip_specified_():
    "Gloss over Elastic IP vs. secondary IP distinction."
    return _is_elasticip_specified_() or _is_secondary_ip_specified_()

def _assign_virtual_ip_():
    "Gloss over Elastic IP vs. secondary IP distinction."
    if _is_elasticip_specified_():
        return assign_elastic_ip()
    else:
        return _assign_secondary_ip_()

def _get_virtual_ip_node_():
    "Gloss over Elastic IP vs. secondary IP distinction."
    if _is_elasticip_specified_():
        return _get_elastic_ip_node_()
    else:
        return _get_secondary_ip_node_()

def _find_elb_(elb_name=None):
    elb_name = elb_name or env.aws_elb_name
    elb = connect_elb().get_all_load_balancers([elb_name])
    if elb and len(elb) == 1:
        return elb[0]
    else:
        error("Cannot locate ELB %s. Known load balancers are: %s" % (
            elb_name,
            [elb.name for elb in connect_elb().get_all_load_balancers()]
        ))
        return None

def _is_elb_specified_():
    return "aws_elb_name" in env

def _assign_to_elb_(elb_name = None, nodes = None):
    """Adds nodes to the the Elastic Load Balancer.
    :type: list
    :param: Nodes to assign or None for env.nodes

    :type: str
    :param: DNS name of ELB or None for env.aws_elb_name.
    """
    nodes = nodes or env.nodes
    elb = _find_elb_(elb_name)
    if elb:
        info("Adding %s to ELB %s" % ([pretty_instance(node) for node in nodes], elb_name))
        elb.register_instances([node.id for node in nodes])


def _unassign_from_elb_(elb_name=None, nodes = None):
    """Removes nodes from the Elastic Load Balancer.
    :type: list
    :param: Nodes to assign or None for env.nodes

    :type: str
    :param: DNS name of ELB or None for env.aws_elb_name.
    """
    nodes = nodes or env.nodes
    elb = _find_elb_(elb_name)
    if elb:
        info("Removing %s from ELB %s" % ([pretty_instance(node) for node in nodes], elb_name))
        elb.deregister_instances([node.id for node in nodes])

def _enumerate_elb_members_(elb_name=None):
    """Returns list of nodes behind the Elastic Load Balancer.

    :type: str
    :param: DNS name of ELB or None for env.aws_elb_name.
    """
    elb = _find_elb_(elb_name)
    result = []
    if elb:
        all_instances = _ec2_instances_by_id()
        for instance_info in elb.instances:
            instance = all_instances.get(instance_info.id)
            if instance:
                result.append(instance)
            else:
                warn("ELB %s reports member node %s, but no such instance is known." % (elb.name,instance_info.id))
    return result

