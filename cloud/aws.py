import boto
from boto.ec2.connection import EC2Connection
from fabric.api import env, execute, run
from fabric.colors import green
from fabric.contrib.files import sed
from . import ip_address, pretty_instance
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
    if verify_env_contains_keys(['aws_access_key_id','aws_secret_access_key','aws_ec2_region']):
        if "aws_ec2_ssh_key" in env and env.key_filename == None:
            error("EC2 SSH key name specified (%s) but no path to key file provided with -i parameter. Either provide both or neither (in which case a temporary one will be generated)." % env.aws_ec2_ssh_key)
            return False
        elif ("aws_ec2_ssh_key" not in env) and env.key_filename != None:
            error("EC2 SSH key name not not specified but path to key file provided with -i parameter. Either provide both or neither (in which case a temporary one will be generated).")
            return False
        env.aws_ec2_security_groups = [env.aws_ec2_security_group] if 'aws_ec2_security_group' in env else None
        env.aws_ec2_security_group_ids = [env.aws_ec2_security_group_id] if 'aws_ec2_security_group_id' in env else None
        env.user=env['ec2_ami_user'] # Force SSH via the configured user for our AMI rather than local user identified by $USER
        env.provider_instance_function = _ec2_instances_
        env.provider_decommission_function = _decommission_ec2_nodes_
        env.provider_provision_function = _provision_ec2_nodes_
        # By default, assume /etc/hosts needs munging if in VPC
        munge_by_default = 'aws_ec2_subnet_id' in env
        if ('aws_ec2_munge_etc_hosts' in env and env.aws_ec2_munge_etc_hosts) or munge_by_default:
            env.provider_post_provision_hook = _munge_etc_hosts_
        debug("AWS access configured. EC2 SSH as %s using key %s" % (env.user, env.key_filename[0] if env.key_filename else "<to be created>"))
        return True
    else:
        return False

def _region_():
    return boto.ec2.get_region(env.aws_ec2_region, aws_access_key_id=env.aws_access_key_id, aws_secret_access_key=env.aws_secret_access_key)

def _connect_():
    return EC2Connection(env.aws_access_key_id, env.aws_secret_access_key, region=_region_())

# Adapted from https://github.com/garethr/cloth/blob/master/src/cloth/utils.py
def _ec2_instances_():
    "Use the EC2 API to get a list of all machines"
    reservations = _region_().connect(aws_access_key_id=env.aws_access_key_id, aws_secret_access_key=env.aws_secret_access_key).get_all_instances()
    instances = []
    for reservation in reservations:
        instances += reservation.instances
    return instances


def create_ec2_key_pair():
    env.aws_ec2_ssh_key = "log_parse_%d_%d" % (int(time.time()), int(1000*random.random()))
    key_pair = _connect_().create_key_pair(env.aws_ec2_ssh_key)
    # Caution: key file left dangling around on disk unless delete_ec2_key_pair is called later on.
    env.key_filename = os.path.join(tempfile.mkdtemp(), env.aws_ec2_ssh_key + ".pem")
    key_pair.save(os.path.dirname(env.key_filename))
    info("Created temporary EC2 key pair '%s' in '%s'" % (env.aws_ec2_ssh_key, env.key_filename))


def _provision_ec2_nodes_(num, next_id):
    "Provision and return num nodes, after verifying that they are running."
    # future: region support. Do boto.ec2.regions(), find the one you want in the result, and use it's connect method.

    if not "aws_ec2_ssh_key" in env:
        create_ec2_key_pair()

    new_reservations = _connect_().run_instances(
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

def _wait_for_ec2_provisioning_(new_node, platform, role, identifier, timeout_secs=180):
    "Waits for instance to come online, applies name to it (using Cloth naming convention)"
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
    # We're using instance-store backed hosts, and they cannot be stopped, only terminated.
    _connect_().terminate_instances([node.id for node in env.nodes])

def delete_ec2_key_pair():
    _connect_().delete_key_pair(env.aws_ec2_ssh_key)
    # Trash the whole temp directory.
    for root, dirs, files in os.walk(os.path.dirname(env.key_filename)):
        for f in files:
            os.remove(os.path.join(root, f))
    os.removedirs(os.path.dirname(env.key_filename))
    info("Deleted temporary EC2 key pair '%s'" % (env.aws_ec2_ssh_key))

def find_orphan_ec2_nodes():
    "Assumes any running instance using a temporary log_parse_ key pair is an orphan. Use with caution."
    reservations = _connect_().get_all_instances()
    instances = [i for r in reservations for i in r.instances]
    #instances = filter(lambda i: i.state == 'running' and i.image_id == 'ami-9a873ff3' and i.launch_time > '2012-12-22T00:54' and i.launch_time < '2012-12-22T06:58' , instances)
    instances = filter(lambda i: i.key_name.startswith("log_parse_") and i.state=='running',instances)
    if instances:
        warn("Possible orphan instances: %s" % [i.id for i in instances])
    for i in instances:
        print("%s %s %s %s %s" % (pretty_instance(i), i.launch_time,i.state,i.instance_type,i.dns_name))
    return instances

def assign_elastic_ip(ip_address, node):
    if ip_address == ip_address(node):
        debug("ElasticIP %s already assigned to %s" % (env.elastic_ip, pretty_instance(node)))
    else:
        info("Assigning ElasticIP %s to %s" % (env.elastic_ip, pretty_instance(node)))
        _connect_().associate_address( node.id,ip_address)
