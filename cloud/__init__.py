from collections import defaultdict
from fabric.api import env, execute, run, task
from .. import debug,error,info,warn,retry,run_and_return_result
import re

def show():
    """Shows members of currently provisioned cluster."""
    if (len(env.nodes) > 0):
        print ("<node id> (<node name> @ <ip address>)")
        print(pretty_instances(env.nodes, "\n"))
    else:
        print("<no nodes>")
    print("")




# Some methods here are from https://github.com/garethr/cloth/blob/master/src/cloth/utils.py

## ----------- Adding and removing nodes from the Fabric environment ---------------
def use(node):
    "Add the node to the fabric environment"
    try:
        role = node.tags.get("Name").split('-')[1]
        env.roledefs[role] += [node.ip_address]
    except IndexError:
        pass
    env.nodes += [node]
    env.hosts += [node.ip_address]

def use_only(nodes):
    "Reverts any prior use(node) invocations and uses the specified nodes."
    env.roledefs = defaultdict(list)
    env.nodes = []
    env.hosts = []
    for node in nodes:
        use(node)

def unuse(node):
    "Remove specified node from the fabric environment; undoes use(node) from Cloth utils.py."
    try:
        role = role_of(node)
        env.roledefs[role] = [ip_address for ip_address in env.roledefs[role] if ip_address != node.ip_address]
    except IndexError:
        pass
    env.nodes = [other_node for other_node in env.nodes if other_node != node]
    env.hosts = [ip_address for ip_address in env.hosts if ip_address != node.ip_address]


## ------------ Node naming conventions ------------------

# Opinionated node naming convention, adopted from Cloth: <platform>-<role>-<unique-identifier>
node_name_re = re.compile("([^-]+)-([^-]+)-(\d+)")

def _node_name_piece_(node,i):
    m = node_name_re.match(node.tags["Name"])
    return m.groups()[i] if m else None

def platform_of(node):
    "Extract platform from a <platform>-<role>-<unique identifier> node name."
    return _node_name_piece_(node, 0)

def role_of(node):
    "Extract role from a <platform>-<role>-<unique identifier> node name."
    return _node_name_piece_(node, 1)

def id_of(node):
    "Extract unique identifier from a <platform>-<role>-<unique identifier> node name."
    id = _node_name_piece_(node, 2)
    return int(id) if id else None

def pretty_instance(node):
    "Format node as human-readable <instance id> (<name> @ <ip address>) String"
    return "%s (%s @ %s)" % (node.id, node.tags.get("Name"), node.ip_address )

def pretty_instances(nodes, joinWith=", "):
    return joinWith.join([pretty_instance(node) for node in nodes])

## -------------- Identifying nodes running on hosting providers ---------------
# Adapted from https://github.com/garethr/cloth/blob/master/src/cloth/utils.py to support Google Compute Engine as well

def instances():
    return [node for node in env.provider_instance_function() if node.tags and node.ip_address]


def instances_with_name(exp=".*"):
    """Return machines in cloud matching provided filter expression (defaults to all machines).
    Provider instance function should be e.g. ec2_instances, google_compute_engine_instances, etc.
    """
    expression = re.compile(exp)
    instances = []

    for node in instances():
        try:
            if expression.match(node.tags.get("Name")):
                instances.append(node)
        except TypeError: # What's this about? Still needed?
            pass
    return instances

def instances_with_role(role):
    return filter(lambda node: role_of(node) == role, instances())

## ---------------- Config handling ---------------

def provider_config(provider_config_function):
    """Verify a provider (e.g. AWS EC2 or Google Compute Engine) is specified and properly configured.
    Usage: provider_config(aws.maybe_config() or gce.maybe_config() ...) for all providers potentially to be used.
    """

    if provider_config_function:
        return provider_config_function()
    elif env.get('provider'):
        error("Cloud provider set to '%s' but no provider_config_function supplied for that provider. Cannot proceed." % env.provider)
    else:
        error("No cloud provider set. Please configure the 'provider' setting.")
    return False

## ------------------ Cluster management -----------------------
def provision_nodes(num, next_id):
    info("Provisioning %d new node(s)" % (num))
    nodes = env.provider_provision_function(num, next_id)
    use_only(nodes)
    wait_for_ssh_access()


# EC2 reports instance state as 'running' before SSH access is available.
# Delay here so downstream tasks can assume all nodes are available.
@retry(SystemExit, total_tries=8)
def wait_for_ssh_access():
    execute(connect)
    info("All nodes are now online.")

@task
def connect():
    "Verify connectivity to node"
    run("uname",quiet=True)

def decommission_nodes():
    "Stop instances"
    info("Decommissioning node(s) %s." % pretty_instances(env.nodes))
    env.provider_decommission_function()
