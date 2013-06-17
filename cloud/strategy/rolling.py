"""
Helpers for a rolling provisioning strategy in which new deployments go to new hosts rather than being applied in-place
to existing ones. This strategy uses a target cluster size and a first-in-first-out strategy based on sequential node
ids. Three operating modes are supported: Simple, Virtual IP, and Load-Balancer.
"""

from fabric.api import env, task, runs_once
from fabric.colors import cyan,green,magenta,red
from fabric.contrib.console import confirm
from fabulous import debug,info,retry
from fabulous.cloud import decommission_nodes,id_of,instances_with_platform_and_role,lb_add_nodes,lb_get_nodes,lb_remove_nodes,lb_specified,provision_nodes,show,use,use_only,virtual_ip_get_node,virtual_ip_specified
from fabulous.config import configure

ACTIVE,EXTRA,INACTIVE,ORPHAN = ['ACTIVE','EXTRA','INACTIVE','ORPHAN']
MAX_ID = "MAX_ID"

@task(name="list")
@runs_once
def list_nodes():
    """List all nodes."""
    configure()
    print("")

    nodes = classify_nodes()

    if nodes[INACTIVE]:
        print("")
        print(magenta("** INACTIVE nodes **"))
        if lb_specified():
            print(magenta("INACTIVE nodes are nodes not behind the load balancer."))
        elif virtual_ip_specified():
            print(magenta("INACTIVE nodes are nodes not behind the virtual IP."))
        else:
            print(magenta("INACTIVE nodes are nodes in excess of the target cluster size."))
        if len(nodes[ORPHAN]) > 0:
            print(magenta("To decommission INACTIVE and ORPHAN nodes, use the prune task."))
        else:
            print(magenta("To decommission these nodes, use the prune task."))
        show(nodes[INACTIVE])

    if nodes[ORPHAN]:
        print("")
        print(red("** ORPHAN nodes **"))
        if lb_specified():
            print(red("ORPHAN nodes are nodes not behind the load balancer but ought to be according to target cluster size and node id sequence."))
        elif virtual_ip_specified():
            print(red("ORPHAN nodes are nodes not behind the virtual IP but ought to be according to target cluster size and node id sequence."))
        if nodes[INACTIVE]:
            print(red("To decommission INACTIVE and ORPHAN nodes, use the prune task."))
        else:
            print(red("To decommission these nodes, use the prune task."))
        show(nodes[ORPHAN])

    if nodes[EXTRA]:
        print(cyan("** EXTRA nodes **"))
        print(cyan("EXTRA nodes are nodes in excess of the target cluster size but live behind the load balancer."))
        print(cyan("To decommission these nodes, use the scale_down task."))
        show(nodes[EXTRA])

    if nodes[ACTIVE]:
        print("")
        print(green("** ACTIVE nodes **"))
        if lb_specified():
            print(green("ACTIVE nodes are nodes behind the load balancer and current according to target cluster size and node id sequence."))
        elif virtual_ip_specified():
            print(green("ACTIVE nodes are nodes behind the virtual IP and current according to the node id sequence."))
        show(nodes[ACTIVE])

    if nodes[ACTIVE] or nodes[ORPHAN] or nodes[EXTRA] or nodes[INACTIVE]:
        print("")
        print("To decommission all nodes, use the teardown task.")
        print("To connect to a node, use:")
        print(green("ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i %s %s@<ip address>" % (env.key_filename[0], env.user)))
    else:
        print(cyan("There are no nodes in the cluster."))


@task(name="provision")
@runs_once
def provision():
    """Provisions env.num_nodes new nodes."""
    configure()
    use_only()
    env.new_nodes = provision_nodes(env.num_nodes, classify_nodes()[MAX_ID] + 1)

@task(name="prune")
@runs_once
def decommission_unused():
    """De-provisions inactive and orphan nodes."""
    configure()
    nodes = classify_nodes()
    use_only(*nodes[INACTIVE] + nodes[ORPHAN])
    if len(env.nodes) == 0:
        info("There are no inactive or orphan nodes to decommission.")
    else:
        decommission_nodes()

@task(name="teardown")
@runs_once
def decommission_all():
    """De-provisions all nodes."""
    configure()
    use_only(*instances_with_platform_and_role(env.platform, env.role))
    if len(env.nodes) == 0:
        info("There are no nodes to decommission.")
    else:
        show()
        if confirm("Are you sure you want to decommission ALL %d nodes? [y/N] " % len(env.nodes), default=False):
            decommission_nodes()

@task(name="scale_down")
@runs_once
def scale_down():
    """Removes excess nodes from the load balancer (thus extra nodes are made into inactive nodes)."""
    use_only(*classify_nodes()[EXTRA])
    if len(env.nodes) == 0:
        info("There are no extra nodes to scale down.")
    else:
        lb_remove_nodes()

def _sort_nodes_(nodes):
    return sorted(nodes, key = id_of) # sequential ids means oldest node to newest

#def all_nodes():
#    return _sort_nodes_(instances_with_platform_and_role(env.platform, env.role))

def classify_nodes():
    # Confusion warning: node.id is platform-role-unique_identifier, id_of(node) is unique_identifier.
    # For rolling strategy, unique identifers are sequential
    cluster_nodes = instances_with_platform_and_role(env.platform, env.role)
    cluster_node_ids = set([node.id for node in cluster_nodes])
    # max gets grumpy if only 1 argument. Hence two zeros to handle case when cluster is empty.
    max_seq_number = max(0, 0, *[id_of(node) for node in cluster_nodes])

    if lb_specified():
        live_nodes = lb_get_nodes()
    elif virtual_ip_specified():
        node = virtual_ip_get_node()
        live_nodes = [node] if node else []
    else:
        live_nodes = cluster_nodes
    live_node_ids = set([node.id for node in live_nodes])
    live_nodes_not_in_cluster = [node for node in live_nodes if not node.id in cluster_node_ids]

    # reversed sequential ids means newest to oldest
    all_nodes = reversed(sorted(live_nodes_not_in_cluster + cluster_nodes, key = id_of))
    active_nodes = []
    extra_nodes = []
    orphan_nodes = []
    inactive_nodes = []
    for node in all_nodes:
        if node.id in live_node_ids:
            if node.id in cluster_node_ids and len(active_nodes) < env.num_nodes:
                # ACTIVE:
                #   live behind load balancer / virtual IP (if applicable)
                #   in the right platform / role
                #   still current per desired cluster size and FIFO order
                active_nodes.append(node)
            else:
                # EXTRA:
                #   live behind the load balancer (thus no extras in Simple or Virtual IP modes)
                #   ANY platform / role
                #   NOT current per desired cluster size and FIFO order
                if lb_specified():
                    extra_nodes.append(node)
                else:
                    # INACTIVE:
                    #   NOT behind the load balancer / virtual IP (if applicable)
                    #   in the right platform /role
                    #   NOT current per desired cluster size and FIFO order
                    inactive_nodes.append(node)
        else:
            # Node not live. All nodes being processed are in cluster or live, so not live implies in cluster
            if len(active_nodes) + len(orphan_nodes) < env.num_nodes:
                # ORPHAN:
                #   NOT behind load balancer / virtual IP (thus no orphans in Simple mode)
                #   in the right platform / role
                #   still current per desired cluster size and FIFO order
                orphan_nodes.append(node)
            else:
                inactive_nodes.append(node)

    return {
        ACTIVE : [node for node in reversed(active_nodes)],
        EXTRA : [node for node in reversed(extra_nodes)],
        INACTIVE : [node for node in reversed(inactive_nodes)],
        ORPHAN : [node for node in reversed(orphan_nodes)],
        MAX_ID : max_seq_number
    }

@task(name="active")
@runs_once
def use_active_nodes():
    """Operate on active nodes: current per cluster id sequence, behind load balancer / virtual IP."""
    for node in classify_nodes()[ACTIVE]:
        use(node)

@task(name="extra")
@runs_once
def use_extra_nodes():
    """Operate on extra nodes: not current per cluster id sequence, but behind load balancer."""
    for node in classify_nodes()[EXTRA]:
        use(node)

@task(name="inactive")
@runs_once
def use_inactive_nodes():
    """Operate on inactive nodes: not current per cluster id sequence, not behind load balancer / virtual IP."""
    for node in classify_nodes()[INACTIVE]:
        use(node)

@task(name="orphan")
@runs_once
def use_orphan_nodes():
    """Operate on orphan nodes: current per cluster id sequence, but not behind load balancer / virtual IP."""
    for node in classify_nodes()[ORPHAN]:
        use(node)
