"""
Helpers for a rolling provisioning strategy in which new deployments go to new hosts rather than being applied in-place
to existing ones. This strategy uses a target cluster size and a first-in-first-out strategy based on sequential node
ids. Three operating modes are supported: Simple, Virtual IP, and Load-Balancer.
"""

from fabric.api import env, task, runs_once
from fabric.colors import cyan,green,magenta,red
from fabric.contrib.console import confirm
from fabulous import debug,info,retry
from fabulous.cloud import decommission_nodes,id_of,instances_with_platform_and_role,lb_add_nodes,lb_get_nodes,lb_remove_nodes,lb_specified,provision_nodes,show,use_only,virtual_ip_get_node,virtual_ip_specified
from fabulous.config import configure

@task(name="list")
@runs_once
def list_nodes():
    """List all nodes."""
    configure()
    print("")

    inactive = identify_inactive_nodes()
    orphan = identify_orphan_nodes()
    extra = identify_extra_nodes()
    active = identify_active_nodes()

    if len(inactive) > 0:
        print("")
        print(magenta("** INACTIVE nodes **"))
        if lb_specified():
            print(magenta("INACTIVE nodes are nodes not behind the load balancer."))
        elif virtual_ip_specified():
            print(magenta("INACTIVE nodes are nodes not behind the virtual IP."))
        else:
            print(magenta("INACTIVE nodes are nodes in excess of the target cluster size."))
        if len(orphan) > 0:
            print(magenta("To decommission INACTIVE and ORPHAN nodes, use the prune task."))
        else:
            print(magenta("To decommission these nodes, use the prune task."))
        show(inactive)

    if len(orphan) > 0:
        print("")
        print(red("** ORPHAN nodes **"))
        if lb_specified():
            print(red("ORPHAN nodes are nodes not behind the load balancer but ought to be according to target cluster size and node id sequence."))
        elif virtual_ip_specified():
            print(red("ORPHAN nodes are nodes not behind the virtual IP but ought to be according to target cluster size and node id sequence."))
        if len(inactive) > 0:
            print(red("To decommission INACTIVE and ORPHAN nodes, use the prune task."))
        else:
            print(red("To decommission these nodes, use the prune task."))
        show(orphan)

    if len(extra) > 0:
        print(cyan("** EXTRA nodes **"))
        print(cyan("EXTRA nodes are nodes in excess of the target cluster size but live behind the load balancer."))
        print(cyan("To decommission these nodes, use the scale_down task."))
        show(extra)

    if len(active) > 0:
        print("")
        print(green("** ACTIVE nodes **"))
        if lb_specified():
            print(green("ACTIVE nodes are nodes behind the load balancer and current according to target cluster size and node id sequence."))
        elif virtual_ip_specified():
            print(green("ACTIVE nodes are nodes behind the virtual IP and current according to the node id sequence."))
        show()

    if (inactive or orphan or extra or active):
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
    last_id = use_all_nodes()
    use_only()
    env.new_nodes = provision_nodes(env.num_nodes, last_id + 1)

@task(name="prune")
@runs_once
def decommission_unused():
    """De-provisions inactive and orphan nodes."""
    configure()
    use_only(*identify_inactive_nodes() + identify_orphan_nodes())
    if len(env.nodes) == 0:
        info("There are no inactive or orphan nodes to decommission.")
    else:
        decommission_nodes()

@task(name="teardown")
@runs_once
def decommission_all():
    """De-provisions all nodes."""
    configure()
    use_all_nodes()
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
    use_only(*identify_extra_nodes())
    if len(env.nodes) == 0:
        info("There are no extra nodes to scale down.")
    else:
        # TODO: wire in service shutdown here as well.
        lb_remove_nodes()

def _sort_nodes_(nodes):
    return sorted(nodes, key = id_of) # sequential ids means oldest node to newest

def all_nodes():
    return _sort_nodes_(instances_with_platform_and_role(env.platform, env.role))

def _identify_live_nodes_():
    """Behind load balancer or virtual IP but not necessarily of the appropriate platform and role or perhaps too old"""

def identify_active_nodes():
    # ACTIVE:
    #   behind load balancer / virtual IP (if applicable)
    #   in the right platform / role
    #   still current per desired cluster size and FIFO order
    if lb_specified() or virtual_ip_specified():
        if lb_specified():
            live_nodes = lb_get_nodes()
        else:
            node = virtual_ip_get_node()
            live_nodes = [node] if node else []

        all_in_cluster = _sort_nodes_(
            instances_with_platform_and_role(env.platform, env.role, live_nodes)
        )
    else:
        all_in_cluster = all_nodes()

    return all_in_cluster[-env.num_nodes:]

def identify_orphan_nodes():
    # ORPHAN:
    #  NOT behind load balancer / virtual IP (thus no orphans in Simple mode)
    #  in the right platform / role
    #  still current per desired cluster size and FIFO order
    if not lb_specified() and not virtual_ip_specified():
        # orphan node concept does not apply to Simple strategy
        return []
    active_nodes = identify_active_nodes()
    if not active_nodes:
        # Cannot have any orphan nodes when there are no active nodes
        return []
    # Orphan nodes are those which are newer than some of the active nodes but are not in the load balancer / virtual IP
    oldest_active_id = id_of(_sort_nodes_(active_nodes)[0])
    return [node for node in all_nodes() if id_of(node) > oldest_active_id]

def identify_extra_nodes():
    # EXTRA:
    #   behind the load balancer (thus no extras in Simple or Virtual IP modes)
    #   ANY platform / role
    #   NOT current per desired cluster size and FIFO order
    if not lb_specified():
        return []
    # Extra nodes are in the LB but no longer needed per desired cluster size and FIFO order (or in wrong platform/role!)
    active_ids = set([id_of(node) for node in identify_active_nodes()])
    return _sort_nodes_([node for node in lb_get_nodes() if not id_of(node) in active_ids])

def identify_inactive_nodes():
    # INACTIVE:
    #   NOT behind the load balancer / virtual IP (if applicable)
    #   in the right platform /role
    #   NOT current per desired cluster size and FIFO order

    not_inactive_ids = set([id_of(node) for node in identify_active_nodes() + identify_extra_nodes() + identify_orphan_nodes()])
    return [node for node in all_nodes() if not id_of(node) in not_inactive_ids]

def use_active_nodes():
    "Uses only nodes that are active in the cluster. Membership is simply based on expected cluster size and sequential node id."
    use_only(*identify_active_nodes())

def use_all_nodes():
    "Uses all existing nodes, returns max id of running nodes (or 0 if none)"
    nodes = all_nodes()
    use_only(*nodes)
    if len(nodes) > 0:
        return max([id_of(node) for node in all_nodes])
    else:
        return 0
