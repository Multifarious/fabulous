from fabric.api import env, task, runs_once
from fabric.colors import cyan,green
from fabric.contrib.console import confirm
from fabulous import debug,info,retry
from fabulous.cloud import decommission_nodes,id_of,instances_with_platform_and_role,provision_nodes,show,use_only
from fabulous.config import configure

# Helpers for a rolling provisioning strategy in which new deployments go to new hosts rather than being applied in-place to existing ones.


@task(name="list")
@runs_once
def list_nodes():
    """List all nodes."""
    configure()
    print("")
    print("** ALL nodes **")
    use_all_nodes()
    show()
    use_inactive_nodes()
    if len(env.nodes) > 0:
        print("")
        print(cyan("** INACTIVE nodes **"))
        show()
        print(cyan("To decommission these nodes, use the prune task."))

    use_active_nodes()
    if len(env.nodes) > 0:
        print("")
        print(green("** ACTIVE nodes **"))
        show()
        print("To decommission all nodes, use the teardown task.")
        print("To connect to a node, use:")
        print(green("ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i %s %s@<ip address>" % (env.key_filename[0], env.user)))


@task(name="provision")
@runs_once
def provision():
    """Provisions new cluster of nodes."""
    configure()
    last_id = use_all_nodes()
    use_only([])
    env.new_nodes = provision_nodes(env.num_nodes, last_id + 1)

@task(name="prune")
@runs_once
def decommission_unused():
    """De-provisions nodes no longer part of the active cluster."""
    configure()
    use_inactive_nodes()
    if len(env.nodes) == 0:
        info("There are no inactive nodes to decommission.")
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
        if confirm("Are you sure you want to decommission these %d nodes? [y/N] " % len(env.nodes), default=False):
            decommission_nodes()

def use_inactive_nodes():
    "Uses only nodes that are inactive in the cluster. Membership is simply based on expected cluster size and sequential node id."
    use_all_nodes()
    if len(env.nodes) > env.num_nodes:
        use_only(env.nodes[0:-env.num_nodes])
    else:
        use_only([])

def use_active_nodes():
    "Uses only nodes that are active in the cluster. Membership is simply based on expected cluster size and sequential node id."
    use_all_nodes()
    if len(env.nodes) > env.num_nodes:
        use_only(env.nodes[-env.num_nodes:])

def use_all_nodes():
    "Uses all existing nodes, returns max id of running nodes (or 0 if none)"
    all_nodes = sorted(instances_with_platform_and_role(env.platform, env.role), key = id_of)
    use_only(all_nodes)
    if len(all_nodes) > 0:
        return max([id_of(node) for node in all_nodes])
    else:
        return 0
