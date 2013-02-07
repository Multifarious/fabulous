from fabric.api import env
from .import pretty_instance
from .. import debug, error, info, run_and_return_result, warn
from ..config import verify_env_contains_keys
import json
from os.path import expanduser

# Should probably use Google's Python API rather than invoking gcutil. The GoogleComputeEngineInstance shim
# to make the results look like boto objects is a bit flimsy.

def is_gce():
    return "provider" in env and env.provider == "google_compute_engine"

def maybe_config():
    "See fabulous.cloud.config"
    return _gce_config_ if is_gce() else None

class GoogleComputeEngineInstance:
    def __init__(self,gcutil_response):
        self.id = gcutil_response["id"]
        self.ip_address = gcutil_response.get("networkInterfaces")[0].get("accessConfigs")[0].get("natIP") # TODO: do not assume first NAT'd IP of first NIC is present or the right one to use
        self.tags = {"Name" : gcutil_response["name"]}

    def __str__(self):
        return "<Google Compute Engine node '%s' @ '%s'>" % (self.tags.get("Name"), self.ip_address)

    @property
    def tags(self):
        return self.tags

    @property
    def ip_address(self):
        return self.ip_address

    @property
    def id(self):
        return self.id

def _gce_config_():
    """Verify Google Compute Engine properly configured."""
    result = verify_env_contains_keys(['google_storage_access_key','google_storage_secret_key'])
    if result:
        code,stdout,stderr = run_and_return_result(['gcutil','auth','--just_check_auth'])
        if code == 0:
            code,stdout,stderr = run_and_return_result(['gcutil','getproject','--format=json'])
            if code == 0:
                project = json.loads(stdout)
                env.google_project_name = project.get('name')
                if env.google_project_name:
                    env.key_filename = expanduser('~/.ssh/google_compute_engine')
                    debug("Google Compute Engine configured for project '%s' and SSH keyfile %s" % (env.google_project_name,env.key_filename))
                    env.user=env['gce_user'] # Force SSH vis the configured user for our AMI rather than local user identified by $USER
                    env.provider_instance_function = _google_compute_engine_instances_
                    env.provider_decommission_function = _decommission_gce_nodes_
                    env.provider_provision_function = _provision_gce_nodes_
                    return True
                else:
                    error("gcutil not properly configured. Appears oauth is set up but no project set. Manually run gcutil with --cache_flag_values")
                    return False

        error("gcutil execution failed!\n%s" % stderr)
        warn("gcutil must be configured and default project must be set (see --cache_flag_values) in order to proceed.")
    return False

# Introduce Google Compute Engine support
def _google_compute_engine_instances_():
    "Use the gcutil command line to get a list of all machines"
    instances = []
    code,stdout,stderr = run_and_return_result(['gcutil','listinstances','--format=json'], assert_successful=True)
    response = json.loads(stdout)
    items = response["items"]
    for item in items:
        # munge gcutil output to include data in same location as boto returns for EC2
        instance = GoogleComputeEngineInstance(item)
        instances.append(instance)
    return instances



def _provision_gce_nodes_(num,next_id):
    "Provision and return num nodes, after verifying that they are running."

    #gcutil addfirewall http2 --description="Incoming http allowed." --allowed="tcp:http"
    names = map(lambda x: "%s-%s-%d" % (env.platform, env.role, x), range(1,num+1))
    info("Provisioning nodes %s" % ", ".join(names))

    run_and_return_result(['gcutil','addfirewall','http8080','--description="Incoming http (port 8080) allowed."','--allowed=tcp:8080'], assert_successful=True)
    code,stdout,stderr = run_and_return_result(['gcutil','addinstance','--zone=%s'%env.gce_zone, '--machine_type=%s'%env.gce_machine_type,'--format=json'] + names, assert_successful=True)
    response = json.loads(stdout[stdout.find('{'):]) # Move past log messages to beginning of response
    items = filter(lambda item: item.get("kind") == "compute#instance", response["items"]) # One item in response is operation acknowledgement (kind:compute#operation), not node info
    instances = []
    for item in items:
        instance = GoogleComputeEngineInstance(item)
        instances.append(instance)
    for new_node in instances:
        info("%s is provisioned." % pretty_instance(new_node))
        print("ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i %s %s@%s" % (env.key_filename[0], env.user, new_node.ip_address))
    return instances

def _decommission_gce_nodes_():
    names = map(lambda node: node.tags.get("Name"), env.nodes)
    run_and_return_result(['gcutil','deleteinstance','-f']+names, assert_successful=True)