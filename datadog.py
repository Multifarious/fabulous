from dogapi import dog_http_api as api
from fabric.api import env
from fabric.contrib.files import sed
from fabric.operations import sudo
from fabulous import debug, error, git, info, retry
from fabulous.config import verify_env_contains_keys
from fabulous.cloud import pretty_instance
import re

def is_datadog_enabled():
    return 'datadog_api_key' in env and env.datadog_api_key

@retry(SystemExit)
def install_datadog_agent(datadog_api_key = None):
    """
    Installs the Datadog agent.
    :param datadog_api_key: interpreted via get_datadog_api_key
    """
    info("Installing Datadog agent.")
    # Per https://app.datadoghq.com/account/settings#agent/ubuntu
    sudo('DD_API_KEY=%s bash -c "$(wget -qO- http://dtdg.co/agent-install-ubuntu)"' % get_datadog_api_key(datadog_api_key))

@retry(SystemExit,total_tries = 8)
def add_datadog_agent_tags(datadog_tags = None):
    """
    Applies node-specific information to the Datadog agent configuration.
    :param datadog_tags: interpreted via get_datadog_tags
    """
    tags = ','.join(get_datadog_tags(datadog_tags))
    info("Updating Datadog configuration with tag(s): %s." % tags)
    sed('/etc/dd-agent/datadog.conf', '^[# ]?tags.*', 'tags: %s' % tags , use_sudo=True)
    sudo('service datadog-agent restart')

def record_deployment(artifact_description = None, node_description = None, datadog_api_key = None, datadog_tags = None):
    """
    Records Datadog event indicating deployment occurred. Git SHA included.
    :param artifact_description: defaults to git.get_sha()
    :param node_description: defaults to pretty_instance()
    :param datadog_api_key: interpreted via get_api_key
    :param datadog_tags: interpreted via get_datadog_tags
    """
    artifact_description = artifact_description or 'Git SHA %s' % git.get_sha()
    node_description = node_description or pretty_instance()
    message = '%s deployed to %s' % (artifact_description, node_description)
    debug("Recording in Datadog: %s"  % message)
    api.api_key = get_datadog_api_key(datadog_api_key)
    api.event_with_response("Deployment", message, tags=get_datadog_tags(datadog_tags))

def get_datadog_api_key(datadog_api_key = None):
    if not datadog_api_key:
        verify_env_contains_keys('datadog_api_key')
        datadog_api_key = env.datadog_api_key
    return datadog_api_key

def get_datadog_tags(datadog_tags = None):
    """Returns provided datadog_tags or, if not provided, loads from env.datadog_tags
   Accepts space and comma delimited tags and returns as list.
    """
    if datadog_tags is None:
        datadog_tags = env.datadog_tags if 'datadog_tags' in env else []
    if datadog_tags == '':
        datadog_tags = []
    return re.split('[, ]', datadog_tags) if isinstance(datadog_tags, basestring) else datadog_tags
