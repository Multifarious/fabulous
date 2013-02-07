from fabric.api import env
from cloud import instances, use_only
from . import error
import sys

# -------------- Configuration helpers ------------------
def configure(defaults, ints, provider_config_function):
    if env.get("configured") is None:
        _apply_defaults_(defaults, ints)
        if provider_config_function == None:
            error("No provider config function supplied. Is env.provider set?")
            sys.exit(1)
        elif provider_config_function():
            use_only(instances())
        else:
            sys.exit(1)

def _apply_defaults_(defaults, ints):
    "Apply default values for optional settings"
    for k in defaults:
        if not k in env:
            env[k] = defaults[k]

    # Force numeric conversions to make easier use later on
    for k in ints:
        env[k] = int(env[k])

def verify_env_contains_keys(keys):
    """Returns true if env contains the specified keys. Logs error and returns false otherwise."""
    missing = filter(lambda key : key not in env, keys)
    if missing:
        # create key1=val1,key2=val2,...
        set_arg_example = ",".join(map(lambda x: x[0]+"=val%d" % x[1], zip(keys,range(1,len(keys)+1))))
        error("Unspecified parameters: %s. Please run with --set %s or specify in config file and run with fab -c <configfile>" % (", ".join(missing), set_arg_example))
        return False
    else:
        return True
