from fabric.api import env
from . import warn,error
import sys

# -------------- Configuration helpers ------------------
def configure():
    """Runs configuration, if it has not already run.
    All config settings in env.defaults are applied to env, if not already set.
    All config keys listed in env.ints have their values converted to ints.
    env.provider_config_functions are tried until one succeeds or fails. E.g. env.provider_config_functions = [aws_config, gce_config]
    If a provider_config_function reports that it is selected but failed, sys.exit(1)
    """
    if env.get("configured") is None:
        if not "ints" in env:
            env.ints = []
        env.ints.append('provisioning_timeout')

        _apply_defaults_()
        env.configured = False
        if env.provider_config_functions and len(env.provider_config_functions) > 0:
            for f in env.provider_config_functions:
                result = f()
                if result is None:
                    # try another provider
                    pass
                elif result:
                    # successfully configured this provider
                    env.configured = True
                    break
                else:
                    # This provider was selected, but configuration failed.
                    # Error should have already been logged by provider_config_function.
                    sys.exit(1)

        if not env.configured:
            warn("No cloud provider config functions were supplied via env.provider_config_functions and/or no provider was selected via env.provider")
            # This might have been intentional for non-cloud use, so don't abort
            env.configured = True
        return True

def _apply_defaults_():
    "Apply default values for optional settings"
    if "defaults" in env:
        for k in env.defaults:
            if not k in env:
                env[k] = env.defaults[k]

    # Force numeric conversions to make easier use later on
    if "ints" in env:
        for k in env.ints:
            env[k] = int(env[k])

def verify_env_contains_keys(keys):
    """Returns true if env contains the specified key(s). Logs error and returns false otherwise."""
    if isinstance(keys,basestring):
        keys = [keys]
    missing = filter(lambda key : key not in env, keys)
    if missing:
        # create key1=val1,key2=val2,...
        set_arg_example = ",".join(map(lambda x: x[0]+"=val%d" % x[1], zip(keys,range(1,len(keys)+1))))
        error("Unspecified parameters: %s. Please run with --set %s or specify in config file and run with fab -c <configfile>" % (", ".join(missing), set_arg_example))
        return False
    else:
        return True
