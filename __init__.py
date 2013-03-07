from itertools import chain
from fabric.api import env, local, task, runs_once
from fabric.colors import green,blue,cyan,yellow,magenta,red
import subprocess
import sys
import time

# -------------- Logging helpers ------------------

def _log_(level,msg,colorFunc = lambda x: x):
    "Logs message, prefixed with currently active host name and timestamp. Optional color."
    print(colorFunc("[%s] (%s) %s: %s" % (level, env.host if env.host else "local",time.strftime("%H:%M:%S"),msg)))

def debug(msg,colorFunc = lambda x :x):
    _log_("DEBUG",msg,colorFunc)
def info(msg,colorFunc = cyan):
    _log_("INFO",msg,colorFunc)
def warn(msg,colorFunc = magenta):
    _log_("WARN",msg,colorFunc)
def error(msg,colorFunc = red):
    _log_("ERROR",msg,colorFunc)

def merge_dicts(*dicts):
    "Merge dictionaries, with later ones overriding earlier ones. From http://stackoverflow.com/a/38990/708883"
    return dict(chain(*[d.iteritems() for d in dicts]))




# -------------- Execution helpers ------------------

def run_and_return_result(args,assert_successful=False):
    """Uses subprocess to run provided command and capture its output. Not suitable for huge amounts of output.
    Logs error and exits(!) on non-zero response."""
    p = subprocess.Popen(args,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    stdout,stderr = p.communicate()
    code = p.poll()
    if (code > 0 and assert_successful):
        error("Could not execute %s:\n%s" % (" ".join(args), stderr))
        sys.exit(1)
    return (code,stdout,stderr)




# -------------- Error helpers ------------------

def retry(ExceptionToCheck, total_tries=4, initial_delay_seconds=3, backoff_multiplier=2):
    """Retry calling the decorated function using an exponential back-off.

    adapted from: http://www.saltycrane.com/blog/2009/11/trying-out-retry-decorator-python/
    original from: http://wiki.python.org/moin/PythonDecoratorLibrary#Retry

    :param ExceptionToCheck: the exception to check. may be a tuple of
        exceptions to check

    Usage:
        @retry(ExceptionToCheck)
        def my_function(arg1, arg2):
            ...

    Default behavior is four tries with 21 seconds of sleep (3, 6, and 12 seconds, respectively).
    8 tries with same initial delay and multiplier yields 6:35. In general total sleep is:
    initial_delay_seconds * backoff_multiplier ^ (total_tries - 1) - initial_delay_seconds
    """
    def deco_retry(f):
        def f_retry(*args, **kwargs):
            mtries_remaining, mdelay = total_tries, initial_delay_seconds
            while mtries_remaining > 0:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck, e:
                    lastException = e
                    mtries_remaining -= 1
                    if mtries_remaining > 0:
                        info("%s invocation failed (%s: %s). Don't panic yet; retrying in %d seconds..." % (f.__name__,type(e).__name__, str(e), mdelay))
                        time.sleep(mdelay)
                        mdelay *= backoff_multiplier
                    else:
                        warn("%s: %s, Giving up after %d attempts." % (type(e).__name__,str(e), total_tries))
            raise lastException
        return f_retry # true decorator
    return deco_retry