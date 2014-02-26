"""
Helper functions for pushing content to a staging directory, moving the old directory aside, and moving the staging directory into place.
"""
from fabric.api import env
from fabric.operations import sudo
from . import debug
from os import path
import time
import re

def make_staging_directory(basename = "project", parent = "/opt"):
    dir_tmp = path.join(parent,basename) + time.strftime("_%Y%m%d_%H%M%S") + ".deploying"
    sudo('mkdir -p %s' % dir_tmp)
    sudo("chown %s:%s %s" % (env.user, env.group, dir_tmp))
    return dir_tmp

def flip(staging_dir):
    active_dir = re.sub(r'_[0-9]{8}_[0-9]{6}.deploying','',staging_dir)
    retired_dir = active_dir + time.strftime("_%Y%m%d_%H%M%S") + ".retired"
    debug("Flipping directory name.")
    sudo("mv %s %s.retired" % (active_dir,retired_dir), quiet=True)
    sudo("mv %s %s" % (staging_dir,active_dir))
    return active_dir
