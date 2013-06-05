from fabric.operations import sudo
from fabulous import retry

@retry(SystemExit) # Oddly on AWS EC2 this sometimes fails on the first try
def install_packages(*packages):
    sudo('apt-get -qq --yes update')
    sudo('DEBIAN_FRONTEND=noninteractive apt-get -qq --yes install %s'  % (" ".join(packages)))

def upgrade_system():
    # --force-confnew, --force-confold: When config file updated, prefer new/old version
    # --force-confdef: if a default selection is specified for the package, allow it to trump
    sudo('DEBIAN_FRONTEND=noninteractive apt-get -qq --yes -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confnew" upgrade')
