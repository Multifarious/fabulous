from subprocess import check_output

def get_sha():
    """Determines Git SHA of current working directory."""
    return check_output(["git", "log","-n1","--pretty=oneline"]).split(' ')[0]
