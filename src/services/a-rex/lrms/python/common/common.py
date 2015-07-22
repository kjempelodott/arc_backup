"""
Declares the ArcError exception and Config, UserConfig and Logger objects.
"""

import arc, os, sys, time, re, stat
from tools import *

class Object(object):
    """
    Generic empty object.
    """
    pass

class ArcError(Exception):
    """
    Print an Arc::ERROR to the log and exit.

    :param str msg: an *informative* error message
    """

    def __init__(self, msg, caller = None):
        log(arc.ERROR, msg, caller)
        time.sleep(10)
        sys.exit(1)  

__logStream__ = arc.LogStream(sys.stderr)
__logStream__.setFormat(arc.EmptyFormat)
arc.Logger_getRootLogger().addDestination(__logStream__)
arc.Logger_getRootLogger().setThreshold(arc.DEBUG)

def log(level = arc.INFO, message = '', caller = None):
    if caller:
        caller = 'PythonLRMS.%s' % caller
    else:
        caller = 'PythonLRMS'

    arc.Logger(arc.Logger_getRootLogger(), caller).msg(level, message)

# Objects get a constant pointer,
# so these will be global to all modules that imports lrms.common.common
Config = Object()
UserConfig = arc.UserConfig()
SSHSession = {}

def ssh_connect(host, user, pkey, window_size = (2 << 15) - 1):
    from paramiko.transport import Transport
    from paramiko import RSAKey

    try:
        SSHSession[host] = Transport((host, 22))
        SSHSession[host].window_size = window_size
        pkey = RSAKey.from_private_key_file(pkey, '')  
        SSHSession[host].connect(username = user, pkey = pkey)
    except Exception as e:
        raise ArcError('Failed to connect to %s:\n%s' % (host, str(e)), 'common.common')
