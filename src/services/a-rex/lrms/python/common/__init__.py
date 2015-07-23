"""
Common classes, functions and objects for all LRMSs.
"""

import sys
import arc
import cancel, common, config, parse, proc, scan, submit, tools

class Object(object):
    """                                                                                                                   
    Generic empty object.                                                                                                 
    """
    pass

__logStream__ = arc.LogStream(sys.stderr)
__logStream__.setFormat(arc.EmptyFormat)
arc.Logger_getRootLogger().addDestination(__logStream__)
arc.Logger_getRootLogger().setThreshold(arc.DEBUG)

UserConfig = arc.UserConfig()
