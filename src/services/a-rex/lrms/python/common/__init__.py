"""
Subpackage with helper functions for the batch system specific modules.

Logging is setup here.
"""

import sys
import arc
import cancel, config, log, parse, proc, scan, ssh, submit

__logStream__ = arc.LogStream(sys.stderr)
__logStream__.setFormat(arc.EmptyFormat)
arc.Logger_getRootLogger().addDestination(__logStream__)
arc.Logger_getRootLogger().setThreshold(arc.DEBUG)
