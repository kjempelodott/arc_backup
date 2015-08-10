"""
Subpackage with helper functions for the batch system specific modules.

Logging is setup here.
"""

import sys
import arc
import cancel, config, files, log, parse, proc, scan, ssh, submit

DEBUG = True
if DEBUG:
    _logStream = arc.LogStream(sys.stderr)
    _logStream.setFormat(arc.EmptyFormat)
    arc.Logger_getRootLogger().addDestination(_logStream)
    arc.Logger_getRootLogger().setThreshold(arc.DEBUG)
