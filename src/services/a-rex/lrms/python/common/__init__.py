"""
Subpackage with helper functions for the batch system specific modules.

Logging is setup here.
"""

import sys
import arc
import cancel, config, files, log, parse, proc, scan, ssh, submit

arc.Logger_getRootLogger().setThreshold(arc.DEBUG)
