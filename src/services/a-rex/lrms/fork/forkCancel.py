#!/usr/bin/env python

import sys, time, traceback

try:
    import arc
except:
    print("No module named arc")
    time.sleep(10)
    sys.exit(2)

try:
    from lrms import fork
    from lrms.common.log import ArcError
except:
    raise ArcError("Failed to import lrms modules")


if __name__ == "__main__":

    if len(sys.argv) != 4:
        raise ArcError("Usage: %s --config <arc.conf> <grami>" % (sys.argv[0]))

    if sys.argv[1] != "--config":
        raise ArcError("Error: First argument must be '--config' followed by path to arc.conf")
  
    arc_conf = sys.argv[2]
    grami = sys.argv[3]

    try:
        fork.Cancel(grami):
    except Exception:
        raise ArcError("Unexpected exception:\n%s" % traceback.format_exc())
