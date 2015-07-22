#!/usr/bin/python

import sys, time, traceback

try:
    import arc
except:
    sys.stderr.write('No module named arc\n')
    time.sleep(10)
    sys.exit(2)

try:
    from lrms import lsf
    from lrms.common.parse import SimpleGramiParser
    from lrms.common.common import *
except:
    sys.stderr.write('Failed to import lrms module\n')
    time.sleep(10)
    sys.exit(3)

  
if __name__ == '__main__':

    if len(sys.argv) != 4:
        raise ArcError('Usage: %s --config <arc.conf> <grami>' % sys.argv[0], 'lsfCancel')

    if sys.argv[1] != '--config':
        raise ArcError('First argument must be \'--config\' followed by path to arc.conf', 'lsfCancel')
  
    arc_conf = sys.argv[2]
    grami = sys.argv[3]

    try:
        grami = SimpleGramiParser(grami)
        lsf.Cancel(arc_conf, grami.jobid)
    except Exception:
        raise ArcError('Unexpected exception:\n%s' % traceback.format_exc(), 'lsfCancel')
