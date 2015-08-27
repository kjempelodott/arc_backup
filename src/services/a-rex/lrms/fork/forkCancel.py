#!/usr/bin/env python

import sys, traceback

try:
    import arc
except:
    sys.stderr.write('No module named arc\n')
    sys.exit(2)

try:
    from lrms import fork
    from lrms.common.parse import SimpleGramiParser
    from lrms.common.log import ArcError, error
except:
    sys.stderr.write('Failed to import lrms module\n')
    sys.exit(3)


if __name__ == '__main__':

    if len(sys.argv) != 4:
        error('Usage: %s --config <arc.conf> <grami>' % sys.argv[0], 'forkCancel')
        sys.exit(1)

    if sys.argv[1] != '--config':
        error('First argument must be \'--config\' followed by path to arc.conf', 'forkCancel')
        sys.exit(1)
  
    arc_conf = sys.argv[2]
    grami = sys.argv[3]

    try:
        grami = SimpleGramiParser(grami)
        sys.exit((1,0)[fork.Cancel(arc_conf, grami.jobid)])
    except ArcError:
        pass
    except Exception:
        error('Unexpected exception:\n%s' % traceback.format_exc(), 'forkCancel')
    sys.exit(1)
        
