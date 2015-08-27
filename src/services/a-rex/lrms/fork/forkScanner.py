#!/usr/bin/env python

import sys, time, traceback

time.sleep(10)

try:
    import arc
except:
    sys.stderr.write('No module named arc\n')
    sys.exit(2)

try:
    from lrms import fork
    from lrms.common.log import ArcError
except:
    sys.stderr.write('Failed to import lrms module\n')
    sys.exit(3)


if __name__ == '__main__':

    if len(sys.argv) != 4:
        error('Usage: %s --config <arc.conf> <control directories>' % sys.argv[0], 'forkScanner')
        sys.exit(1)

    if sys.argv[1] != '--config':
        error('First argument must be \'--config\' followed by path to arc.conf', 'forkScanner')
        sys.exit(1)

    arc_conf = sys.argv[2]
    ctr_dirs = sys.argv[3:]

    try:
        fork.Scan(arc_conf, ctr_dirs)
    except ArcError:
        sys.exit(1)
    except Exception:
        error('Unexpected exception:\n%s' % traceback.format_exc(), 'forkScanner')
        sys.exit(1)
        
        
