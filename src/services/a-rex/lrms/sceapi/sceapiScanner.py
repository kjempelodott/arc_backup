#!/usr/bin/env python

import sys, time, traceback

try:
    import arc
except:
    sys.stderr.write('No module named arc\n')
    time.sleep(10)
    sys.exit(2)
try:
    from lrms import sceapi
    from lrms.common.log import ArcError
except:
    sys.stderr.write('Failed to import lrms module\n')
    time.sleep(10)
    sys.exit(3)


if __name__ == '__main__':

    if len(sys.argv) != 4:
        raise ArcError('Usage: %s --config <arc.conf> <control directories>' % (sys.argv[0]), 'sceapiScanner')

    if sys.argv[1] != '--config':
        raise ArcError('First argument must be \'--config\' followed by path to arc.conf', 'sceapiScanner')

    arc_conf = sys.argv[2]
    ctr_dirs = sys.argv[3:]
    
    try:
        sceapi.Scan(arc_conf, ctr_dirs)
    except Exception:
        raise ArcError('Unexpected exception:\n%s' % traceback.format_exc(), 'sceapiScanner')
