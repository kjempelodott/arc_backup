#!/usr/bin/env python

import sys, traceback

try:
    import arc
    from lrms.common.log import ArcError, error
except:
    sys.stderr.write('Failed to import arc or lrms.common module\n')
    sys.exit(2)

def get_lrms_module(lrmsname):
    try:
        return  __import__('lrms.' + lrmsname, fromlist = ['lrms'])
    except:
        error('Failed to import lrms.%s module' % lrmsname, 'pyCancel')
        sys.exit(2)


if __name__ == '__main__':

    if len(sys.argv) != 4:
        error('Usage: %s <arc.conf> <jobid> <lrms>' % (sys.argv[0]), 'pyCancel')
        sys.exit(1)

    lrms = get_lrms_module(sys.argv[3])
    try:
        if lrms.Cancel(sys.argv[1], sys.argv[2]):
            sys.exit(0)
    except Exception:
        error('Unexpected exception:\n%s' % traceback.format_exc(), 'pyCancel')
    except ArcError:
        pass
    sys.exit(1)
