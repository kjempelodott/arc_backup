#!/usr/bin/env python

import sys, time, traceback

try:
    time.sleep(10)
    import arc
    from lrms.common.log import ArcError, error
except:
    sys.stderr.write('Failed to import arc or lrms.common module\n')
    sys.exit(2)

def get_lrms_module(lrmsname):
    try:
        return  __import__('lrms.' + lrmsname, fromlist = ['lrms'])
    except:
        error('Failed to import lrms.%s module' % lrmsname, 'pyScanner')
        sys.exit(2)


if __name__ == '__main__':
    
    if len(sys.argv) != 4:
        error('Usage: %s <arc.conf> <lrms> <control directories ... >' % (sys.argv[0]), 'pyScanner')
        sys.exit(1)

    lrms = get_lrms_module(sys.argv[2])
    try:
        lrms.Scan(sys.argv[1], sys.argv[3:])
        sys.exit(0)
    except (ArcError, AssertionError):
        pass
    except Exception:
        error('Unexpected exception:\n%s' % traceback.format_exc(), 'pySubmit')
    sys.exit(1)

