#!/usr/bin/env python

import sys, traceback

try:
    import arc
    from lrms.common.log import ArcError, error
    from lrms.common.parse import JobDescriptionParserGRAMi
except:
    sys.stderr.write('Failed to import arc or lrms.common module\n')
    sys.exit(2)

def get_lrms_module(lrmsname):
    try:
        return  __import__('lrms.' + lrmsname, fromlist = ['lrms'])
    except:
        error('Failed to import lrms.%s module' % lrmsname, 'pySubmit')
        sys.exit(2)


if __name__ == '__main__':
    
    if len(sys.argv) != 4:
        error('Usage: %s <arc.conf> <lrms> <grami>' % (sys.argv[0]), 'pySubmit')
        sys.exit(1)

    lrms = get_lrms_module(sys.argv[2])
    grami = sys.argv[3]
    gridid = grami.split('.')[-2]
    is_parsed = False
    try:
        jds = arc.JobDescriptionList()
        with open(grami, 'r+') as jobdesc:
            content = jobdesc.read()
            is_parsed = JobDescriptionParserGRAMi.Parse(content, jds)
            jd = jds[0]
            localid = lrms.Submit(sys.argv[1], jd)
            assert(type(localid) == str and localid.isnum())
            jobdesc.write('joboption_jobid=%s\n' % localid)
        sys.exit(0)
    except (ArcError, AssertionError):
        pass
    except IOError:
        error('%s: Failed to access GRAMi file' % gridid, 'pySubmit')
    except Exception:
        error('Unexpected exception:\n%s' % traceback.format_exc(), 'pySubmit')
    sys.exit(1)

