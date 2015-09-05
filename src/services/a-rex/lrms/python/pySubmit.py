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
        error('Failed to import lrms.%s module' % lrmsname, 'pySubmit')
        sys.exit(2)


if __name__ == '__main__':
    
    if len(sys.argv) != 4:
        error('Usage: %s <arc.conf> <lrms> <jobdesc>' % (sys.argv[0]), 'pySubmit')
        sys.exit(1)

    lrms = get_lrms_module(sys.argv[2])
    descfile = sys.argv[3]
    localfile = descfile.replace('.description', '.local')
    gridid = descfile.split('.')[-2]
    is_parsed = False
    try:
        jds = arc.JobDescriptionList()
        with open(descfile, 'r') as jobdesc:
            is_parsed = arc.JobDescription.Parse(jobdesc.read(), jds, '', 'GRIDMANAGER')
        jd = jds[0]
        jd.OtherAttributes['joboption;gridid'] = gridid
        localid = lrms.Submit(sys.argv[1], jd)
        assert(type(localid) == str)
        with open(localfile, 'a') as local:
            local.write('localid=%s\n' % localid)
        sys.exit(0)
    except (ArcError, AssertionError):
        pass
    except IOError:
        if not is_parsed:
            error('%s: Failed to read job description file' % gridid, 'pySubmit')
        else:
            error('%s: Failed to write job ID to local file' % gridid, 'pySubmit')
    except Exception:
        error('Unexpected exception:\n%s' % traceback.format_exc(), 'pySubmit')
    sys.exit(1)

