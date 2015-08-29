import sys, traceback

def setup(lrmsname):
    modules = {}
    try:
        modules['arc'] = __import__('arc')
    except:
        return 'Failed to import arc module'

    try:
        modules['lrms'] =  __import__('lrms.' + lrmsname, fromlist = ['lrms'])
        return modules
    except:
        return 'Failed to import lrms module'


if __name__ == '__main__':
    
    if len(sys.argv) != 4:
        error('Usage: %s <arc.conf> <jobdesc> <lrms>' % (sys.argv[0]), 'pySubmit')
        sys.exit(1)

    modules = setup(sys.argv[3])
    if not type(modules) == dict:
        sys.stderr.write(modules)
        sys.exit(1)

    try:
        from lrms.common.log import ArcError, error
        _jds = arc.JobDescriptionList()
        content = open(sys.argv[2], 'r').read()
        is_parsed = arc.JobDescription.Parse(jd.read(), _jds)
        localid = modules['lrms'].Submit(sys.argv[1], _jds[0])
        assert(type(localid) == str)
        # write localid to local file
        sys.exit(0)
    except (ArcError, AssertionError):
        pass
    except IOError:
        error('Failed to read job description file (%s)' % jobdesc, 'pySubmit')
    except Exception:
        error('Unexpected exception:\n%s' % traceback.format_exc(), 'pySubmit')
    sys.exit(1)

