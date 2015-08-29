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


def submit(arc_conf, jobdesc, lrmsname):
    modules = setup(lrmsname)
    if not type(modules) == dict:
        return modules

    try:
        from lrms.common.log import ArcError, error
        jobdesc = modules['arc'].JobDescription.fromPy(jobdesc)
        localid = modules['lrms'].Submit(arc_conf, jobdesc)
        assert(type(localid) == str)
        return int(localid)
    except (ArcError, AssertionError):
        pass
    except Exception:
        error('Unexpected exception:\n%s' % traceback.format_exc(), 'pyModule.submit')
    else:
        return


def cancel(arc_conf, localid, lrmsname):
    modules = setup(lrmsname)
    if not type(modules) == dict:
        return modules

    try:
        if modules['lrms'].Cancel(arc_conf, localid):
            return 0
    except Exception:
        error('Unexpected exception:\n%s' % traceback.format_exc(), 'pyModule.submit')
    except ArcError:
        pass
    else:
        return 1
