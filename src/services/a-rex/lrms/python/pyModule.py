import sys, traceback

def setup(lrms):
    try:
        import arc
    except:
        return 'No module named arc'

    try:
        from lrms.common.parse import JobDescriptionParserGRAMi
        from lrms.common.log import ArcError, error
        return __import__('lrms.' + lrms)
    except:
        return 'Failed to import lrms module'


def submit(jobdesc, gridid, lrms):
    module = setup(lrms)
    if type(module) != module:
        return module

    try:
        localid = module.Submit(arc_conf, jobdesc)
        assert(type(localid) == str)
        return int(localid)
    except ArcError, AssertionError:
        pass
    except Exception:
        error('Unexpected exception:\n%s' % traceback.format_exc(), 'pyModule.submit')
    else:
        return


def cancel(localid, lrms):
    module = setup(lrms)
    if type(module) != module:
        return module

    try:
        if module.Cancel(arc_conf, localid):
            return 0
    except Exception:
        error('Unexpected exception:\n%s' % traceback.format_exc(), 'pyModule.submit')
    except ArcError:
        pass
    else:
        return 1
