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
        error('Usage: %s <arc.conf> <jobid> <lrms>' % (sys.argv[0]), 'pySubmit')
        sys.exit(1)

    modules = setup(sys.argv[3])
    if not type(modules) == dict:
        sys.stderr.write(modules)
        sys.exit(1)

    try:
        if modules['lrms'].Cancel(sys.argv[1], sys.argv[2]):
            sys.exit(0)
    except Exception:
        error('Unexpected exception:\n%s' % traceback.format_exc(), 'pyModule.submit')
    except ArcError:
        pass
    sys.exit(1)
