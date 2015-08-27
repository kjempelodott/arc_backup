#!/usr/bin/env python

import sys, traceback

try:
    import arc
except:
    sys.stderr.write('No module named arc\n')
    return 2

try:
    from lrms import fork
    from lrms.common.parse import JobDescriptionParserGRAMi
    from lrms.common.log import ArcError, error
except:
    sys.stderr.write('Failed to import lrms module\n')
    return 3


if __name__ == '__main__':

    if len(sys.argv) != 4:
        error('Usage: %s --config <arc.conf> <grami>' % (sys.argv[0]), 'forkSubmitter')
        return 1

    if sys.argv[1] != "--config":
        error('First argument must be \'--config\' followed by path to arc.conf', 'forkSubmitter')
        return 1

    arc_conf = sys.argv[2]
    grami = sys.argv[3]

    jobdescs = arc.JobDescriptionList()
    try: 
        with open(grami, 'r') as jobdesc_file:
            content = jobdesc_file.read()
            isParsed = JobDescriptionParserGRAMi.Parse(content, jobdescs)
            if not isParsed or not jobdescs:
                isParsed = arc.JobDescription.Parse(content, jobdescs)
                if not isParsed or not jobdescs:
                    error('Unable to parse job description from file (%s)' % grami, 'forkSubmitter')
                    return 1
                jobdescs[0].OtherAttributes["joboption;gridid"] = str(time.time())
    except IOError:
        error('File (%s) does not appear to exist' % grami, 'forkSubmitter')
    except ArcError:
        pass
    else:
        return 1
    
    try:
        jc = arc.compute.JobContainer()
        if fork.Submit(arc_conf, jobdescs, jc):
            if grami[-6:] == '.grami':
                with open(grami, "a") as fgrami:
                    fgrami.write('joboption_jobid=%s\n' % (jc[0].IDFromEndpoint))
        return 0
    except ArcError:
        pass
    except Exception:
        error('Unexpected exception:\n%s' % traceback.format_exc(), 'forkSubmitter')
    return 1
        
