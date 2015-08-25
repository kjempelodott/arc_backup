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
    from lrms.common.parse import JobDescriptionParserGRAMi
    from lrms.common.log import ArcError
except:
    sys.stderr.write('Failed to import lrms module\n')
    time.sleep(10)
    sys.exit(3)


if __name__ == '__main__':

    if len(sys.argv) != 4:
        raise ArcError('Usage: %s --config <arc.conf> <grami>' % (sys.argv[0]), 'sceapiSubmitter')
    
    if sys.argv[1] != '--config':
        raise ArcError('First argument must be \'--config\' followed by path to arc.conf', 'sceapiSubmitter')
    
    arc_conf = sys.argv[2]
    grami = sys.argv[3]

    jobdescs = arc.JobDescriptionList()
    try: 
        with open(grami, 'r') as jobdesc_file:
            content = jobdesc_file.read()
            is_parsed = JobDescriptionParserGRAMi.Parse(content, jobdescs)
            if not is_parsed or not jobdescs:
                is_parsed = arc.JobDescription.Parse(content, jobdescs)
                if not is_parsed or not jobdescs:
                    raise ArcError('Unable to parse job description from file (%s)' % grami, 'sceapiSubmitter')
                jobdescs[0].OtherAttributes['joboption;gridid'] = str(time.time())
    except IOError:
        raise ArcError('File (%s) does not appear to exist' % grami, 'sceapiSubmitter')
    
    try:
        jc = arc.compute.JobContainer()
        if sceapi.Submit(arc_conf, jobdescs, jc):
            if grami[-6:] == '.grami':
                with open(grami, 'a') as fgrami:
                    fgrami.write('joboption_jobid=%s\n' % (jc[0].IDFromEndpoint))
    except Exception:
        raise ArcError('Unexpected exception:\n%s' % traceback.format_exc(), 'sceapiSubmitter')
