#!/usr/bin/env python

import sys, time, traceback

try:
    import arc
except:
    print("No module named arc")
    time.sleep(10)
    sys.exit(2)

try:
    from lrms import fork
    from lrms.common.parse import JobDescriptionParserGRAMi
    from lrms.common.log import ArcError
except:
    raise ArcError("Failed to import lrms modules")


if __name__ =="__main__":

    if len(sys.argv) != 4:
        raise ArcError("Usage: %s --config <arc.conf> <grami>" % (sys.argv[0]))
    
    if sys.argv[1] != "--config":
        raise ArcError("Error: First argument must be '--config' followed by path to arc.conf")
    
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
                    raise ArcError("Unable to parse job description from file (%s)" % grami)
                jobdescs[0].OtherAttributes["joboption;gridid"] = str(time.time())
    except IOError:
        raise ArcError("Error: File (%s) does not appear to exist" % grami)
    
    try:
        jc = arc.compute.JobContainer()
        if fork.Submit(arc_conf, jobdescs, jc):
            if grami[-6:] == ".grami":
                with open(grami, "a") as fgrami:
                    fgrami.write("joboption_jobid=%s\n" % (jc[0].IDFromEndpoint))
    except Exception:
        raise ArcError("Unexpected exception:\n%s" % traceback.format_exc())    

