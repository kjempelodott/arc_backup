from common.cancel import *
from common.common import *
from common.config import *
from common.proc import *
from common.scan import *
from common.submit import *
from common.files import read
import json

def setup_api():
    from sceclient import isLogin, getHttpClient
    # JSON cookie lives for 5 minutes
    if not isLogin(): # Will also resets time of cookie
        raise ArcError('SCEAPI cookie expired! Run \'%s/sceapiLogin.py [grid-user]\' to '
                       'login.' % arc.common.ArcLocation.GetDataDir(), 'sceapi')
    client = getHttpClient()
    if not client:
        raise ArcError('SCEAPI cookie empty! Run \'%s/sceapiLogin.py [grid-user]\' to '
                       'login.' % arc.common.ArcLocation.GetDataDir(), 'sceapi')
    return client


def set_sceapi(cfg):
    for section in cfg.sections():
        if section[:6] != 'queue/' or not section[6:]:
            continue
        if section[6:] not in Config.queue:
            Config.queue[section[6:]] = Object()
        if not cfg.has_option(section, 'sceapi_host'):
            raise ArcError('SCEAPI requires \'sceapi_host\' in queue section of arc.conf', 'sceapi')
        Config.queue[section[6:]].sceapi_host = cfg.get(section, 'sceapi_host').strip('"')

    

#---------------------
# Submit methods
#---------------------

def Submit(config, jobdescs, jc):

    import fcntl
    # Allow only one submit at the same time
    _lock = open('/tmp/sceapi-submit-job.lock', 'a')
    fcntl.flock(_lock, fcntl.LOCK_EX)

    cfg = get_parsed_config(config)
    set_common(cfg)
    set_gridmanager(cfg)
    set_cluster(cfg)
    set_queue(cfg)
    set_sceapi(cfg)
    client = setup_api()

    jd = jobdescs[0]
    validate_attributes(jd)

    # Run RTE stage0

    log(arc.DEBUG, '----- starting sceapiSubmitter.py -----', 'sceapi.Submit')
    rel = re.compile(r'APPS/HEP/ATLAS-(?P<release>[\d\.]+-[\w_-]+)')
    release = None
    for rte in jd.Resources.RunTimeEnvironment.getSoftwareList():
        match = rel.match(str(rte))
        if match:
            release = match.groupdict()['release']
            break
    if not release:
        raise ArcError('ATLAS release not specified', 'sceapi.Submit')

    # Create job dict
    jobJSDL = assemble_dict(jd, release)
    args = jobJSDL.pop('arguments')
    input_dict = get_input_dict(jd, args)

    log(arc.DEBUG, 'SCEAPI jobname: %s' % jd.Identification.JobName, 'sceapi.Submit')
    log(arc.DEBUG, 'SCEAPI job dict built', 'sceapi.Submit')
    log(arc.DEBUG, '----------------- BEGIN job dict -----', 'sceapi.Submit')
    for key, val in jobJSDL.items():
        log(arc.DEBUG, '%s : %s' % (key, val), 'sceapi.Submit')
    log(arc.DEBUG, '----------------- END job dict -----', 'sceapi.Submit')

    #######################################
    #  Submit the job
    ######################################

    directory = jd.OtherAttributes['joboption;directory']
    log(arc.DEBUG, 'session directory: %s' % directory, 'sceapi.Submit')
    resp = client.submitJSON(jobJSDL)
    handle = None
    try:
        handle = json.loads(resp, 'utf8')
    except:
        raise ArcError('SCEAPI client response:\n%s' % str(resp), 'sceapi.Submit')

    failure = ''
    if handle['status_code'] == 0:

        jobID = handle['gidujid']['ujid']
        gid = handle['gidujid']['gid']
        
        upload_tries = 0
        ret_code = -1
        while upload_tries < 5:
            resp_text = client.putfiles(gid, input_dict)
            try:
                ret_code = json.loads(resp_text, 'utf8')['status_code']
                assert(ret_code == 0)
                break
            except AssertionError:
                sleep(2)
                upload_tries += 1
            except:
                raise ArcError('SCEAPI client response:\n%s' % str(resp_text), 'sceapi')

        if ret_code == 0:
            if json.loads(client.run(jobID), 'utf8')['status_code'] == 0:
                log(arc.DEBUG, 'job submitted successfully!', 'sceapi.Submit')
                log(arc.DEBUG, 'local job id: %s' % (jobID), 'sceapi.Submit')
                log(arc.DEBUG, '----- exiting sceapiSubmitter.py -----', 'sceapi.Submit')
                newJob = arc.Job()
                newJob.IDFromEndpoint = str(jobID)
                jc.addEntity(newJob)
                return True
            failure = 'Start job query failed.'
        else:
            failure = 'Failed to upload input files.'
    else:
        failure = 'Status code %i' % handle['status_code']
        # TODO: handle['status_reason'] in Chinese. Decode and print!

    log(arc.DEBUG, 'job *NOT* submitted successfully!', 'sceapi.Submit')
    if failure:
        log(arc.DEBUG, str(failure), 'sceapi.Submit')
    log(arc.DEBUG, '----- exiting sceapiSubmitter.py -----', 'sceapi.Submit')
    return False

#---------------------
# Cancel methods
#---------------------

def Cancel(config, jobid):

    client = setup_api()
    resp = client.killJob(str(jobid))
    put_cleanup_file(client, jobid)
    try:
        return json.loads(resp, 'utf8')['status_code']
    except:
        raise ArcError('SCEAPI client response:\n%s' % str(resp), 'sceapi.Cancel')


#---------------------
# Scan methods
#---------------------
    
def Scan(config, ctr_dirs):
    # Before anything can go wrong ...
    time.sleep(10)

    cfg = get_parsed_config(config)
    set_gridmanager(cfg)

    if Config.scanscriptlog:
        scanlogfile = arc.common.LogFile(Config.scanscriptlog)
        arc.common.Logger_getRootLogger().addDestination(scanlogfile)

    jobs = get_jobs(ctr_dirs)
    process_jobs(jobs)


def process_jobs(jobs):
    if not jobs: return

    sce_jobs = {}
    index = 0
    client = setup_api()
    for localid in jobs:
        query = 'ujids=' + localid
        resp = client.bjobs(query)
        try:
            ret_json = json.loads(resp, 'utf8')
            sce_jobs[localid] = ret_json['jobs_list'][0]
        except:
            add_failure(job)
            log(Arc.ERROR, 'SCEAPI client response:\n%s' % str(resp), 'sceapi.Scan')

    for localid, jdict in sce_jobs.items():
        try:
            job = jobs[localid]
            job.state = str(jdict['status'])
        except:
            add_failure(job)
            log(arc.ERROR, 'Job (%s) query returned empty json.' % job.globalid, 'sceapi.Scan')
            continue

        if job.state in RUNNING:
            continue

        if not download_output(job, client):
            add_failure(job)
            log(arc.ERROR, 'Failed to download all files for job %s.' % job.globalid, 'sceapi.Scan')
            if os.path.isfile(job.count_file): # Count file is removed after 5 failures
                continue

        job.exitcode = (-1, 0)[job.state == '20']
        job.message = MESSAGES[job.state]

        re_time = re.compile(r'^(?P<dd>\d\d\d)d(?P<HH>\d\d)h(?P<MM>\d\d)m(?P<SS>\d\d)s')
        walltime = re_time.match(jdict['walltime']).groupdict()
        y = int(walltime['dd'])/365
        d = int(walltime['dd']) % 365
        m, d = d/30, d%30
        walltime['yyyy'] = '%04i' % y
        walltime['mm'] = '%02i' % m
        walltime['dd'] = '%02i' % d
        zero_time = arc.common.Time('0000-00-00T00:00:00')

        job.WallTime = arc.common.Time(str(get_MDS(walltime))) - zero_time
        job.Processors = jdict['corenum']
        put_cleanup_file(client, job.localid)

        with open(job.lrms_done_file, 'w') as f:
            f.write('%d %s\n' % (job.exitcode, job.message))
        update_diag(job)
        gm_kick([job])


def put_cleanup_file(client, jobid):
    # Let cleanup cron job know this job can be deleted
    TO_DELETE = '/tmp/TO_DELETE'
    open(TO_DELETE, 'w').close()
    client.putfiles(int(jobid), {'TO_DELETE': TO_DELETE})


def assemble_dict(j, rel):

    job_dict = {}
    gridid = j.OtherAttributes['joboption;gridid']
    ### jobname #PBS -N
    if j.Identification.JobName:
        prefix = 'N' if not j.Identification.JobName[0].isalpha() else ''
        job_dict['jobName'] = prefix + re.sub(r'\W', '_', j.Identification.JobName)[:15-len(prefix)]

    ### stdout, stderr #PBS -o,-e
    job_dict['stdout'] = os.path.basename(j.Application.Output)
    job_dict['stderr'] = os.path.basename(j.Application.Error)

    ### ARCpilot-ATLAS app
    job_dict['execName'] = job_dict['appName'] = 'ARCpilot-ATLAS'
    if str(j.Application.Executable.Argument[0]) == rel:
	j.Application.Executable.Argument = j.Application.Executable.Argument[1:]
    job_dict['arguments'] = '%s %s %s' % (gridid, rel, 
                            ' '.join("'%s'" % arg for arg in j.Application.Executable.Argument))
    ### queue #PBS -q
    queue = j.Resources.QueueName
    job_dict['queue'] = queue
    job_dict['hostName'] = Config.queue[queue].sceapi_host
   
    ### number of CPUs, #PBS -n
    job_dict['cpuCount'] = j.Resources.SlotRequirement.NumberOfSlots \
                       if j.Resources.SlotRequirement.NumberOfSlots > 1 else 1
  
    ### walltime, #PBS -l walltime
    cputime = j.Resources.TotalCPUTime.range.max
    walltime = j.Resources.IndividualWallTime.range.max
    if walltime >= 0:
        job_dict['wallTime'] = walltime/60
    elif cputime >=0:
        job_dict['wallTime'] = cputime/60

    ### output files
    job_dict['targetFiles'] = get_output_list(j)

    return job_dict


def get_output_list(j):
    flist = [{'fileName':f.Name, 'url':f.Name, 'delFlag':True} for f in j.DataStaging.OutputFiles]
    gridid = j.OtherAttributes['joboption;gridid']
    flist.append({'fileName':'*.root*'      , 'url':'*.root*'      , 'delFlag':True})
    flist.append({'fileName':'log*.tgz*'    , 'url':'log*.tgz*'    , 'delFlag':True})
    flist.append({'fileName':'%s.*' % gridid, 'url':'%s.*' % gridid, 'delFlag':True})
    return flist


def get_input_dict(j, args):

    input_dict = {}
    directory = j.OtherAttributes['joboption;directory']
    args_fname = 'input.parameter'
    args_path = os.path.join(directory, args_fname)
    with open(args_path, 'w') as f:
        f.write(args)
    input_dict[args_fname] = args_path

    # arcsub will download input files to local sessiondir
    for f in j.DataStaging.InputFiles:
        fname = os.path.basename(f.Sources[0].fullstr())
        if not fname or fname == os.path.sep:
            continue
        path = os.path.join(directory, fname)
        if not os.path.exists(path):
            raise ArcError('Input file (%s) does not exist.' % path, 'sceapi.Submit')
        input_dict[fname] = path
    return input_dict


def download_output(job, client):
    dest = job.sessiondir
    error = False
    files = None
    jobid = int(job.localid)

    try:
        files = json.loads(client.listJob(jobid), 'utf8')['files']
    except:
        return False

    # First download files specified in output file in ctrdir (output.list ...)
    is_good, fn = read(job.output_file)
    if is_good:
        try:
            fn.remove('/gmlog\n')
            fn.remove('/@output.list\n')
        except ValueError:
            pass
        try:
            for line in fn:
                _f = line.strip().lstrip('/')
                if _f not in files:
                    log(arc.DEBUG, 'File (%s) not found for job (%s)' % (str(_f), job.globalid),
                        'sceapi.Scan')
                    continue
                _f_local = os.path.join(dest, _f)
                if os.path.isfile(_f_local):
                    continue
                client.downloadFile(jobid, _f, dest)['filePath'] 
                log(arc.DEBUG, 'Downloaded output file (%s)' % str(_f), 'sceapi.Scan')
                os.chown(_f_local, job.uid, job.gid)
        except KeyError:
            log(arc.DEBUG, 'Failed to download output file (%s)' % str(_f), 'sceapi.Scan')
            error = True
    else:
        log(arc.DEBUG, 'Failed to read file (%s)' % os.path.basename(job.output_file), 'sceapi.Scan')
        error = True

    # Then continue with files in 'output.list'
    output_list = os.path.join(dest, 'output.list')    
    is_good, fn = read(output_list)
    if is_good:
        try:
            for line in fn:
                _f = line.strip().split()[0] # Second field is rucio destination
                if _f not in files:
                    log(arc.DEBUG, 'File (%s) not found for job (%s)' % (str(_f), job.globalid),
                        'sceapi.Scan')
                    continue
                _f_local = os.path.join(dest, _f)
                if os.path.isfile(_f_local):
                    continue
                client.downloadFile(jobid, _f, dest)['filePath'] 
                log(arc.DEBUG, 'Downloaded output file (%s)' % str(_f), 'sceapi.Scan')
                os.chown(_f_local, job.uid, job.gid)
        except KeyError:
            log(arc.DEBUG, 'Failed to download output file (%s)' % str(_f), 'sceapi.Scan')
            error = True
    else:
        log(arc.DEBUG, 'Failed to read file (%s)' % output_list, 'sceapi.Scan')
        error = True
    return not error
    
# class job():
#     pass

# j = job()
# job.globalid = "jjNODm1sbJmnBw7jSoCD2RhnABFKDmABFKDm92KKDmABFKDmKSYh4n"
# j.sessiondir = "/scratch/grid/" + job.globalid
# job.localid = 7001
# job.uid = 500
# job.gid = 500
# job.output_file = "/scratch/jobstatus/job." + job.globalid + ".output"
# from lrms import sceclient, sceapi
# client = sceclient.getHttpClient()
# sceapi.download_output(j,client)
# fd = {"TO_DELETE": "TO_DELETE"}
# h = client.putfiles(job.localid, fd)
