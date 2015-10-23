"""
ScGrid SCEAPI (a RESTful API) interface module.
"""

import os, sys, re, json
import arc
from common.cancel import cancel
from common.config import Config, configure, is_conf_setter
from common.files import read
from common.log import debug, verbose, info, warn, error, ArcError
from common.scan import *
from common.submit import *


def setup_api():
    """
    Get SCEAPI client. Raises ArcError and prints login instruction to log if not logged in.

    :return: SCEAPI client
    :rtype: :py:class:`lrms.sceclient.ApiClient`
    """


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


@is_conf_setter
def set_sceapi(cfg):
    """
    Set SCEAPI specific :py:data:`~lrms.common.Config` attributes.

    :param cfg: parsed arc.conf
    :type cfg: :py:class:`ConfigParser.ConfigParser`
    """

    for name, queue in Config.queue.iteritems():
        if not cfg.has_option('queue/' + name, 'scgrid_host'):
            raise ArcError('SCEAPI backend requires \'scgrid_host\' in queue section of arc.conf', 'sceapi')
        queue.sceapi_host = cfg.get('queue/' + name, 'scgrid_host').strip('"')
                

def translate(text):
    """
    Translate Chinese error message to English (requires Goslate).

    :param str text: message to translate
    :return: translated message if Goslate installed, else original message
    :rtype: :py:obj:`str`
    """
    try:
        import goslate
        return goslate.Goslate().translate(text, 'en')
    except:
        return text
    
#---------------------
# Submit methods
#---------------------

def Submit(config, jobdesc):
    """    
    Submits an ATLAS job to the ScGrid host specified in arc.conf. This method executes the required
    RunTimeEnvironment scripts and assembles the bash job script. The job script is
    written to file and submitted with SCEAPI.
                                                                                     
    :param str config: path to arc.conf
    :param jobdesc: job description object  
    :type jobdesc: :py:class:`arc.JobDescription`
    :return: local job ID if successfully submitted, else ``None``
    :rtype: :py:obj:`str`
    """

    import fcntl
    # Allow only one submit at the same time
    _lock = open('/tmp/sceapi-submit-job.lock', 'a')
    fcntl.flock(_lock, fcntl.LOCK_EX)

    configure(config, set_sceapi)
    client = setup_api()

    validate_attributes(jobdesc)

    # Run RTE stage0
    debug('----- starting sceapiSubmitter.py -----', 'sceapi.Submit')
    rel = re.compile(r'APPS/HEP/ATLAS-(?P<release>[\d\.]+-[\w_-]+)')
    release = None
    for rte in jobdesc.Resources.RunTimeEnvironment.getSoftwareList():
        match = rel.match(str(rte))
        if match:
            release = match.groupdict()['release']
            break
    if not release:
        raise ArcError('ATLAS release not specified', 'sceapi.Submit')

    # Create job dict
    jobJSDL = assemble_dict(jobdesc, release)
    args = jobJSDL.pop('arguments')
    input_dict = get_input_dict(jobdesc, args)

    debug('SCEAPI jobname: %s' % jobdesc.Identification.JobName, 'sceapi.Submit')
    debug('SCEAPI job dict built', 'sceapi.Submit')
    debug('----------------- BEGIN job dict -----', 'sceapi.Submit')
    for key, val in jobJSDL.items():
        debug('%s : %s' % (key, val), 'sceapi.Submit')
    debug('----------------- END job dict -----', 'sceapi.Submit')

    #######################################
    #  Submit the job
    ######################################

    directory = jobdesc.OtherAttributes['joboption;directory']
    debug('session directory: %s' % directory, 'sceapi.Submit')
    resp = client.submitJSON(jobJSDL)
    handle = None
    try:
        handle = json.loads(resp, 'utf8')
    except:
        raise ArcError('SCEAPI client response:\n%s' % str(resp), 'sceapi.Submit')

    failure = ''
    if handle['status_code'] == 0:

        jobid = handle['gidujid']['ujid']
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
            if json.loads(client.run(jobid), 'utf8')['status_code'] == 0:
                debug('job submitted successfully!', 'sceapi.Submit')
                debug('local job id: %s' % jobid, 'sceapi.Submit')
                debug('----- exiting sceapiSubmitter.py -----', 'sceapi.Submit')
                return jobid
            failure = 'Start job query failed.'
        else:
            failure = 'Failed to upload input files.'
    else:
        failure = 'Status code %i: %s' % (handle['status_code'], translate(handle['status_reason']))
        
    debug('job *NOT* submitted successfully!', 'sceapi.Submit')
    if failure:
        debug(failure.encode('utf-8'), 'sceapi.Submit')
    debug('----- exiting sceapiSubmitter.py -----', 'sceapi.Submit')


#---------------------
# Cancel methods
#---------------------

def Cancel(config, jobid):
    """
    Cancel a job running at an ScGrid host.

    :param str config: path to arc.conf
    :param str jobid: local job ID
    :return: ``True`` if successfully cancelled, else ``False``
    :rtype: :py:obj:`bool`
    """

    client = setup_api()
    resp = client.killJob(str(jobid))
    put_cleanup_file(client, jobid)
    try:
        return json.loads(resp, 'utf8')['status_code'] == 0
    except:
        raise ArcError('SCEAPI client response:\n%s' % str(resp), 'sceapi.Cancel')


#---------------------
# Scan methods
#---------------------
    
def Scan(config, ctr_dirs):
    """
    Query SCEAPI for all jobs in /[controldir]/processing. If the job has stopped running,
    the diagnostics file is updated and ``gm-kick`` is executed.

    :param str config: path to arc.conf
    :param ctr_dirs: list of paths to control directories 
    :type ctr_dirs: :py:obj:`list` [ :py:obj:`str` ... ]
    """

    configure(config, set_sceapi);
    time.sleep(30)

    if Config.scanscriptlog:
        scanlogfile = arc.common.LogFile(Config.scanscriptlog)
        arc.common.Logger_getRootLogger().addDestination(scanlogfile)
        arc.common.Logger_getRootLogger().setThreshold(Config.log_threshold)

    jobs = get_jobs(ctr_dirs)
    if not jobs: return

    query = 'length=%i&ujids=%s' % (len(jobs), ','.join(jobs))
    resp = client.bjobs(query)
    sce_jobs = []

    try:
        ret_json = json.loads(resp, 'utf8')
        sce_jobs = ret_json['jobs_list']
    except:
        error('SCEAPI client response:\n%s' % str(resp), 'sceapi.Scan')

    for jdict in sce_jobs:

	localid = str(jdict['ujid'])
	job = jobs[localid]
        job.state = str(jdict['status'])

        if job.state in RUNNING:
            continue

        if not download_output(job, client):
            add_failure(job)
            error('Failed to download all files for job %s.' % job.globalid, 'sceapi.Scan')
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
    """
    Upload an empty TO_DELETE file to mark job for deletion.
    
    :param client: SCEAPI client
    :param type: :py:class:`lrms.sceclient.ApiClient`
    :param str jobid: local job ID
    """
    TO_DELETE = '/tmp/TO_DELETE'
    open(TO_DELETE, 'w').close()
    client.putfiles(int(jobid), {'TO_DELETE': TO_DELETE})


def assemble_dict(jobdesc, rel):
    """
    Translate job description to JSDL.

    :param jobdesc: job description object
    :type jobdesc: :py:class:`arc.JobDescription` 
    :param str rel: ATLAS release
    :return: JSDL dictionary
    :rtype: :py:obj:`dict`
    """
    
    job_dict = {}
    gridid = jobdesc.OtherAttributes['joboption;gridid']
    ### jobname #PBS -N
    if jobdesc.Identification.JobName:
        prefix = 'N' if not jobdesc.Identification.JobName[0].isalpha() else ''
        job_dict['jobName'] = prefix + re.sub(r'\W', '_', jobdesc.Identification.JobName)[:15-len(prefix)]

    ### stdout, stderr #PBS -o,-e
    job_dict['stdout'] = os.path.basename(jobdesc.Application.Output)
    job_dict['stderr'] = os.path.basename(jobdesc.Application.Error)

    ### ARCpilot-ATLAS app
    job_dict['execName'] = job_dict['appName'] = 'ARCpilot-ATLAS'
    if str(jobdesc.Application.Executable.Argument[0]) == rel:
	jobdesc.Application.Executable.Argument = jobdesc.Application.Executable.Argument[1:]
    job_dict['arguments'] = '%s %s %s' % (gridid, rel, 
                            ' '.join("'%s'" % arg for arg in jobdesc.Application.Executable.Argument))
    ### queue #PBS -q
    queue = jobdesc.Resources.QueueName
    job_dict['queue'] = queue
    job_dict['hostName'] = Config.queue[queue].sceapi_host
   
    ### number of CPUs, #PBS -n
    job_dict['cpuCount'] = jobdesc.Resources.SlotRequirement.NumberOfSlots \
                       if jobdesc.Resources.SlotRequirement.NumberOfSlots > 1 else 1
  
    ### walltime, #PBS -l walltime
    cputime = jobdesc.Resources.TotalCPUTime.range.max
    walltime = jobdesc.Resources.IndividualWallTime.range.max
    if walltime >= 0:
        job_dict['wallTime'] = walltime/60
    elif cputime >=0:
        job_dict['wallTime'] = cputime/60

    ### output files
    job_dict['targetFiles'] = get_output_list(jobdesc)

    return job_dict


def get_output_list(jobdesc):
    """
    Create an output file JSDL.

    :param jobdesc: job description object
    :type jobdesc: :py:class:`arc.JobDescription` 
    :return: output files JSDL
    :rtype: :py:obj:`dict`
    """

    flist = [{'fileName':f.Name, 'url':f.Name, 'delFlag':True} for f in jobdesc.DataStaging.OutputFiles]
    gridid = jobdesc.OtherAttributes['joboption;gridid']
    flist.append({'fileName':'*.root*'      , 'url':'*.root*'      , 'delFlag':True})
    flist.append({'fileName':'log*.tgz*'    , 'url':'log*.tgz*'    , 'delFlag':True})
    flist.append({'fileName':'%s.*' % gridid, 'url':'%s.*' % gridid, 'delFlag':True})
    return flist


def get_input_dict(jobdesc, args):
    """
    Create an input file map.

    :param jobdesc: job description object
    :type jobdesc: :py:class:`arc.JobDescription` 
    :return: input file map
    :rtype: :py:obj:`dict`
    """

    input_dict = {}
    directory = jobdesc.OtherAttributes['joboption;directory']
    args_fname = 'input.parameter'
    args_path = os.path.join(directory, args_fname)
    with open(args_path, 'w') as f:
        f.write(args)
    input_dict[args_fname] = args_path

    # arcsub will download input files to local sessiondir
    for f in jobdesc.DataStaging.InputFiles:
        fname = os.path.basename(f.Sources[0].fullstr())
        if not fname or fname == os.path.sep:
            continue
        path = os.path.join(directory, fname)
        if not os.path.exists(path):
            raise ArcError('Input file (%s) does not exist.' % path, 'sceapi.Submit')
        input_dict[fname] = path
    return input_dict


def download_output(job, client):
    """
    Download output files to local session directory.

    :param job: job object from :py:meth:`lrms.common.scan.get_jobs`
    :param type: :py:class:`object`
    :param client: SCEAPI client
    :param type: :py:class:`lrms.sceclient.ApiClient`
    :return: True if all downloads succeeded, else False
    :rtype: :py:obj:`bool`
    """

    dest = job.sessiondir
    error = False
    files = None
    jobid = int(job.localid)

    try:
        files = json.loads(client.listJob(jobid), 'utf8')['files']
    except:
        return False

    # First download files specified in output file in ctrdir (output.list ...)
    fl = read(job.output_file)
    if fl:
        try:
            fl.remove('/gmlog\n')
            fl.remove('/@output.list\n')
        except ValueError:
            pass
        try:
            for line in fl:
                _f = line.strip().lstrip('/')
                if _f not in files:
                    debug('File (%s) not found for job (%s)' % (str(_f), job.globalid), 'sceapi.Scan')
                    continue
                _f_local = os.path.join(dest, _f)
                if os.path.isfile(_f_local):
                    continue
                client.downloadFile(jobid, _f, dest)['filePath'] 
                debug('Downloaded output file (%s)' % str(_f), 'sceapi.Scan')
                os.chown(_f_local, job.uid, job.gid)
        except KeyError:
            debug('Failed to download output file (%s)' % str(_f), 'sceapi.Scan')
            error = True
    else:
        debug('Failed to read file (%s)' % os.path.basename(job.output_file), 'sceapi.Scan')
        error = True

    # Then continue with files in 'output.list'
    output_list = os.path.join(dest, 'output.list')    
    fl = read(output_list)
    if fl:
        try:
            for line in fl:
                _f = line.strip().split()[0] # Second field is rucio destination
                if _f not in files:
                    debug('File (%s) not found for job (%s)' % (str(_f), job.globalid), 'sceapi.Scan')
                    continue
                _f_local = os.path.join(dest, _f)
                if os.path.isfile(_f_local):
                    continue
                client.downloadFile(jobid, _f, dest)['filePath'] 
                debug('Downloaded output file (%s)' % str(_f), 'sceapi.Scan')
                os.chown(_f_local, job.uid, job.gid)
        except KeyError:
            debug('Failed to download output file (%s)' % str(_f), 'sceapi.Scan')
            error = True
    else:
        debug('Failed to read file (%s)' % output_list, 'sceapi.Scan')
        error = True
    return not error
    
# class job():
#     pass

# j = job()
# job.globalid = "jjNODm1sbJmnBw7jSoCD2RhnABFKDmABFKDm92KKDmABFKDmKSYh4n"
# jobdesc.sessiondir = "/scratch/grid/" + job.globalid
# job.localid = 7001
# job.uid = 500
# job.gid = 500
# job.output_file = "/scratch/jobstatus/job." + job.globalid + ".output"
# from lrms import sceclient, sceapi
# client = sceclient.getHttpClient()
# sceapi.download_output(j,client)
# fd = {"TO_DELETE": "TO_DELETE"}
# h = client.putfiles(job.localid, fd)
