"""
Functions for LSF job handling.
"""

from common.cancel import *
from common.common import *
from common.config import *
from common.proc import *
from common.scan import *
from common.submit import *

def set_lsf(cfg):
    """
    Fill :py:data:`~lrms.common.common.Config` with LSF-specific options.

    :param cfg: parsed config (from :py:meth:`~lrms.common.config.get_parsed_config`)
    :type cfg: :py:class:`ConfigParser.ConfigParser`
    """

    Config.lsf_bin_path = str(cfg.get('common', 'lsf_bin_path')).strip('"') if \
                          cfg.has_option('common', 'lsf_bin_path') else '/usr/bin'

    if cfg.has_option('common', 'lsf_profile_path'):
        Config.lsf_cmd = 'source %s &&' % str(cfg.get('common', 'lsf_profile_path')).strip('"')
    else:
        log(arc.WARNING, 'lsf_profile_path not set in arc.conf', 'lsf')
        Config.lsf_cmd = ''

    Config.localtransfer = False
    Config.lsf_architecture = str(cfg.get('common', 'lsf_architecture')).strip('"') if \
                              cfg.has_option('common', 'lsf_architecture') else ''
            
#---------------------
# Submit methods
#---------------------

def Submit(config, jobdescs, jc):
    """
    Submit a job to LSF.

    :param str config: path to arc.conf
    :param jobdescs: job description list object
    :type jobdescs: :py:class:`arc.JobDescriptionList (Swig Object of type 'Arc::JobDescriptionList *')`
    :param jc: job container object
    :type jc: :py:class:`arc.compute.JobContainer (Swig Object of type Arc::EntityContainer< Arc::Job > *')`
    :return: ``True`` if successfully submitted, else ``False``
    :rtype: :py:obj:`bool`
    """

    cfg = get_parsed_config(config)
    set_common(cfg)
    set_gridmanager(cfg)
    set_cluster(cfg)
    set_queue(cfg)
    set_lsf(cfg)

    jd = jobdescs[0]
    validate_attributes(jd)
        
    # Run RTE stage0
    log(arc.DEBUG, '----- starting lsfSubmitter.py -----', 'lsf.Submit')
    RTE_stage0(jobdescs, 'LSF')

    # Create script file and write job script
    jobscript = get_job_script(jobdescs)
    script_file = write_script_file(jobscript)

    log(arc.DEBUG, 'LSF jobname: %s' % jd.Identification.JobName, 'lsf.Submit')
    log(arc.DEBUG, 'LSF job script built', 'lsf.Submit')
    log(arc.DEBUG, '----------------- BEGIN job script -----', 'lsf.Submit')
    for line in jobscript.split('\n'):
        log(arc.DEBUG, line, 'lsf.Submit')
    log(arc.DEBUG, '----------------- END job script -----', 'lsf.Submit')

    if 'ONLY_WRITE_JOBSCRIPT' in os.environ and os.environ['ONLY_WRITE_JOBSCRIPT'] == 'yes':
        return False

    #######################################
    #  Submit the job
    ######################################

    execute = excute_local if not Config.remote_host else execute_remote
    directory = jd.OtherAttributes['joboption;directory']

    log(arc.DEBUG, 'Session directory: %s' % directory, 'lsf.Submit')

    LSF_TRIES = 0
    args = '%s %s/bsub < %s' % (Config.lsf_cmd, Config.lsf_bin_path, script_file)
    log(arc.common.VERBOSE, 'executing \'%s\' on %s' % 
        (args, Config.remote_host if Config.remote_host else 'localhost'), 'lsf.Submit')
    bsub_handle = execute(args)
    if bsub_handle.returncode == 0:

        jobID = get_job_id(bsub_handle)
        log(arc.DEBUG, 'job submitted successfully!', 'lsf.Submit')
        log(arc.DEBUG, 'local job id: %s' % (jobID), 'lsf.Submit')
        log(arc.DEBUG, '----- exiting lsfSubmitter.py -----', 'lsf.Submit')

        endpointURL = arc.common.URL(Config.remote_endpoint)
        newJob = arc.Job()
        newJob.JobID = endpointURL.Host() + ':' + jobID if endpointURL.Host() else jobID
        # TODO: What interface name to use?
        newJob.ServiceInformationInterfaceName = 'org.nordugrid.slurm.sbatch'
        newJob.JobStatusInterfaceName = 'org.nordugrid.slurm.sbatch'
        newJob.JobManagementInterfaceName = 'org.nordugrid.slurm.sbatch'
        # TODO: Change returned endpoints for job.
        # Currently these URLs are not usable.
        newJob.ServiceInformationURL = arc.common.URL('test://localhost')
        newJob.JobStatusURL = endpointURL
        newJob.JobManagementURL = endpointURL
        newJob.SessionDir  = arc.common.URL('file://' + directory)
        newJob.StageInDir  = newJob.SessionDir
        newJob.StageOutDir = newJob.SessionDir
        newJob.IDFromEndpoint = str(jobID)
        jc.addEntity(newJob)
        return True

    else:
        log(arc.DEBUG, 'job *NOT* submitted successfully!', 'lsf.Submit')
        log(arc.DEBUG, 'got error code from bsub: %d !' % bsub_handle.returncode, 'lsf.Submit')

    log(arc.DEBUG, 'Output is:', 'lsf.Submit')
    log(arc.DEBUG, '\n'.join(bsub_handle.stdout), 'lsf.Submit')
    log(arc.DEBUG, 'Error output is:', 'lsf.Submit')
    log(arc.DEBUG, '\n'.join(bsub_handle.stderr), 'lsf.Submit')
    log(arc.DEBUG, '----- exiting lsfSubmitter.py -----', 'lsf.Submit')
    return False


def get_job_id(handle):
    """
    Get local job ID from bsub stdout or stderr.

    :param handle: list of job description objects
    :type handle: :py:obj:`subprocess.Popen` or :py:data:`~lrms.common.common.Config`
    :return: local ID
    :rtype: :py:obj:`str`
    """

    for line in handle.stdout:
        match = re.search(r'Job <(\d+)> .+', line)
        if match:
            return match.group(1)
    raise ArcError('Job ID not found in stdout', 'lsf.Submit')


def get_job_script(jobdescs):
    """
    Get the complete bsub job script.

    :param jobdescs: list of job description objects
    :type jd: :py:obj:`list` [ :py:class:`arc.JobDescription (Swig Object of type Arc::JobDescription *')` ... ]
    :return: job script
    :rtype: :py:obj:`str`
    """

    set_req_mem(jobdescs)

    jd = jobdescs[0]
    jobscript = assemble_BSUB(jd)

    jobscript += \
        '# Overide umask of execution node ' \
        '(sometime values are really strange)\n' \
        'umask 077\n' \
        ' \n' \
        '# source with arguments for DASH shells\n' \
        'sourcewithargs() {\n' \
        'script=$1\n' \
        'shift\n' \
        '. $script\n' \
        '}\n'

    jobscript += add_user_env(jobdescs)
    jobsessiondir = jd.OtherAttributes['joboption;directory'].rstrip('/')
    gridid = jd.OtherAttributes['joboption;gridid']

    if Config.shared_filesystem:
        jobscript += setup_runtime_env(jobdescs)
    else:
        runtime_stdin = jd.Application.Input[len(jobsessiondir) + 1:] \
                        if jd.Application.Input.startswith(jobsessiondir +'/') \
                        else jd.Application.Input
        if runtime_stdin  != jd.Application.Input:
            runtime_stdin = '%s/%s/%s' % (Config.scratchdir, gridid, runtime_stdin)

        runtime_stdout = jd.Application.Output[len(jobsessiondir) + 1:] \
                         if jd.Application.Output.startswith(jobsessiondir + '/') \
                         else jd.Application.Output
        if runtime_stdout != jd.Application.Output:
            runtime_stdout = '%s/%s/%s' % (Config.scratchdir, gridid, runtime_stdout)

        runtime_stderr = jd.Application.Error[len(jobsessiondir) + 1:] \
                         if jd.Application.Error.startswith(jobsessiondir + '/') \
                         else jd.Application.Error
        if runtime_stderr != jd.Application.Error:
            runtime_stderr = '%s/%s/%s' % (Config.scratchdir, gridid, runtime_stderr)

        scratchdir = (Config.scratchdir, gridid)
        jobscript += 'RUNTIME_JOB_DIR=%s/%s\n' % scratchdir + \
                     'RUNTIME_JOB_DIAG=%s/%s.diag\n' % scratchdir + \
                     'RUNTIME_JOB_STDIN="%s"\n'  % runtime_stdin + \
                     'RUNTIME_JOB_STDOUT="%s"\n' % runtime_stdout + \
                     'RUNTIME_JOB_STDERR="%s"\n' % runtime_stderr

    jobscript += move_files_to_node(jobdescs)
    jobscript += '\nRESULT=0\n\n' \
                 '# Changing to session directory\n' \
                 'cd $RUNTIME_JOB_DIR\n' \
                 'export HOME=$RUNTIME_JOB_DIR\n\n' \
                 'if [ "$RESULT" = \'0\' ] ; then\n' \
                 'echo "runtimeenvironments=$runtimeenvironments" ' \
                 '>> "$RUNTIME_JOB_DIAG"\n'

    jobscript += RTE_stage1(jobdescs)
    if not Config.shared_filesystem:
        raise ArcError('Nodes detached from gridarea are not supported when LSF is used. '
                       'Aborting job submit', 'lsf.Submit')
    jobscript += cd_and_run(jobdescs)
    jobscript += 'fi\n'
    jobscript += configure_runtime(jobdescs)
    jobscript += move_files_to_frontend()
    return jobscript


#---------------------
# Cancel methods
#---------------------

def Cancel(config, jobid):
    """
    Cancel an LSF job.

    :param str config: path to arc.conf
    :param str jobid: local job ID
    :return: ``True`` if successfully cancelled, else ``False``
    :rtype: :py:obj:`bool`
    """

    cfg = get_parsed_config(config)
    set_gridmanager(cfg)
    set_lsf(cfg)

    cmd = '%s %s/bkill -s 9 %s' % (Config.lsf_cmd, Config.lsf_bin_path, jobid)
    return cancel(cmd, jobid, 'lsf')


#---------------------
# Scan methods
#---------------------
    
def Scan(config, ctr_dirs):
    """
    Scan and update status of LSF jobs.

    :param str config: path to arc.conf
    :param ctr_dirs: list of paths to control directories 
    :type ctr_dirs: :py:obj:`list` [ :py:obj:`str` ... ]
    """

    time.sleep(10)

    cfg = get_parsed_config(config)
    set_gridmanager(cfg)
    set_lsf(cfg)

    if Config.scanscriptlog:
        scanlogfile = arc.common.LogFile(Config.scanscriptlog)
        arc.common.Logger_getRootLogger().addDestination(scanlogfile)

    jobs = get_jobs(ctr_dirs)
    process_jobs(jobs)


def process_jobs(jobs):
    """
    Check if jobs are still running. If job is finished, get the exit code,
    update the ``lrms_done_file`` and write to the ``comments_file``.

    :param job: list of job objects (from :py:meth:`~lrms.common.scan.get_jobs`)
    :type job: :py:obj:`list` [ :py:class:`~lrms.common.common.Object` ... ]
    """

    if not jobs: return

    lsf_bin_path = Config.lsf_bin_path
    execute = excute_local if not Config.remote_host else execute_remote
    args = Config.lsf_cmd + ' ' + lsf_bin_path + '/bjobs -w -W ' + ' '.join(jobs.keys()) 
    if os.environ.has_key('__LSF_TEST'):
	handle = execute(args, env = dict(os.environ))
    else:
        handle = execute(args)

    def kick_job(job):
        with open(job.lrms_done_file, 'w') as f:
            f.write('%d %s\n' % (job.exitcode, job.message))
        write_comments(job)
        update_diag(job)
        gm_kick([job])

    def handle_job(job, stats = []):

        if job.state in RUNNING:
            if os.path.exists(job.count_file):
                os.remove(job.count_file)
            return

        if set_exit_code_from_diag(job):
            if stats:
                start, end = stats[-2:]
                re_date = re.compile(r'^(?P<mm>\d\d)/(?P<dd>\d\d)-(?P<HH>\d\d):(?P<MM>\d\d)')
                job.LRMSStartTime = arc.common.Time(get_MDS(re_date.match(start).groupdict()))
                if end != '-':
                    job.LRMSEndTime = arc.common.Time(get_MDS(re_date.match(end).groupdict()))
                    job.WallTime = job.LRMSEndTime - job.LRMSStartTime
        else:
            raise Exception
        # Job finished and exitcode found
        job.message = MESSAGES[job.state]
        kick_job(job)

    # Handle jobs lost in LSF
    if handle.returncode != 0:
        log(arc.DEBUG, 'Got error code %i from bjobs' % handle.returncode, 'lsf.Scan')
        log(arc.DEBUG, 'Error output is:\n' + '\n'.join(handle.stderr), 'lsf.Scan')
        lost_job = re.compile('Job <(\d+)> is not found')
        for line in handle.stderr:
            try:
                job = jobs[lost_job.match(line).groups()[0]]
                job.state = 'UNKNOWN'
                handle_job(job)
            except:
                add_failure(job)
                # After 5 failures, job gets exitcode -1
                if hasattr(job, 'exitcode'):
                    kick_job(job)

    # Handle jobs in LSF
    for line in handle.stdout[1:]:
        try:
            stats = line.strip().split()
            assert(len(stats) == 15)
        except:
            if line:
                log(arc.WARNING, 'Failed to parse bjobs line: ' + line, 'lsf.Scan')
            continue
        try:
            job = jobs[stats[0]]
            job.state = stats[2]
            handle_job(job, stats)
        except:
            add_failure(job)
            # After 5 failures, job gets exitcode -1
            if hasattr(job, 'exitcode'):
                kick_job(job)


def assemble_BSUB(j):
    """
    Get bsub section of job script.

    :param jobdescs: job description object
    :type jobdescs: :py:class:`arc.JobDescription (Swig Object of type 'Arc::JobDescription *')`
    :return: jobscript section
    :rtype: :py:obj:`str`
    """

    product  = '#!/bin/bash\n'
    product += '#LSF batch job script built by grid-manager\n#\n'

    ### output to comment file
    if 'joboption;directory' in j.OtherAttributes:
        product += '#BSUB -oo ' + j.OtherAttributes['joboption;directory'] + '.comment\n\n'

    ### queue
    if j.Resources.QueueName:
        product += '#BSUB -q ' + j.Resources.QueueName + '\n'
    if 'joboption;rsl_architecthure' in j.OtherAttributes:
        product += '#BSUB -R type=' + j.OtherAttributes['joboption;rsl_architecthure'] + '\n'
    elif Config.lsf_architecture:
        product += '#BSUB -R type=' + Config.lsf_architecture + '\n'

    ### project name
    if 'joboption;rsl_project' in j.OtherAttributes:
        product += '#BSUB -P ' + j.OtherAttributes['joboption;rsl_project'] + '\n'

    ### job name
    if j.Identification.JobName:
        prefix = 'N' if not j.Identification.JobName[0].isalpha() else ''
        product += '#BSUB -J ' + prefix + \
                   re.sub(r'\W', '_', j.Identification.JobName)[:15-len(prefix)] + '\n'
    ### job name is required in lsf to get parsable output from bjobs
    else:
        product += '#BSUB -J arcjob\n'

    ### (non-)parallel jobs
    nslots = j.Resources.SlotRequirement.NumberOfSlots \
             if j.Resources.SlotRequirement.NumberOfSlots > 1 else 1
    product += '#BSUB -n ' + str(nslots) + '\n'
    ### parallel jobs
    if j.Resources.SlotRequirement.SlotsPerHost > 1:
        product += '#BSUB -R span[ptile=' + str(j.Resources.SlotRequirement.SlotsPerHost) + ']\n'

    ### exclusive execution
    if j.Resources.SlotRequirement.ExclusiveExecution:
        product += '#BSUB -x\n'

    ### execution times (seconds)
    if j.Resources.TotalCPUTime.range.max > 0:
        product += '#BSUB -c %g\n' % (j.Resources.TotalCPUTime.range.max/60.)
    if j.Resources.IndividualWallTime.range.max > 0:
        product += '#BSUB -W %g\n' % (j.Resources.IndividualWallTime.range.max/60.)

    ### requested memory(mb)
    if j.Resources.IndividualPhysicalMemory.max > 0:
        product += '#BSUB -M %i\n' % (j.Resources.IndividualPhysicalMemory.max*1024)

    ### start time
    if j.Application.ProcessingStartTime > arc.common.Time(): # now
        date_MDS = re.compile(r'^(?P<YYYY>\d\d\d\d)-(?P<MM>\d\d)-(?P<DD>\d\d)T(?P<hh>\d\d):'
                                '(?P<mm>\d\d):(?P<ss>\d\d)$')
        f = arc.common.MDSTime
        m = date_MDS.match(j.Application.ProcessingStartTime(f))
        product += '#BSUB -b %i:%i:%i:%i\n' % (m['MM'], m['DD'], m['HH'], m['mm'])
        
    return product
        
