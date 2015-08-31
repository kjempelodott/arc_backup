"""
LSF batch system interface module.
"""

import os, sys, re
import arc
from common.cancel import cancel
from common.config import Config, configure, is_conf_setter
from common.proc import execute_local, execute_remote
from common.log import debug, verbose, info, warn, error, ArcError
from common.scan import *
from common.ssh import ssh_connect
from common.submit import *


@is_conf_setter
def set_lsf(cfg):
    """
    Set LSF specific :py:data:`~lrms.common.Config` attributes.

    :param cfg: parsed arc.conf
    :type cfg: :py:class:`ConfigParser.ConfigParser`
    """

    Config.lsf_bin_path = str(cfg.get('common', 'lsf_bin_path')).strip('"') if cfg.has_option('common', 'lsf_bin_path') else '/usr/bin'

    if cfg.has_option('common', 'lsf_profile_path'):
        Config.lsf_setup = 'source %s &&' % str(cfg.get('common', 'lsf_profile_path')).strip('"')
    else:
        warn('lsf_profile_path not set in arc.conf', 'lsf')
        Config.lsf_setup = ''

    Config.localtransfer = False
    Config.lsf_architecture = str(cfg.get('common', 'lsf_architecture')).strip('"') if cfg.has_option('common', 'lsf_architecture') else ''
            
#---------------------
# Submit methods
#---------------------

def Submit(config, jobdesc):
    """
    Submits a job to the LSF queue specified in arc.conf. This method executes the required
    RunTimeEnvironment scripts and assembles the bash job script. The job script is
    written to file and submitted with ``bsub``.

    :param str config: path to arc.conf
    :param jobdesc: job description object
    :type jobdesc: :py:class:`arc.JobDescription`
    :return: ``True`` if successfully submitted, else ``False``
    :rtype: :py:obj:`bool`
    """

    configure(config, set_lsf)

    validate_attributes(jobdesc)
    if Config.remote_host:
        ssh_connect(Config.remote_host, Config.remote_user, Config.private_key)
        
    # Run RTE stage0
    debug('----- starting lsfSubmitter.py -----', 'lsf.Submit')
    RTE_stage0(jobdesc, 'LSF')

    # Create script file and write job script
    jobscript = get_job_script(jobdesc)
    script_file = write_script_file(jobscript)

    debug('LSF jobname: %s' % jobdesc.Identification.JobName, 'lsf.Submit')
    debug('LSF job script built', 'lsf.Submit')
    debug('----------------- BEGIN job script -----', 'lsf.Submit')
    for line in jobscript.split('\n'):
        debug(line, 'lsf.Submit')
    debug('----------------- END job script -----', 'lsf.Submit')

    if 'ONLY_WRITE_JOBSCRIPT' in os.environ and os.environ['ONLY_WRITE_JOBSCRIPT'] == 'yes':
        return False

    #######################################
    #  Submit the job
    ######################################

    execute = excute_local if not Config.remote_host else execute_remote
    directory = jobdesc.OtherAttributes['joboption;directory']

    debug('Session directory: %s' % directory, 'lsf.Submit')

    LSF_TRIES = 0
    args = '%s %s/bsub < %s' % (Config.lsf_setup, Config.lsf_bin_path, script_file)
    verbose('executing \'%s\' on %s' % (args, Config.remote_host if Config.remote_host else 'localhost'), 'lsf.Submit')
    handle = execute(args)

    job = arc.Job()
    if handle.returncode == 0 and set_job_id(handle, job):

        debug('job submitted successfully!', 'lsf.Submit')
        debug('local job id: %s' % (job.JobID), 'lsf.Submit')
        debug('----- exiting lsfSubmitter.py -----', 'lsf.Submit')

        # TODO: What interface name to use?
        job.ServiceInformationInterfaceName = 'org.nordugrid.slurm.sbatch'
        job.JobStatusInterfaceName = 'org.nordugrid.slurm.sbatch'
        job.JobManagementInterfaceName = 'org.nordugrid.slurm.sbatch'
        # TODO: Change returned endpoints for job.
        # Currently these URLs are not usable.
        endpointURL = arc.common.URL(Config.remote_endpoint)
        job.ServiceInformationURL = arc.common.URL('test://localhost')
        job.JobStatusURL = endpointURL
        job.JobManagementURL = endpointURL
        job.SessionDir  = arc.common.URL('file://' + directory)
        job.StageInDir  = job.SessionDir
        job.StageOutDir = job.SessionDir
        job.IDFromEndpoint = str(job.JobID)
        return True

    debug('job *NOT* submitted successfully!', 'lsf.Submit')
    debug('got error code from bsub: %d !' % handle.returncode, 'lsf.Submit')
    debug('Output is:\n' + ''.join(handle.stdout), 'lsf.Submit')
    debug('Error output is:\n' + ''.join(handle.stderr), 'lsf.Submit')
    debug('----- exiting lsfSubmitter.py -----', 'lsf.Submit')
    return False


def set_job_id(handle, job):
    """
    Read local job ID from ``bsub`` output and set ``JobID`` attribute of job.

    :param object handle: sbatch handle
    :param job: job object 
    :type job: :py:class:`arc.Job`
    :return: ``True`` if found, else ``False``
    :rtype: :py:obj:`str`
    """

    for line in handle.stdout:
        match = re.search(r'Job <(\d+)> .+', line)
        if match:
            job.JobID = match.group(1)
            return True
    return False
    error('Job ID not found in stdout', 'lsf.Submit')


def get_job_script(jobdesc):
    """
    Assemble bash job script for an LSF host.

    :param jobdesc: job description objects
    :type jobdesc: :py:class:`arc.JobDescription`
    :return: job script
    :rtype: :py:obj:`str`
    """

    set_req_mem(jobdesc)
    return JobscriptAssemblerLSF(jobdesc).assemble()


#---------------------
# Cancel methods
#---------------------

def Cancel(config, jobid):
    """
    Cancel a job running at an LSF host with ``bkill``.

    :param str config: path to arc.conf
    :param str grami_file: path to grami file
    :return: ``True`` if successfully cancelled, else ``False``
    :rtype: :py:obj:`bool`
    """

    configure(config, set_lsf)
    return cancel('lsf', jobid)


#---------------------
# Scan methods
#---------------------
    
def Scan(config, ctr_dirs):
    """
    Query the LSF host for all jobs in /[controldir]/processing with ``bjobs``.
    If the job has stopped running, the exit code is read and the 
    diagnostics and comments files are updated. Finally ``gm-kick`` is executed
    on all jobs with an exit code.

    If the exit code can not be read from the diagnostics file, it will (after
    5 tries) be kicked with status UNKNOWN.

    :param str config: path to arc.conf
    :param ctr_dirs: list of paths to control directories 
    :type ctr_dirs: :py:obj:`list` [ :py:obj:`str` ... ]
    """

    configure(config, set_lsf)
    if Config.scanscriptlog:
        scanlogfile = arc.common.LogFile(Config.scanscriptlog)
        arc.common.Logger_getRootLogger().addDestination(scanlogfile)
        arc.common.Logger_getRootLogger().setThreshold(Config.log_threshold)

    jobs = get_jobs(ctr_dirs)
    if not jobs: return
    if Config.remote_host:
        # NOTE: Assuming 256 B of TCP window needed for each job
        ssh_connect(Config.remote_host, Config.remote_user, Config.private_key, (2 << 7)*len(jobs))

    lsf_bin_path = Config.lsf_bin_path
    execute = excute_local if not Config.remote_host else execute_remote
    args = Config.lsf_setup + ' ' + lsf_bin_path + '/bjobs -w -W ' + ' '.join(jobs.keys()) 
    if os.environ.has_key('__LSF_TEST'):
	handle = execute(args, env = dict(os.environ))
    else:
        handle = execute(args)

    def handle_job(info, in_lsf = True):
        job = jobs[info[0]]
        job.state = info[2]
        if job.state in RUNNING:
            if os.path.exists(job.count_file):
                os.remove(job.count_file)
            return

        if set_exit_code_from_diag(job):
            if in_lsf:
                start, end = info[-2:]
                re_date = re.compile(r'^(?P<mm>\d\d)/(?P<dd>\d\d)-(?P<HH>\d\d):(?P<MM>\d\d)')
                job.LRMSStartTime = arc.common.Time(get_MDS(re_date.match(start).groupdict()))
                if end != '-':
                    job.LRMSEndTime = arc.common.Time(get_MDS(re_date.match(end).groupdict()))
                    job.WallTime = job.LRMSEndTime - job.LRMSStartTime
            # Job finished and exitcode found
            job.message = MESSAGES[job.state]
            return
        # else
        add_failure(job)

    # Handle jobs known to LSF
    for line in handle.stdout[1:]:
        try:
            info = line.strip().split()
            assert(len(info) == 15)
            handle_job(info)
        except Exception as e:
            if line:
                warn('Failed to parse bjobs line: %s\n%s' % (line, str(e)), 'lsf.Scan')

    # Handle jobs lost in LSF
    if handle.returncode != 0:
        debug('Got error code %i from bjobs' % handle.returncode, 'lsf.Scan')
        debug('Error output is:\n' + ''.join(handle.stderr), 'lsf.Scan')
        lost_job = re.compile('Job <(\d+)> is not found')
        for line in handle.stderr:
            match = lost_job.match(line)
            if match:
                handle_job([match.groups()[0], None, 'UNKNOWN'], False)

    kicklist = []
    for job in jobs.itervalues():
        if hasattr(job, 'exitcode'):
            with open(job.lrms_done_file, 'w') as f:
                f.write('%d %s\n' % (job.exitcode, job.message))
            write_comments(job)
            update_diag(job)
            kicklist.append(job)
    gm_kick(kicklist)


class JobscriptAssemblerLSF(JobscriptAssembler):

    def __init__(self, jobdesc):
        super(JobscriptAssemblerLSF, self).__init__(jobdesc)

    def assemble(self):
        script = JobscriptAssemblerLSF.assemble_BSUB(self.jobdesc)
        if not script:
            return
        script += self.get_stub('umask_and_sourcewithargs')
        script += self.get_stub('user_env')
        script += self.get_stub('runtime_env')
        script += self.get_stub('move_files_to_node')
        script += self.get_stub('rte_stage1')
        script += self.get_stub('cd_and_run')
        script += self.get_stub('rte_stage2')
        script += self.get_stub('clean_scratchdir')
        script += self.get_stub('move_files_to_frontend')
        return script

    @staticmethod
    def assemble_BSUB(jobdesc):
        """
        Assemble the ``bsub`` specific section of the job script.
        
        :param jobdesc: job description object
        :type jobdesc: :py:class:`arc.JobDescription`
        :return: job script part
        :rtype: :py:obj:`str`
        """
        
        product  = '#!/bin/bash\n'
        product += '#LSF batch job script built by grid-manager\n#\n'

        ### output to comment file
        if 'joboption;directory' in jobdesc.OtherAttributes:
            product += '#BSUB -oo ' + jobdesc.OtherAttributes['joboption;directory'] + '.comment\n\n'

        ### queue
        if jobdesc.Resources.QueueName:
            product += '#BSUB -q ' + jobdesc.Resources.QueueName + '\n'
        if 'joboption;rsl_architecthure' in jobdesc.OtherAttributes:
            product += '#BSUB -R type=' + jobdesc.OtherAttributes['joboption;rsl_architecthure'] + '\n'
        elif Config.lsf_architecture:
            product += '#BSUB -R type=' + Config.lsf_architecture + '\n'

        ### project name
        if 'joboption;rsl_project' in jobdesc.OtherAttributes:
            product += '#BSUB -P ' + jobdesc.OtherAttributes['joboption;rsl_project'] + '\n'

        ### job name
        if jobdesc.Identification.JobName:
            prefix = 'N' if not jobdesc.Identification.JobName[0].isalpha() else ''
            product += '#BSUB -J ' + prefix + \
                       re.sub(r'\W', '_', jobdesc.Identification.JobName)[:15-len(prefix)] + '\n'
        ### job name is required in lsf to get parsable output from bjobs
        else:
            product += '#BSUB -J arcjob\n'

        ### (non-)parallel jobs
        nslots = jobdesc.Resources.SlotRequirement.NumberOfSlots \
                 if jobdesc.Resources.SlotRequirement.NumberOfSlots > 1 else 1
        product += '#BSUB -n ' + str(nslots) + '\n'
        ### parallel jobs
        if jobdesc.Resources.SlotRequirement.SlotsPerHost > 1:
            product += '#BSUB -R span[ptile=' + str(jobdesc.Resources.SlotRequirement.SlotsPerHost) + ']\n'

        ### exclusive execution
        if jobdesc.Resources.SlotRequirement.ExclusiveExecution:
            product += '#BSUB -x\n'

        ### execution times (seconds)
        if jobdesc.Resources.TotalCPUTime.range.max > 0:
            product += '#BSUB -c %g\n' % (jobdesc.Resources.TotalCPUTime.range.max/60.)
        if jobdesc.Resources.IndividualWallTime.range.max > 0:
            product += '#BSUB -W %g\n' % (jobdesc.Resources.IndividualWallTime.range.max/60.)

        ### requested memory(mb)
        if jobdesc.Resources.IndividualPhysicalMemory.max > 0:
            product += '#BSUB -M %i\n' % (jobdesc.Resources.IndividualPhysicalMemory.max*1024)

        ### start time
        if jobdesc.Application.ProcessingStartTime > arc.common.Time(): # now
            date_MDS = re.compile(r'^(?P<YYYY>\d\d\d\d)-(?P<MM>\d\d)-(?P<DD>\d\d)T(?P<hh>\d\d):'
                                    '(?P<mm>\d\d):(?P<ss>\d\d)$')
            f = arc.common.MDSTime
            m = date_MDS.match(jobdesc.Application.ProcessingStartTime(f))
            product += '#BSUB -b %i:%i:%i:%i\n' % (m['MM'], m['DD'], m['HH'], m['mm'])

        return product

