"""
SLURM batch system interface module.
"""

import os, sys, time, re
import arc
from common.cancel import cancel
from common.config import Config, configure, is_conf_setter
from common.proc import execute_local, execute_remote
from common.log import debug, verbose, info, warn, error, ArcError
from common.scan import *
from common.ssh import ssh_connect
from common.submit import *


@is_conf_setter
def set_slurm(cfg):
    """
    Set SLURM specific :py:data:`~lrms.common.Config` attributes.

    :param cfg: parsed arc.conf
    :type cfg: :py:class:`ConfigParser.ConfigParser`
    """
    Config.slurm_bin_path = str(cfg.get('common', 'slurm_bin_path')).strip('"') if \
        cfg.has_option('common', 'slurm_bin_path') else '/usr/bin'
    Config.slurm_wakeupperiod = int(cfg.get('common', 'slurm_wakeupperiod')).strip('"') if \
        cfg.has_option('common', 'slurm_wakeupperiod') else 30

            
#---------------------
# Submit methods
#---------------------

def Submit(config, jobdesc):
    """
    Submits a job to the SLURM queue specified in arc.conf. This method executes the required
    RunTimeEnvironment scripts and assembles the bash job script. The job script is
    written to file and submitted with ``sbatch``.

    :param str config: path to arc.conf
    :param jobdesc: job description object
    :type jobdesc: :py:class:`arc.JobDescription`
    :return: local job ID if successfully submitted, else ``None``
    :rtype: :py:obj:`str`
    """

    configure(config, set_slurm)

    validate_attributes(jobdesc)
    if Config.remote_host:
        ssh_connect(Config.remote_host, Config.remote_user, Config.private_key)
        
    # Run RTE stage0
    debug('----- starting slurmSubmitter.py -----', 'slurm.Submit')
    RTE_stage0(jobdesc, 'SLURM', SBATCH_ACCOUNT = 'OtherAttributes.SBATCH_ACCOUNT')

    # Create script file and write job script
    jobscript = get_job_script(jobdesc)
    script_file = write_script_file(jobscript)

    debug('SLURM jobname: %s' % jobdesc.Identification.JobName, 'slurm.Submit')
    debug('SLURM job script built', 'slurm.Submit')
    debug('----------------- BEGIN job script -----', 'slurm.Submit')
    for line in jobscript.split('\n'):
        debug(line, 'slurm.Submit')
    debug('----------------- END job script -----', 'slurm.Submit')

    if 'ONLY_WRITE_JOBSCRIPT' in os.environ and os.environ['ONLY_WRITE_JOBSCRIPT'] == 'yes':
        return

    #######################################
    #  Submit the job
    ######################################

    execute = execute_local if not Config.remote_host else execute_remote
    directory = jobdesc.OtherAttributes['joboption;directory']

    debug('Session directory: %s' % directory, 'slurm.Submit')

    SLURM_TRIES = 0
    handle = None
    while SLURM_TRIES < 10:
        args = '%s/sbatch %s' % (Config.slurm_bin_path, script_file)
        verbose('Executing \'%s\' on %s' % 
                (args, Config.remote_host if Config.remote_host else 'localhost'), 'slurm.Submit')
        handle = execute(args)
        if handle.returncode == 0:
            break
        if handle.returncode == 198 or wait_for_queue(handle):
            debug('Waiting for queue to decrease', 'slurm.Submit')
            time.sleep(60)
            SLURM_TRIES += 1
            continue
        break # Other error than full queue

    if handle.returncode == 0:
        # TODO: Test what happens when the jobqueue is full or when the slurm
        # ctld is not responding. SLURM 1.x and 2.2.x outputs the jobid into 
        # STDERR and STDOUT respectively. Concat them, and let sed sort it out. 
        # From the exit code we know that the job was submitted, so this
        # is safe. Ulf Tigerstedt <tigerste@csc.fi> 1.5.2011 
        localid = get_job_id(handle)
        if localid:
            debug('Job submitted successfully!', 'slurm.Submit')
            debug('Local job id: ' + localid, 'slurm.Submit')
            debug('----- exiting submitSubmitter.py -----', 'slurm.Submit')
            return localid

    debug('job *NOT* submitted successfully!', 'slurm.Submit')
    debug('got error code from sbatch: %d !' % handle.returncode, 'slurm.Submit')
    debug('Output is:\n' + ''.join(handle.stdout), 'slurm.Submit')
    debug('Error output is:\n' + ''.join(handle.stderr), 'slurm.Submit')
    debug('----- exiting slurmSubmitter.py -----', 'slurm.Submit')


def wait_for_queue(handle):
    """
    Read from ``sbatch`` output whether the queue is full.

    :param object handle: sbatch handle
    :return: ``True`` if queue is full, else ``False``
    :rtype: :py:obj:`bool`
    """
    
    for f in (handle.stdout, handle.stderr):
        for line in f:
            if ("maximum number of jobs" in line or
                # A rare SLURM error, but may cause chaos in the 
                # information/accounting system
                "unable to accept job" in line):
                return True
    return False


def get_job_id(handle):
    """
    Read local job ID from ``sbatch`` output.

    :param object handle: sbatch handle
    :return: local job ID if found, else ``None``
    :rtype: :py:obj:`str`
    """

    for f in (handle.stdout, handle.stderr):
        for line in f:
            match = re.search(r'Submitted batch job (\d+)', line)
            if match:
                return match.group(1)
    error('Job ID not found in stdout or stderr', 'slurm.Submit')


def get_job_script(jobdesc):
    """
    Assemble bash job script for a SLURM host.

    :param jobdesc: job description object
    :type jobdesc: :py:class:`arc.JobDescription`
    :return: job script
    :rtype: :py:obj:`str`
    """

    set_req_mem(jobdesc)

    # TODO: Maybe change way in which JobDescriptionParserSLURM is loaded.
    jobscript = JobscriptAssemblerSLURM(jobdesc).assemble()
    if not jobscript:
        raise ArcError('Unable to assemble SLURM job option', 'slurm.Submit')
    return jobscript


#---------------------
# Cancel methods
#---------------------

def Cancel(config, jobid):
    """
    Cancel a job running at a SLURM host with ``scancel``.

    :param str config: path to arc.conf
    :param str jobid: local job ID
    :return: ``True`` if successfully cancelled, else ``False``
    :rtype: :py:obj:`bool`
    """

    verify_job_id(jobid)
    configure(config, set_slurm)
    return cancel('slurm', jobid)


def verify_job_id(jobid):
    """
    Verify that the job ID is an integer else raise :py:class:`~lrms.common.common.ArcError`.

    :param str jobid: local job ID
    """

    try:
        int(jobid)
    except:
        raise ArcError('Job ID is not set, or it contains non-numeric characters (%s)' % jobid, 'slurm.Cancel')


#---------------------
# Scan methods
#---------------------
    
def Scan(config, ctr_dirs):
    """
    Query the SLURM host for all jobs in /[controldir]/processing with ``squeue``.
    If the job has stopped running, more detailed information is fetched with ``scontrol``,
    and the diagnostics and comments files are updated. Finally ``gm-kick`` is executed
    on all jobs with an exit code.

    :param str config: path to arc.conf
    :param ctr_dirs: list of paths to control directories 
    :type ctr_dirs: :py:obj:`list` [ :py:obj:`str` ... ]
    """
    
    configure(config, set_slurm)
    if Config.scanscriptlog:
        scanlogfile = arc.common.LogFile(Config.scanscriptlog)
        arc.common.Logger_getRootLogger().addDestination(scanlogfile)
        arc.common.Logger_getRootLogger().setThreshold(Config.log_threshold)

    jobs = get_jobs(ctr_dirs)
    if not jobs: return
    if Config.remote_host:
        # NOTE: Assuming 256 B of TCP window needed for each job (squeue)
        ssh_connect(Config.remote_host, Config.remote_user, Config.private_key, (2 << 7)*len(jobs)) 

    execute = execute_local if not Config.remote_host else execute_remote
    args = Config.slurm_bin_path + '/squeue -a -h -o %i:%T -t all -j ' + ','.join(jobs.keys())
    if os.environ.has_key('__SLURM_TEST'):
        handle = execute(args, env=dict(os.environ))
    else:
        handle = execute(args)
    if handle.returncode != 0:
        debug('Got error code %i from squeue' % handle.returncode, 'slurm.Scan')
        debug('Error output is:\n' + ''.join(handle.stderr), 'slurm.Scan')

    # Slurm can report StartTime and EndTime in at least these two formats:
    # 2010-02-15T15:30:29 (MDS)
    # 02/15-15:25:15
    # Python does not support duplicate named groups.
    # Have to use separate regex if we want to use named groups.
    date_MDS = re.compile(r'^(?P<YYYY>\d\d\d\d)-(?P<mm>\d\d)-(?P<dd>\d\d)T(?P<HH>\d\d):(?P<MM>\d\d):(?P<SS>\d\d)$')
    date_2 = re.compile(r'^(?P<mm>\d\d)/(?P<dd>\d\d)-(?P<HH>\d\d):(?P<MM>\d\d):(?P<SS>\d\d)$')

    for line in handle.stdout:
        try:
            localid, state = line.strip().split(':', 1)
        except:
            if line:
                warn('Failed to parse squeue line: ' + line, 'slurm.Scan')
            continue
        job = jobs[localid]
        job.state = state 
        if job.state in RUNNING:
            continue

        if not job.state:
            set_exit_code_from_diag(job)
        job.message = MESSAGES.get(job.state, '')

        args = Config.slurm_bin_path + '/scontrol -o show job %s' % localid
        scontrol_handle = execute(args)
        if scontrol_handle.returncode != 0:
            debug('Got error code %i from scontrol' % scontrol_handle.returncode, 'slurm.Scan')
            debug('Error output is:\n' + ''.join(scontrol_handle.stderr), 'slurm.Scan')

        try:
            scontrol_dict = dict(item.split('=', 1) for item in re.split(' (?=[^ =]+=)', scontrol_handle.stdout[0]))
            job = jobs[scontrol_dict['JobId']]
        except:
            warn('Failed to parse scontrol line: ' + line, 'slurm.Scan')
            continue

        if 'ExitCode' in scontrol_dict:
            ec1, ec2 = scontrol_dict['ExitCode'].split(':')
            job.exitcode = int(ec2) + 256 if int(ec2) != 0 else int(ec1)
        else:
            job.exitcode = 0 if state == 'COMPLETED' else -1

        if (state == 'NODE_FAIL' or state == 'CANCELLED') and ('ExitCode' not in scontrol_dict or job.exitcode == 0):
            job.exitcode = 15
            job.message = 'Job was cancelled by SLURM'

        if 'StartTime' in scontrol_dict:
            match = date_MDS.match(scontrol_dict['StartTime']) or date_2.match(scontrol_dict['StartTime'])
            scontrol_dict['StartTime'] = get_MDS(match.groupdict())
            job.LRMSStartTime = arc.common.Time(scontrol_dict['StartTime'])
        if 'EndTime' in scontrol_dict:
            match = date_MDS.match(scontrol_dict['EndTime']) or date_2.match(scontrol_dict['EndTime'])
            scontrol_dict['EndTime'] = get_MDS(match.groupdict())
            job.LRMSEndTime = arc.common.Time(scontrol_dict['EndTime'])

        if 'StartTime' in scontrol_dict and 'EndTime' in scontrol_dict:
            job.WallTime = job.LRMSEndTime - job.LRMSStartTime

        if 'NumCPUs' in scontrol_dict:
            job.Processors = scontrol_dict['NumCPUs']
            
        with open(job.lrms_done_file, 'w') as f:
            f.write('%d %s\n' % (job.exitcode, job.message))
        write_comments(job)
        update_diag(job)

    kicklist = [job for job in jobs.itervalues() if job.state not in RUNNING]
    kicklist.extend([job for job in jobs.itervalues() if job.state == 'CANCELLED']) # kick twice
    gm_kick(kicklist)


### \mapname slurm_sbatch SLURM sbatch options
### SLURM sbatch options.
class JobscriptAssemblerSLURM(JobscriptAssembler):

    def __init__(self, jobdesc):
        super(JobscriptAssemblerSLURM, self).__init__(jobdesc)

    def assemble(self):
        script = JobscriptAssemblerSLURM.assemble_SBATCH(self.jobdesc)
        if not script:
            return
        script += self.get_stub('umask_and_sourcewithargs')
        script += self.get_stub('user_env')
        if Config.localtransfer:
            script += self.get_stub('local_transfer')
        script += self.get_stub('runtime_env')
        script += self.get_stub('move_files_to_node')
        if Config.localtransfer:
            script += self.get_stub('download_input_files')
        script += self.get_stub('rte_stage1')
        script += self.get_stub('slurm_nodefile')
        script += self.get_stub('cd_and_run')
        script += self.get_stub('rte_stage2')
        if Config.localtransfer:
            script += self.get_stub('upload_output_files')
        else:
            script += self.get_stub('clean_scratchdir')
        script += self.get_stub('move_files_to_frontend')
        return script

    @staticmethod
    def assemble_SBATCH(j, language = "", dialect = ""):
        # TODO: What about localtransfer, adjusting working directory,
        #       diagnostics, and uploading output files. These should
        #       probably be handled by submisison script.
        
        # First check if the job description is valid.
        #~ if not j.Application.Executable.Path:
        #~    logger.msg(arc.DEBUG, "Missing executable")
        #~    return (False, "")
    
        product  = "#!/bin/bash -l\n"
        product += "# SLURM batch job script built by grid-manager\n" # << TODO
        
        # TODO: Make configurable    
        # rerun is handled by GM, do not let SLURM requeue jobs itself.
        product += "#SBATCH --no-requeue\n"
    
        # TODO: Description is missing
        # TODO: Maybe output and error file paths should be passed using
        # the separately map, instead of using the session_directory key.
        # write SLURM output to 'comment' file
        if "joboption;directory" in j.OtherAttributes:
            product += "#SBATCH -e " + j.OtherAttributes["joboption;directory"] + ".comment\n"
            product += "#SBATCH -o " + j.OtherAttributes["joboption;directory"] + ".comment\n"
            product += "\n"
        
        ### Choose queue ###
        # SLURM v2.3 option description: -p, --partition=<partition_names>
        # Request a specific partition for the resource allocation. If not
        # specified, the default behavior is to allow the slurm controller to
        # select the default partition as designated by the system 
        # administrator. If the job can use more than one partition, specify
        # their names in a comma separate list and the one offering earliest
        # initiation will be used.
        ### \mapattr --partition <- QueueName
        if j.Resources.QueueName:
            product += "#SBATCH -p " + j.Resources.QueueName + "\n"
        
        ### Project name for accounting ###
        # SLURM v2.4 option description: -A, --account=<account>
        # Charge resources used by this job to specified account. The account is
        # an arbitrary string. The account name may be changed after job
        # submission using the scontrol command.
        #
        # The short option was renamed from '-U' to '-A' in SLURM v2.1.
        ### \mapattr --account <- OtherAttributes
        if "joboption;rsl_project" in j.OtherAttributes:
            product += "#SBATCH -A " + j.OtherAttributes["joboption;rsl_project"] + "\n"
        elif 'SBATCH_ACCOUNT' in j.OtherAttributes:
            product += "#SBATCH -A " + j.OtherAttributes['SBATCH_ACCOUNT'] + "\n"

        ### Job name for convenience ###
        # SLURM v2.3 option description: -J, --job-name=<jobname>
        # Specify a name for the job allocation. The specified name will appear
        # along with the job id number when querying running jobs on the system.
        # The default is the name of the batch script, or just "sbatch" if the
        # script is read on sbatch's standard input.
        ### \mapattr --job-name <- JobName
        if j.Identification.JobName:
            # TODO: is this necessary? do parts of the infosys need these 
            # limitations? # 's/^\([^[:alpha:]]\)/N\1/' # Prefix with 'N' if 
            # starting with non-alpha character. 's/[^[:alnum:]]/_/g' 
            # Replace all non-letters and numbers with '_'.
            # 's/\(...............\).*/\1/' # Limit to 15 characters.
            prefix = 'N' if not j.Identification.JobName[0].isalpha() else ''
            product += "#SBATCH -J '" +  prefix + re.sub(r'\W', '_', j.Identification.JobName)[:15-len(prefix)] + "'\n"
        else:
            product += "#SBATCH -J 'gridjob'\n"
    
    
        # Set up the user's environment on the compute node where the script is
        # executed. SLURM v2.3 option description: 
        # --get-user-env[=timeout][mode]. This option will tell sbatch to 
        # retrieve the login environment variables for the user specified in the
        # --uid option. The environment variables are retrieved by running 
        # something of this sort: "su - <username> -c /usr/bin/env" and parsing 
        # the output. Be aware that any environment variables already
        # set in sbatch's environment will take precedence over any environment
        # variables in the user's login environment. Clear any environment
        # variables before calling sbatch that you do not want propagated to the
        # spawned program. The optional timeout value is in seconds. Default 
        # value is 8 seconds. The optional mode value control the "su" options. 
        # With a mode value of "S", "su" is executed without the "-" option. 
        # With a mode value of "L", "su" is executed with the "-" option, 
        # replicating the login environment. If mode not specified, the mode
        # established at SLURM build time is used. Example of use include 
        # "--get-user-env", "--get-user-env=10", "--get-user-env=10L", and
        # "--get-user-env=S". This option was originally created for use by 
        # Moab.
        product += "#SBATCH --get-user-env=10L\n"


        ### (non-)parallel jobs ###
        # SLURM v2.3 option description: -n, --ntasks=<number>
        # sbatch does not launch tasks, it requests an allocation of resources
        # and submits a batch script. This option advises the SLURM controller
        # that job steps run within the allocation will launch a maximum of
        # number tasks and to provide for sufficient resources. The default is
        # one task per node, but note that the --cpus-per-task option will
        # change this default.
        ### \mapattr --ntasks <- NumberOfSlots
        nslots = j.Resources.SlotRequirement.NumberOfSlots \
            if j.Resources.SlotRequirement.NumberOfSlots > 1 else 1
        product += "#SBATCH -n " + str(nslots) + "\n"


        ### SLURM v2.3 option description: --ntasks-per-node=<ntasks>
        # Request the maximum ntasks be invoked on each node. Meant to be used
        # with the --nodes option. This is related to --cpus-per-task=ncpus,
        # but does not require knowledge of the actual number of cpus on each
        # node. In some cases, it is more convenient to be able to request that
        # no more than a specific number of tasks be invoked on each node.
        # Examples of this include submitting a hybrid MPI/OpenMP app where only
        # one MPI "task/rank" should be assigned to each node while allowing the
        # OpenMP portion to utilize all of the parallelism present in the node,
        # or submitting a single setup/cleanup/monitoring job to each node of a
        # pre-existing allocation as one step in a larger job script.
        ### \mapattr --ntasks-per-node <- SlotsPerHost
        if j.Resources.SlotRequirement.SlotsPerHost > 1:
            product += "#SBATCH --ntasks-per-node " + \
                str(j.Resources.SlotRequirement.SlotsPerHost) + "\n"


        # Node properties: Set by e.g. RTE script in stage 0.
        if "joboption;nodeproperty" in j.OtherAttributes:
            product += "#SBATCH " + j.OtherAttributes["joboption;nodeproperty"]
        product += "\n"


        # SLURM v2.3 option description: --exclusive
        # The job allocation can not share nodes with other running jobs. This
        # is the opposite of --share, whichever option is seen last on the
        # command line will be used. The default shared/exclusive behavior
        # depends on system configuration and the partition's Shared option take
        # s precedence over the job's option.
        ### \mapattr --exclusive <- ExclusiveExecution
        if j.Resources.SlotRequirement.ExclusiveExecution:
            product += "#SBATCH --exclusive\n"


        ### Execution times (minutes) ###
        # SLURM v2.4 option description: -t, --time=<time>
        # Set a limit on the total run time of the job allocation. If the
        # requested time limit exceeds the partition's time limit, the job will
        # lbe eft in a PENDING state (possibly indefinitely). The default time
        # limit is the partition's default time limit. When the time limit is
        # reached, each task in each job step is sent SIGTERM followed by
        # SIGKILL. The interval between signals is specified by the SLURM
        # configuration parameter KillWait. A time limit of zero requests that
        # no time limit be imposed. Acceptable time formats include "minutes",
        # "minutes:seconds", "hours:minutes:seconds", "days-hours",
        # "days-hours:minutes" and "days-hours:minutes:seconds".
        #
        # Mapping-wise it seems that 'total run time of the job allocation' is
        # equivalent with wall-clock time, i.e. IndividualWallTime.

        # Benchmark must not be set, since we are currently not able to
        # scale at this level.
        if j.Resources.IndividualWallTime.benchmark[0]:
            error("Unable to scale 'IndividualWallTime' to specified benchmark"
                  "'{0}'".format(j.Resources.IndividualWallTime.benchmark[0]),
                  'slurm.JobDescriptionParserSLURM', 'slurm.Assemble')
            return
        ### TODO: Expression: \mapattr --time <- TotalCPUTime/NumberOfSlots
        if j.Resources.TotalCPUTime.range.max >= 0:
            # TODO: Check for benchmark
            individualCPUTime = j.Resources.TotalCPUTime.range.max/nslots
            product += "#SBATCH -t {0}:{1}\n".format(str(individualCPUTime/60), str(individualCPUTime%60))
            if j.Resources.IndividualWallTime.range.max >= 0:
                n, m = (str(j.Resources.IndividualWallTime.range.max/60),
                        str(j.Resources.IndividualWallTime.range.max%60))
                product += "#SBATCH -t {0}:{1}\n".format(n,m)
            else:
                product += "#SBATCH -t {0}:{1}\n".format(str(individualCPUTime/60), str(individualCPUTime%60))
        ### TODO: Note ordering
        ### \mapattr --time <- IndividualWallTime
        elif j.Resources.IndividualWallTime.range.max >= 0:
            n, m = (str(j.Resources.IndividualWallTime.range.max/60),
                    str(j.Resources.IndividualWallTime.range.max%60))
            product += \
                "#SBATCH -t {0}:{1}\n".format(n,m)
        ### \mapattr --time <- IndividualCPUTime
        elif j.Resources.IndividualCPUTime.range.max >= 0: 
            # TODO: Check for benchmark
            # IndividualWallTime not set, use IndividualCPUTime instead.
            n, m = (str(j.Resources.IndividualCPUTime.range.max/60),
                    str(j.Resources.IndividualCPUTime.range.max%60))
            product += \
                "#SBATCH -t {0}:{1}\n".format(n,m)
        
        # SLURM v2.4 option description: --mem-per-cpu=<MB>
        # Mimimum memory required per allocated CPU in MegaBytes. Default
        # value is DefMemPerCPU and the maximum value is MaxMemPerCPU (see
        # exception below). If configured, both of parameters can be seen 
        # using the scontrol show config command. Note that if the job's 
        # --mem-per-cpu value exceeds the configured MaxMemPerCPU, then the
        # user's limit will be treated as a memory limit per task; 
        # --mem-per-cpu will be reduced to a value no larger than MaxMemPerCPU;
        # --cpus-per-task will be set and value of --cpus-per-task multiplied by
        # the new --mem-per-cpu value will equal the original --mem-per-cpu
        # value specified by the user. This parameter would generally be used 
        # if individual processors are allocated to jobs 
        # (SelectType=select/cons_res). Also see --mem. --mem and
        # --mem-per-cpu are mutually exclusive.
        ### \mapattr --mem-per-cpu <- IndividualPhysicalMemory
        memPerCPU = j.Resources.IndividualPhysicalMemory.max if \
            j.Resources.IndividualPhysicalMemory.max > 0 else 1000
        product += "#SBATCH --mem-per-cpu=" + str(memPerCPU) + "\n";
        
        return product

        
'''
    SLURM v2.4 sbatch options not supported:
    --acctg-freq=<seconds>
    -B --extra-node-info=<sockets[:cores[:threads]]>
    --begin=<time>
    --checkpoint=<time>
    --checkpoint-dir=<directory>
    --comment=<string>
    -C, --constraint=<list>
    --contiguous
    --cores-per-socket=<cores>
    --cpu_bind=[{quiet,verbose},]type
    -d, --dependency=<dependency_list>
    -D, --workdir=<directory>
    --export=<environment variables | ALL | NONE>
    --export-file=<filename | fd>
    -F, --nodefile=<node file>
    --gid=<group>
    --gres=<list>
    -H, --hold
    -h, --help
    --hint=<type>
    -I, --immediate
    -i, --input=<filename pattern>
    --jobid=<jobid>
    -k, --no-kill
    -L, --licenses=<license>
    -M, --clusters=<string>
    -m, --distribution=
    --mail-type=<type>
    --mail-user=<user>
    --mem=<MB>
    --mem-per-cpu=<MB>
    --mem_bind=[{quiet,verbose},]type
    --mincpus=<n>
    -N, --nodes=<minnodes[-maxnodes]>
    --network=<type>
    --nice[=adjustment]
    -c, --cpus-per-task=<ncpus>
    --ntasks-per-core=<ntasks>
    --ntasks-per-socket=<ntasks>
    --ntasks-per-node=<ntasks>
    -O, --overcommit
    --open-mode=append|truncate
    --propagate[=rlimits]
    -Q, --quiet
    --qos=<qos>
    --requeue
    --reservation=<name>
    -s, --share
    --signal=<sig_num>[@<sig_time>]
    --sockets-per-node=<sockets>
    --switches=<count>[@<max-time>]
    --tasks-per-node=<n>
    --threads-per-core=<threads>
    --time-min=<time>
    --tmp=<MB>
    -u, --usage
    --uid=<user>
    -V, --version
    -v, --verbose
    -w, --nodelist=<node name list>
    --wait-all-nodes=<value>
    --wckey=<wckey>
    --wrap=<command string>
    -x, --exclude=<node name list>
    --blrts-image=<path>
    --cnload-image=<path>
    --conn-type=<type>
    -g, --geometry=<XxYxZ>
    --ioload-image=<path>
    --linux-image=<path>
    --mloader-image=<path>
    -R, --no-rotate
    --ramdisk-image=<path>
    --reboot
'''
