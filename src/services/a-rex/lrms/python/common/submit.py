"""
Common submit job functions.
"""

import os, re
import arc
from config import Config
from proc import execute_local, execute_remote
from files import write
from log import *


def validate_attributes(jd):
     """
     Checks if GRID (global) job ID and executable are set in job description
     and that runtime environment is resolved. For none-shared filesystems, 
     the scratchdir must also be specified.

     :param jd: job description object
     :type jd: :py:class:`arc.JobDescription`
     """
  
     if 'joboption;gridid' not in jd.OtherAttributes:
          raise ArcError('Grid ID not specified', 'common.submit')
     
     if 'joboption;directory' not in jd.OtherAttributes:
          jd.OtherAttributes['joboption;directory'] = \
              os.path.join(Config.sessiondir, jd.OtherAttributes['joboption;gridid'])

     jd.OtherAttributes['joboption;local_directory'] = jd.OtherAttributes['joboption;directory']

     if Config.remote_sessiondir:
          jd.OtherAttributes['joboption;directory'] = \
              os.path.join(Config.remote_sessiondir, jd.OtherAttributes['joboption;gridid'])
          jd.Application.Output = jd.Application.Output.replace(Config.sessiondir, Config.remote_sessiondir)
          jd.Application.Input = jd.Application.Input.replace(Config.sessiondir, Config.remote_sessiondir)
          jd.Application.Error = jd.Application.Error.replace(Config.sessiondir, Config.remote_sessiondir)

     if 'joboption;controldir' in jd.OtherAttributes:
          Config.controldir = jd.OtherAttributes['joboption;controldir']
     if not jd.Application.Executable.Path:
          raise ArcError('Executable is not specified', 'common.submit')
     if not jd.Resources.RunTimeEnvironment.isResolved():
          raise ArcError('Run-time Environment not satisfied', 'common.submit')
     if not Config.shared_filesystem and not Config.scratchdir:
          raise ArcError('Need to know at which directory to run job: '
                         'RUNTIME_LOCAL_SCRATCH_DIR must be set if '
                         'RUNTIME_NODE_SEES_FRONTEND is empty', 'common.submit')


def write_script_file(jobscript):
     """
     Write job script to file.

     :param str jobscript: job script buffer
     :return: path to file
     :rtype: :py:obj:`str`
     """
     
     import uuid

     jobscript += '\nrm -- "$0"\n' # self destroy
     mode = stat.S_IXUSR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXGRP | stat.S_IRGRP | stat.S_IXOTH | stat.S_IROTH

     path = '/tmp/' + str(uuid.uuid4()) + '.sh'

     if not write(path, jobscript, mode, False, Config.remote_host):
          raise ArcError('Failed to write jobscript', 'common.submit')

     return path


def set_req_mem(jobdescs):
   """
   Set memory requirement attributes in job description.

   :param jobdescs: job description list object
   :type jobdescs: :py:class:`arc.JobDescriptionList`
   """

   if jobdescs[0].Resources.IndividualPhysicalMemory.max <= 0:
       nodememory = 0
       if Config.defaultmemory <= 0:
           if (jobdescs[0].Resources.QueueName in Config.queue
               and hasattr(Config.queue[jobdescs[0].Resources.QueueName], 'nodememory')):
                nodememory = Config.queue[jobdescs[0].Resources.QueueName].nodememory
           elif Config.nodememory > 0:
                nodememory = Config.nodememory

       debug('-'*69, 'common.submit')
       debug('WARNING: The job description contains no explicit memory requirement.', 'common.submit')
       if Config.defaultmemory > 0:
           jobdescs[0].Resources.IndividualPhysicalMemory.max = Config.defaultmemory
           debug('         A default memory limit taken from \'defaultmemory\' in', 'common.submit')
           debug('         arc.conf will apply.', 'common.submit')
           debug('         Limit is: %s mb.' % (Config.defaultmemory), 'common.submit')
       elif nodememory > 0:
           jobdescs[0].Resources.IndividualPhysicalMemory.max = nodememory
           debug('         A default memory limit taken from \'nodememory\' in', 'common.submit')
           debug('         arc.conf will apply.', 'common.submit')
           debug('         You may want to set \'defaultmemory\' to something', 'common.submit')
           debug('         else in arc.conf to better handle jobs with no memory', 'common.submit')
           debug('         specified.', 'common.submit')
           debug('         Limit is: %s mb.' % (nodememory), 'common.submit')
       else:
           jobdescs[0].Resources.IndividualPhysicalMemory.max = 1000
           debug('         nodememory is not specified in arc.conf. A default', 'common.submit')
           debug('         memory limit of 1GB will apply.', 'common.submit')
       debug('-'*69, 'common.submit')

   if Config.localtransfer and jobdescs[0].Resources.IndividualPhysicalMemory.max < 1000:
       debug('-'*69, 'common.submit')
       debug('WARNING: localtransfers are enabled and job has less than 1GB of', 'common.submit')
       debug('         ram. up- and downloaders take up a lot of ram,', 'common.submit')
       debug('         this can give you problems.', 'common.submit')
       debug('-'*69, 'common.submit')


def add_user_env(jobdescs):
     """
     Get user environment part of job script.

     :param jobdescs: job description list object
     :type jobdescs: :py:class:`arc.JobDescriptionList`
     :return: jobscript part
     :rtype: :py:obj:`str`
     """

     jobscript = ''
     jobscript += '# Setting environment variables as specified by user\n'
     has_gridglobalid = False

     for env_pair in jobdescs[0].Application.Environment:
          has_gridglobalid |= env_pair[0] == 'GRID_GLOBAL_JOBID'
          jobscript += "export '%s=" % (env_pair[0])
          value = env_pair[1]
          if value[0] == '"' and value[-1] == '"':
               value = value.replace("'", "\\\'") # ' -> \'
               value = value.replace("\\\\\\\\", "\\") # \\\\ -> \
               value = value.replace('\\\'', "'\\\''") # \' -> '\''
          else:
               value = re.sub(r"^'(?<!\\'')(.*)(?<!'\\')'$", "\\1", value)
               value = value.replace("\\\\\\\\", "\\\\") # \\\\ -> \
          if value:
               jobscript += '%s' % (value)
          jobscript += "'\n"

     # guess globalid in case not already provided
     if not has_gridglobalid:
          if not Config.hostname:
               import socket
               Config.hostname = socket.gethostname()
          jobscript += "export GRID_GLOBAL_JOBID='gsiftp://%s:%s%s/%s'\n" % \
              (Config.hostname, Config.gm_port, Config.gm_mount_point, 
               jobdescs[0].OtherAttributes['joboption;gridid'])

     jobscript += '\n'
     return jobscript





# Limitation: In RTE stage 0, scripts MUST use the 'export' command if any
# new variables are defined which should be picked up by the submit script.
# Also no output from the RTE script in stage 0 will be printed.
def RTE_stage0(jobdescs, lrms, **mapping):
   """
   Source RTE scripts and update job description environment.

   :param jobdescs: job description list object
   :type jobdescs: :py:class:`arc.JobDescriptionList (Swig Object of type 'Arc::JobDescriptionList *')`
   :param str lrms: name of the LRMS
   """     

   if jobdescs[0].Resources.RunTimeEnvironment.getSoftwareList():
        from parse import RTE0EnvCreator
        envCreator = RTE0EnvCreator(jobdescs[0], Config, mapping)
        stage0_environ = envCreator.getShEnv()
        stage0_environ['joboption_lrms'] = lrms

        # Source RTE script and update the RTE stage0 dict
        def source_sw(sw, opts):
            if os.path.exists('%s/%s' % (Config.runtimedir, sw)):
                args = 'sourcewithargs () { script=$1; shift; . $script;}; sourcewithargs %s/%s 0' % (Config.runtimedir, sw)
                for opt in opts:
                    args += ' "%s"' % (opt.replace('"', '\\"'))
                args += ' > /dev/null 2>&1 && env'
                handle = execute_local(args, env = stage0_environ)
                stdout = handle.stdout
                if handle.returncode != 0:
                     raise ArcError('Runtime script %s failed' % sw, 'common.submit')
                new_env = dict((k,v.rstrip('\n')) for k,v in (l.split('=', 1) for l in stdout if l != '\n'))
                stage0_environ.update(new_env)
            else:
                warn('Runtime script %s is missing' % sw, 'common.submit')
                #should we exit here?

        # Source RTE scripts from the software list
        sw_list = []
        for sw in jobdescs[0].Resources.RunTimeEnvironment.getSoftwareList():
            sw_list.append(str(sw))
            source_sw(sw, sw.getOptions())

        # Source new RTE scripts set by the previous step (if any)
        rte_environ = dict((k,v) for k,v in stage0_environ.items() if re.match('joboption_runtime_\d+', k))
        rte_environ_opts = dict((k,v) for k,v in stage0_environ.items() if re.match('joboption_runtime_\d+_\d+', k))
        while len(rte_environ.keys()) > len(sw_list):
           for rte, sw in rte_environ.items():
              try:
                 i = re.match('joboption_runtime_(\d+)', rte).groups()[0]
                 if sw not in sw_list:
                    opts = []
                    for rte_, opt in rte_environ_opts.items():
                       try:
                          j = re.match(rte + '_(\d+)', rte_).groups()[0]
                          opts.append(opt)
                       except:
                          pass
                    source_sw(sw, opts)
                    sw_list.append(sw)
              except:
                 pass
           rte_environ = dict((k,v) for k,v in stage0_environ.items() if re.match('joboption_runtime_\d+', k))
           rte_environ_opts = dict((k,v) for k,v in stage0_environ.items()if re.match('joboption_runtime_\d+_\d+', k))
        # Update jobdesc
        envCreator.setPyEnv(stage0_environ)
        if "RUNTIME_ENABLE_MULTICORE_SCRATCH" in stage0_environ:
             os.environ["RUNTIME_ENABLE_MULTICORE_SCRATCH"] = ""

   # joboption_count might have been changed by an RTE.
   # Save it for accouting purposes.
   if jobdescs[0].Resources.SlotRequirement.NumberOfSlots > 0:
       try:
           diagfilename = '%s/job.%s.diag' % (Config.controldir, jobdescs[0].OtherAttributes['joboption;gridid'])
           with open(diagfilename, 'w+') as diagfile:
               diagfile.write('ExecutionUnits=%s\n' % jobdescs[0].Resources.SlotRequirement.NumberOfSlots)
       except IOError:
           debug('Unable to write to diag file (%s)' % diagfilename)


def RTE_stage1(jobdescs):
   """
   Get RTE stage 1 part of job script.

   :param jobdescs: job description list object
   :type jobdescs: :py:class:`arc.JobDescriptionList`
   :return: jobscript part
   :rtype: :py:obj:`str`
   """

   jobscript = \
       '# Running runtime scripts\n' \
       'export RUNTIME_CONFIG_DIR=${RUNTIME_CONFIG_DIR:-%s}\n'  \
       'runtimeenvironments=\n' % \
       (Config.remote_runtimedir if Config.remote_runtimedir else Config.runtimedir)

   if jobdescs[0].Resources.RunTimeEnvironment.empty():
       jobscript += '\n'
       return jobscript

   jobscript += 'if [ ! -z "$RUNTIME_CONFIG_DIR" ] ; then\n'
   for rte in jobdescs[0].Resources.RunTimeEnvironment.getSoftwareList():
       jobscript += \
         '  if [ -r "${RUNTIME_CONFIG_DIR}/%s" ] ; then\n' % str(rte) + \
         '    runtimeenvironments="${runtimeenvironments}%s;"\n' % str(rte) + \
         '    cmdl=${RUNTIME_CONFIG_DIR}/%s \n' % str(rte) + \
         '    sourcewithargs $cmdl 1  '
       for arg in rte.getOptions():
            jobscript += '"%s" ' % arg.replace('"', '\\"')
       jobscript += \
               '\n' \
               "    if [ $? -ne '0' ] ; then \n" \
               '      echo "Runtime %s script failed " 1>&2 \n' % str(rte) + \
               '      echo "Runtime %s script failed " ' % str(rte) + \
               '1>"$RUNTIME_JOB_DIAG" \n' \
               '      exit 1\n' \
               '    fi \n' \
               '  fi\n' \

   jobscript += 'fi\n\n'
   return jobscript

   
def setup_runtime_env(jobdescs):
     """
     Get runtime environment setup part of job script.

     :param jobdescs: job description list object
     :type jobdescs: :py:class:`arc.JobDescriptionList`
     :return: jobscript part
     :rtype: :py:obj:`str`
     """

     # Adjust working directory for tweaky nodes.
     # RUNTIME_GRIDAREA_DIR should be defined by external means on nodes.
     return \
         'RUNTIME_JOB_DIR=%s\n' % \
          jobdescs[0].OtherAttributes['joboption;directory'] + \
         'RUNTIME_JOB_STDIN=%s\n' % jobdescs[0].Application.Input + \
         'RUNTIME_JOB_STDOUT=%s\n' % jobdescs[0].Application.Output + \
         'RUNTIME_JOB_STDERR=%s\n' % jobdescs[0].Application.Error + \
         'RUNTIME_JOB_DIAG=%s.diag\n' % \
          jobdescs[0].OtherAttributes['joboption;directory'] + \
         'if [ ! -z "$RUNTIME_GRIDAREA_DIR" ] ; then\n' \
         '  RUNTIME_JOB_DIR=$RUNTIME_GRIDAREA_DIR/`basename ' \
         '$RUNTIME_JOB_DIR`\n' \
         '  RUNTIME_JOB_STDIN=`echo "$RUNTIME_JOB_STDIN" ' \
         '| sed "s#^$RUNTIME_JOB_DIR#$RUNTIME_GRIDAREA_DIR#"`\n' \
         '  RUNTIME_JOB_STDOUT=`echo "$RUNTIME_JOB_STDOUT" ' \
         '| sed "s#^$RUNTIME_JOB_DIR#$RUNTIME_GRIDAREA_DIR#"`\n' \
         '  RUNTIME_JOB_STDERR=`echo "$RUNTIME_JOB_STDERR" ' \
         '| sed "s#^$RUNTIME_JOB_DIR#$RUNTIME_GRIDAREA_DIR#"`\n' \
         '  RUNTIME_JOB_DIAG=`echo "$RUNTIME_JOB_DIAG" ' \
         '| sed "s#^$RUNTIME_JOB_DIR#$RUNTIME_GRIDAREA_DIR#"`\n' \
         '  RUNTIME_CONTROL_DIR=`echo "$RUNTIME_CONTROL_DIR" ' \
         '| sed "s#^$RUNTIME_JOB_DIR#$RUNTIME_GRIDAREA_DIR#"`\n' \
         'fi\n'


def move_files_to_node(jobdescs):
     """
     Get move files to node part of job script.

     :param jobdescs: job description list object
     :type jobdescs: :py:class:`arc.JobDescriptionList`
     :return: jobscript part
     :rtype: :py:obj:`str`
     """

     runtime_local_scratch_dir = ''

     if (jobdescs[0].Resources.SlotRequirement.NumberOfSlots == 1
         or 'RUNTIME_ENABLE_MULTICORE_SCRATCH' in os.environ):
          runtime_local_scratch_dir = Config.scratchdir

     return \
         'RUNTIME_LOCAL_SCRATCH_DIR=' \
         '${RUNTIME_LOCAL_SCRATCH_DIR:-%s}\n' % (runtime_local_scratch_dir) + \
         'RUNTIME_FRONTEND_SEES_NODE=' \
         '${RUNTIME_FRONTEND_SEES_NODE:-%s}\n' % (Config.shared_scratch) + \
         'RUNTIME_NODE_SEES_FRONTEND=${RUNTIME_NODE_SEES_FRONTEND:-%s}\n' % \
          ('yes' if Config.shared_filesystem else '') + \
         '  if [ ! -z "$RUNTIME_LOCAL_SCRATCH_DIR" ] && ' \
         '[ ! -z "$RUNTIME_NODE_SEES_FRONTEND" ]; then\n' \
         '    RUNTIME_NODE_JOB_DIR=' \
         '"$RUNTIME_LOCAL_SCRATCH_DIR"/`basename "$RUNTIME_JOB_DIR"`\n' \
         '    rm -rf "$RUNTIME_NODE_JOB_DIR"\n' \
         '    mkdir -p "$RUNTIME_NODE_JOB_DIR"\n' \
         '    # move directory contents\n' \
         '    for f in "$RUNTIME_JOB_DIR"/.* "$RUNTIME_JOB_DIR"/*; do \n' \
         '      [ "$f" = "$RUNTIME_JOB_DIR/*" ] && continue '\
         '# glob failed, no files\n' \
         '      [ "$f" = "$RUNTIME_JOB_DIR/." ] && continue\n' \
         '      [ "$f" = "$RUNTIME_JOB_DIR/.." ] && continue\n' \
         '      [ "$f" = "$RUNTIME_JOB_DIR/.diag" ] && continue\n' \
         '      [ "$f" = "$RUNTIME_JOB_DIR/.comment" ] && continue\n' \
         '      if ! mv "$f" "$RUNTIME_NODE_JOB_DIR"; then\n' \
         '        echo "Failed to move \'$f\' to ' \
         '\'$RUNTIME_NODE_JOB_DIR\'" 1>&2\n' \
         '        exit 1\n' \
         '      fi\n' \
         '    done\n' \
         '    if [ ! -z "$RUNTIME_FRONTEND_SEES_NODE" ] ; then\n' \
         '      # creating link for whole directory\n' \
         '       ln -s "$RUNTIME_FRONTEND_SEES_NODE"' \
         '/`basename "$RUNTIME_JOB_DIR"` "$RUNTIME_JOB_DIR"\n' \
         '    else\n' \
         '      # keep stdout, stderr and control directory on frontend\n' \
         '      # recreate job directory\n' \
         '      mkdir -p "$RUNTIME_JOB_DIR"\n' \
         '      # make those files\n' \
         '      mkdir -p `dirname "$RUNTIME_JOB_STDOUT"`\n' \
         '      mkdir -p `dirname "$RUNTIME_JOB_STDERR"`\n' \
         '      touch "$RUNTIME_JOB_STDOUT"\n' \
         '      touch "$RUNTIME_JOB_STDERR"\n' \
         '      RUNTIME_JOB_STDOUT__=`echo "$RUNTIME_JOB_STDOUT" ' \
         '| sed "s#^${RUNTIME_JOB_DIR}#${RUNTIME_NODE_JOB_DIR}#"`\n' \
         '      RUNTIME_JOB_STDERR__=`echo "$RUNTIME_JOB_STDERR" ' \
         '| sed "s#^${RUNTIME_JOB_DIR}#${RUNTIME_NODE_JOB_DIR}#"`\n' \
         '      rm "$RUNTIME_JOB_STDOUT__" 2>/dev/null\n' \
         '      rm "$RUNTIME_JOB_STDERR__" 2>/dev/null\n' \
         '      if [ ! -z "$RUNTIME_JOB_STDOUT__" ] && ' \
         '[ "$RUNTIME_JOB_STDOUT" != "$RUNTIME_JOB_STDOUT__" ]; then\n' \
         '        ln -s "$RUNTIME_JOB_STDOUT" "$RUNTIME_JOB_STDOUT__"\n' \
         '      fi\n' \
         '      if [ "$RUNTIME_JOB_STDOUT__" != ' \
         '"$RUNTIME_JOB_STDERR__" ] ; then\n' \
         '        if [ ! -z "$RUNTIME_JOB_STDERR__" ] && ' \
         '[ "$RUNTIME_JOB_STDERR" != "$RUNTIME_JOB_STDERR__" ]; then\n' \
         '          ln -s "$RUNTIME_JOB_STDERR" ' \
         '"$RUNTIME_JOB_STDERR__"\n' \
         '        fi\n' \
         '      fi\n' \
         '      if [ ! -z "$RUNTIME_CONTROL_DIR" ] ; then\n' \
         '        # move control directory back to frontend\n' \
         '        RUNTIME_CONTROL_DIR__=`echo "$RUNTIME_CONTROL_DIR" ' \
         '| sed "s#^${RUNTIME_JOB_DIR}#${RUNTIME_NODE_JOB_DIR}#"`\n' \
         '        mv "$RUNTIME_CONTROL_DIR__" "$RUNTIME_CONTROL_DIR"\n' \
         '      fi\n' \
         '    fi\n' \
         '    # adjust stdin,stdout & stderr pointers\n' \
         '    RUNTIME_JOB_STDIN=`echo "$RUNTIME_JOB_STDIN" ' \
         '| sed "s#^${RUNTIME_JOB_DIR}#${RUNTIME_NODE_JOB_DIR}#"`\n' \
         '    RUNTIME_JOB_STDOUT=`echo "$RUNTIME_JOB_STDOUT" ' \
         '| sed "s#^${RUNTIME_JOB_DIR}#${RUNTIME_NODE_JOB_DIR}#"`\n' \
         '    RUNTIME_JOB_STDERR=`echo "$RUNTIME_JOB_STDERR" ' \
         '| sed "s#^${RUNTIME_JOB_DIR}#${RUNTIME_NODE_JOB_DIR}#"`\n' \
         '    RUNTIME_FRONTEND_JOB_DIR="$RUNTIME_JOB_DIR"\n' \
         '    RUNTIME_JOB_DIR="$RUNTIME_NODE_JOB_DIR"\n' \
         '  fi\n' \
         '  if [ -z "$RUNTIME_NODE_SEES_FRONTEND" ] ; then\n' \
         '    mkdir -p "$RUNTIME_JOB_DIR"\n' \
         '  fi\n'


def download_input_files(jobdescs):
   """
   Get download input files section of job script.

   :param jobdescs: job description list object
   :type jobdescs: :py:class:`arc.JobDescriptionList (Swig Object of type 'Arc::JobDescriptionList *')`
   :return: jobscript section
   :rtype: :py:obj:`str`
   """

   return \
       'ARC_LOCATION=${ARC_LOCATION:-%s}\n' % arc.common.ArcLocation.Get() + \
       'GLOBUS_LOCATION=${GLOBUS_LOCATION:-%s}\n' % \
        (os.environ['GLOBUS_LOCATION'] 
         if 'GLOBUS_LOCATION' in os.environ else '') + \
       'DOWNLOADER=${DOWNLOADER:-%s/downloader}\n' % \
        arc.common.ArcLocation.GetToolsDir() + \
       '    if [ -z "$ARC_LOCATION" ] ; then\n' \
       '      echo \'Variable ARC_LOCATION is not set\' 1>&2\n' \
       '      exit 1\n' \
       '    fi\n' \
       '    if [ -z "$GLOBUS_LOCATION" ] ; then\n' \
       '      echo \'Variable GLOBUS_LOCATION is not set\' 1>&2\n' \
       '      exit 1\n' \
       '    fi\n' \
       '    export GLOBUS_LOCATION\n' \
       '    export ARC_LOCATION\n' \
       '    if [ "x$LD_LIBRARY_PATH" = "x" ]; then\n' \
       '      export LD_LIBRARY_PATH="$GLOBUS_LOCATION/lib"\n' \
       '    else\n' \
       '      export ' \
       'LD_LIBRARY_PATH="$GLOBUS_LOCATION/lib:$LD_LIBRARY_PATH"\n' \
       '    fi\n' \
       '    export SASL_PATH="$GLOBUS_LOCATION/lib/sasl"\n' \
       '    export X509_USER_KEY=' \
       '"${RUNTIME_CONTROL_DIR}/job.local.proxy"\n' \
       '    export X509_USER_CERT=' \
       '"${RUNTIME_CONTROL_DIR}/job.local.proxy"\n' \
       '    export X509_USER_PROXY=' \
       '"${RUNTIME_CONTROL_DIR}/job.local.proxy"\n' \
       '    unset X509_RUN_AS_SERVER\n' \
       '    $DOWNLOADER -p -c \'local\' "$RUNTIME_CONTROL_DIR" ' \
       '"$RUNTIME_JOB_DIR" 2>>${RUNTIME_CONTROL_DIR}/job.local.errors\n' \
       '    if [ $? -ne \'0\' ] ; then\n' \
       '      echo \'ERROR: Downloader failed.\' 1>&2\n' \
       '      exit 1\n' \
       '    fi\n' \


def setup_local_transfer(jobdescs):
   """
   Create and get path to control directory.

   :param jobdescs: job description list object
   :type jobdescs: :py:class:`arc.JobDescriptionList (Swig Object of type 'Arc::JobDescriptionList *')`
   :return: path to control directory
   :rtype: :py:obj:`str`
   """

   import tempfile, shutil
   from os.path import join
   jd = jobdescs[0]
   gridid = jd.OtherAttributes['joboption;gridid']
   sessiondir = jd.OtherAttributes['joboption;local_directory']
   # create runtime controldir
   runtime_control_dir = rcd = tempfile.mkdtemp(dir = sessiondir, prefix = 'control.')

   # safely copy the .proxy to runtime controldir
   with os.fdopen(os.open(join(rcd, 'job.local.proxy'), os.O_WRONLY | os.O_CREAT, 0600), 'w') as local_proxy:
        with open(join(jd.OtherAttributes['joboption;controldir'], 'job.%s.proxy' % gridid), 'r') as proxy:
             local_proxy.write(proxy.read())

   rcd_rel = os.path.basename(rcd)
   job_local_input_filename = join(rcdm, 'job.local.input')

   # copy the .input file to runtime controldir
   try:
       shutil.copyfile(join(Config.controldir, 'job.%s.input' % gridid), job_local_input_filename)
   except Exception as e:
       pass
   # write to the .input file (in runtime controldir)
   with open(job_local_input_filename, 'a') as local_input:
       local_input.write(rcd_rel + ' *.*\n')
       local_input.write("%s *.*\n" % jd.Application.Output[len(gridid):]
                         if jd.Application.Output.startswith(gridid) else jd.Application.Output)
       local_input.write("%s *.*\n" % jd.Application.Error[len(gridid):]
                         if jd.Application.Error.startswith(gridid) else jd.Application.Error)

   # write to the .output file 
   job_output = join(Config.controldir, 'job.%s.output' % gridid)
   with open(job_output , 'a') as output:
       output.write(rcd_rel + '\n')
   # copy the .output file to runtime controldir
   shutil.copyfile(job_output, join(rcd, 'job.local.output'))

   # copy the .local file to runtime control dir, else create it
   try:
       shutil.copyfile(join(Config.controldir, 'job.%s.local' % gridid), join(rcd, 'job.local.local'))
   except Exception as e:
       with open(join(rcd, 'job.local.local', 'w')):
           pass

   return 'RUNTIME_CONTROL_DIR=%s\n' % runtime_control_dir


def cd_and_run(jobdescs):
   """
   Get cd and run section of job script.

   :param jobdescs: job description list object
   :type jobdescs: :py:class:`arc.JobDescriptionList (Swig Object of type 'Arc::JobDescriptionList *')`
   :return: jobscript section
   :rtype: :py:obj:`str`
   """

   args = '" "'.join([jobdescs[0].Application.Executable.Path] +
                     list(jobdescs[0].Application.Executable.Argument))
   input_redirect  = '<$RUNTIME_JOB_STDIN' if jobdescs[0].Application.Input  else ''
   output_redirect = '1>$RUNTIME_JOB_STDOUT' if jobdescs[0].Application.Output else ''
   if jobdescs[0].Application.Error:
       output_redirect += ' 2>&1' \
           if jobdescs[0].Application.Output == jobdescs[0].Application.Error \
           else ' 2>$RUNTIME_JOB_STDERR'

   jobscript = \
       '  # Changing to session directory\n' \
       '  HOME=$RUNTIME_JOB_DIR\n' \
       '  export HOME\n' \
       '  if ! cd "$RUNTIME_JOB_DIR"; then\n' \
       '    echo "Failed to switch to \'$RUNTIME_JOB_DIR\'" 1>&2\n' \
       '    RESULT=1\n' \
       '  fi\n' \
       '  if [ ! -z "$RESULT" ] && [ "$RESULT" != 0 ]; then\n' \
       '    exit $RESULT\n' \
       '  fi\n'

   if Config.nodename:
       jobscript += 'nodename=`%s`\n' % (Config.nodename)
       jobscript += 'echo "nodename=$nodename" >> "$RUNTIME_JOB_DIAG"\n'

   #TODO this should probably be done on headnode instead
   jobscript += 'echo "ExecutionUnits=%s" >> "$RUNTIME_JOB_DIAG"\n' % \
       jobdescs[0].Resources.SlotRequirement.NumberOfSlots

   # In case the job executable does not exist the error message might be
   # printed by GNU_TIME, which can be confusing for the user.
   # This will print more appropriate error message.
   jobscript += \
       'executable=\'%s\'\n' % jobdescs[0].Application.Executable.Path + \
       '# Check if executable exists\n' \
       'if [ ! -f "$executable" ]; \n' \
       'then \n' \
       '  echo "Path \\"$executable\\" does not seem to exist" ' \
       '1>$RUNTIME_JOB_STDOUT 2>$RUNTIME_JOB_STDERR 1>&2\n' \
       '  exit 1\n' \
       'fi\n' \

   # In case the job executable is written in a scripting language and the
   # interpreter is not found, the error message printed by GNU_TIME is
   # misleading.  This will print a more appropriate error message.
   jobscript += \
       '# See if executable is a script, ' \
       'and extract the name of the interpreter\n' \
       'line1=`dd if="$executable" count=1 2>/dev/null | head -n 1`\n' \
       'command=`echo $line1 | sed -n \'s/^#! *//p\'`\n' \
       'interpreter=`echo $command | awk \'{print $1}\'`\n' \
       'if [ "$interpreter" = /usr/bin/env ]; ' \
       'then interpreter=`echo $command | awk \'{print $2}\'`; fi\n' \
       '# If it\'s a script and the interpreter is not found ...\n' \
       '[ "x$interpreter" = x ] || type "$interpreter" > /dev/null 2>&1' \
       ' || {\n' \
       '\n' \
       '  echo "Cannot run $executable: $interpreter: not found" ' \
       '1>$RUNTIME_JOB_STDOUT 2>$RUNTIME_JOB_STDERR 1>&2\n' \
       '  exit 1; }\n'

   # Check that gnu_time works
   jobscript += \
       'GNU_TIME=\'%s\'\n' % Config.gnu_time + \
       'if [ ! -z "$GNU_TIME" ] && ! "$GNU_TIME" ' \
       '--version >/dev/null 2>&1; then\n' \
       '  echo "WARNING: GNU time not found at: $GNU_TIME" 2>&1;\n' \
       '  GNU_TIME=\n' \
       'fi \n' \
       '\n' \
       'if [ -z "$GNU_TIME" ] ; then\n' \
       '   "%s" %s %s\n' % (args, input_redirect, output_redirect) + \
       'else\n' \
       '  $GNU_TIME -o "$RUNTIME_JOB_DIAG" -a -f \'' \
       'WallTime=%es\\nKernelTime=%Ss\\nUserTime=%Us\\nCPUUsage=%P\\n' \
       'MaxResidentMemory=%MkB\\nAverageResidentMemory=%tkB\\n' \
       'AverageTotalMemory=%KkB\\nAverageUnsharedMemory=%DkB\\n' \
       'AverageUnsharedStack=%pkB\\nAverageSharedMemory=%XkB\\n' \
       'PageSize=%ZB\\nMajorPageFaults=%F\\nMinorPageFaults=%R\\n' \
       'Swaps=%W\\nForcedSwitches=%c\\nWaitSwitches=%w\\n' \
       'Inputs=%I\\nOutputs=%O\\nSocketReceived=%r\\nSocketSent=%s\\n' \
       'Signals=%k\\n\' '
   jobscript += \
       ' "%s" %s %s\n' % (args, input_redirect, output_redirect) + \
       '\n' \
       'fi\n' \
       'RESULT=$?\n\n'

   return jobscript


def upload_output_files(jobdescs):
   """
   Get upload output files section of job script.

   :param jobdescs: job description list object
   :type jobdescs: :py:class:`arc.JobDescriptionList (Swig Object of type 'Arc::JobDescriptionList *')`
   :return: jobscript section
   :rtype: :py:obj:`str`
   """

   return \
       'UPLOADER=${UPLOADER:-%s/uploader}\n' % arc.common.ArcLocation.GetToolsDir() + \
       '     if [ "$RESULT" = \'0\' ] ; then\n' \
       '       $UPLOADER -p -c \'local\' "$RUNTIME_CONTROL_DIR" "$RUNTIME_JOB_DIR" 2>>${RUNTIME_CONTROL_DIR}/job.local.errors\n' \
       '       if [ $? -ne \'0\' ] ; then\n' \
       '         echo \'ERROR: Uploader failed.\' 1>&2\n' \
       '         if [ "$RESULT" = \'0\' ] ; then RESULT=1 ; fi\n' \
       '       fi\n' \
       '     fi\n' \
       '     rm -f "${RUNTIME_CONTROL_DIR}/job.local.proxy"\n' \


def configure_runtime(jobdescs):
   """
   Get runtime configuration part of job script.

   :param jobdescs: job description list object
   :type jobdescs: :py:class:`arc.JobDescriptionList`
   :return: jobscript part
   :rtype: :py:obj:`str`
   """

   if jobdescs[0].Resources.RunTimeEnvironment.empty():
       return '\n'

   jobscript = 'if [ ! -z "$RUNTIME_CONFIG_DIR" ] ; then\n'
   for rte in jobdescs[0].Resources.RunTimeEnvironment.getSoftwareList():
       jobscript += \
           '  if [ -r "${RUNTIME_CONFIG_DIR}/%s" ] ; then\n' % str(rte) + \
           '    cmdl=${RUNTIME_CONFIG_DIR}/%s\n' % str(rte) + \
           '    sourcewithargs $cmdl 2  '
       for arg in rte.getOptions():
           jobscript += '"%s" ' % (arg.replace('"', '\\"'))
       jobscript += '\n  fi\n'

   jobscript += 'fi\n\n'
   return jobscript


def move_files_to_frontend():
   """
   Get file transfer to frontend, cleanup and exit part of job script.

   :return: job script part
   :rtype: :py:obj:`str`
   """

   return \
       '  if [ ! -z "$RUNTIME_LOCAL_SCRATCH_DIR" ] && ' \
       '[ ! -z "$RUNTIME_NODE_SEES_FRONTEND" ]; then \n' \
       '    if [ ! -z "$RUNTIME_FRONTEND_SEES_NODE" ] ; then\n' \
       '      # just move it\n' \
       '      rm -rf "$RUNTIME_FRONTEND_JOB_DIR"\n' \
       '      destdir=`dirname "$RUNTIME_FRONTEND_JOB_DIR"`\n' \
       '      if ! mv "$RUNTIME_NODE_JOB_DIR" "$destdir"; then\n' \
       '        echo "Failed to move \'$RUNTIME_NODE_JOB_DIR\' to ' \
       '\'$destdir\'" 1>&2\n' \
       '        RESULT=1\n' \
       '      fi\n' \
       '    else\n' \
       '      # remove links\n' \
       '      rm -f "$RUNTIME_JOB_STDOUT" 2>/dev/null\n' \
       '      rm -f "$RUNTIME_JOB_STDERR" 2>/dev/null\n' \
       '      # move directory contents\n' \
       '      for f in "$RUNTIME_NODE_JOB_DIR"/.* ' \
       '"$RUNTIME_NODE_JOB_DIR"/*; do \n' \
       '        [ "$f" = "$RUNTIME_NODE_JOB_DIR/*" ] && ' \
       'continue # glob failed, no files\n' \
       '        [ "$f" = "$RUNTIME_NODE_JOB_DIR/." ] && continue\n' \
       '        [ "$f" = "$RUNTIME_NODE_JOB_DIR/.." ] && continue\n' \
       '        [ "$f" = "$RUNTIME_NODE_JOB_DIR/.diag" ] && continue\n' \
       '        [ "$f" = "$RUNTIME_NODE_JOB_DIR/.comment" ] && ' \
       'continue\n' \
       '        if ! mv "$f" "$RUNTIME_FRONTEND_JOB_DIR"; then\n' \
       '          echo "Failed to move \'$f\' to ' \
       '\'$RUNTIME_FRONTEND_JOB_DIR\'" 1>&2\n' \
       '          RESULT=1\n' \
       '        fi\n' \
       '      done\n' \
       '      rm -rf "$RUNTIME_NODE_JOB_DIR"\n' \
       '    fi\n' \
       '  fi\n' \
       '  echo "exitcode=$RESULT" >> "$RUNTIME_JOB_DIAG"\n' \
       '  exit $RESULT\n'






class JobscriptAssembler(object):
     
     def __init__(self, jobdesc):

          # String format map
          self._map = {
               'GRIDID'               : jobdesc.OtherAttributes['joboption;gridid'],
               'SESSIONDIR'           : jobdesc.OtherAttributes['joboption;directory'],
               'STDIN'                : jobdesc.Application.Input,
               'STDOUT'               : jobdesc.Application.Output,
               'STDERR'               : jobdesc.Application.Error,
               'STDIN_REDIR'          : '<$RUNTIME_JOB_STDIN' if jobdesc.Application.Input else '',
               'STDOUT_REDIR'         : '1>$RUNTIME_JOB_STDOUT' if jobdesc.Application.Output else '',
               'STDERR_REDIR'         : '2>$RUNTIME_JOB_STDERR' if \
                    jobdesc.Application.Output == jobdesc.Application.Error else '2>&1',
               
               'ENVS'                 : jobdesc.Application.Environment, # Iterable
               'ENV'                  : lambda item: item[0], # Macro
               'VAL'                  : lambda item: item[1], # Macro
               
               'EXEC'                 : jobdesc.Application.Executable.Path, 
               'ARGS'                 : '" "'.join(list(jobdesc.Application.Executable.Argument)),
               
               'RTES'                 : jobdesc.Resources.RunTimeEnvironment.getSoftwareList(), # Iterable
               'RTE'                  : lambda item: str(item), # Macro
               'OPTS'                 : lambda item: '"%s"' % '" "'.join([opt.replace('"', '\\"') for
                                                                          opt in item.getOptions()]), # Macro
               
               'PROCESSORS'           : jobdesc.Resources.SlotRequirement.NumberOfSlots,
               'NODENAME'             : Config.nodename,
               'GNU_TIME'             : Config.gnu_time,
               'GM_MOUNTPOINT'        : Config.gm_mount_point,
               'GM_PORT'              : Config.gm_port,
               'GM_HOST'              : Config.hostname,
               'RUNTIME_CONTROLDIR'   : Config.runtime_controldir,
               'LOCAL_SCRATCHDIR'     : Config.local_scratchdir, 
               'RUNTIMEDIR'           : Config.runtimedir,
               'SHARED_SCRATCHDIR'    : Config.shared_scratch, 
               'IS_SHARED_FILESYSTEM' : 'yes' if Config.shared_filesystem else '',
               'ARC_LOCATION'         : arc.common.ArcLocation.Get(),
               'ARC_TOOLSDIR'         : arc.common.ArcLocation.GetToolsDir(),
               'GLOBUS_LOCATION'      : os.environ.get('GLOBUS_LOCATION', '')
               }

          self._stubs = {}

          PARSE = 0
          SKIP  = 1
          READ  = 2
          LOOP  = 4
          SAVE  = 8

          do = PARSE
          stub_name = ''
          stub = ''
          loop = ''
          _iterable = None
          _macros = None
          _locals = dict(item for item in self._map.items())
                                        
          with open('job_script.stubs', 'r') as stubs:
               try:
                    for num, line in enumerate(stubs.readlines()):
                         if do & READ:
                              # END of stub
                              #
                              # Next step is saving it to the stubs map
                              if line[0] == '>':
                                   do = SAVE
                         
                              # Encountered loop tag
                              elif line[0] == '@':   

                                   # END of loop stub
                                   # 
                                   # Loop over the iterable, and for each item:
                                   #   run the macro(s) to resolve the string format variable(s)
                                   #   format the loop stub
                                   #   add formatted loop stub to stub
                                   if do & LOOP:
                                        for item in _iterable:
                                             for key in _macros:
                                                  _locals[key] = self._map[key](item)
                                             stub += loop % _locals
                                        do ^= LOOP

                                   # BEGIN loop stub
                                   #
                                   # Get the iterable and macros to resolve the string format variables
                                   else: 
                                        loop = ''
                                        keys = line[1:].split()
                                        key = keys[0]
                                        _iterable = self._map[key]
                                        _macros = []
                                        for key in keys[1:]:
                                             assert(key[0] == '%')
                                             _macros.append(key[1:])
                                        do |= LOOP

                              # Inside loop. Add line to loop stub. Format later
                              elif do & LOOP:
                                   loop += line
                              # Format line and add to stub
                              else:
                                   stub += line % self._map

                         elif do & SKIP:
                              # BEGIN stub
                              #
                              # Everything after the '>' tag is read into the stub string, inlcuding empty lines
                              stub = ''
                              if line[0] == '>':
                                   do = READ

                         elif do & SAVE:
                              self._stubs[stub_name] = stub
                              do = PARSE

                         elif line[:2] == '>>':
                              # Signals that a new stub is coming up
                              stub_name = line[2:].strip()
                              stub = ''
                              do = SKIP

               except ValueError:
                    raise ArcError('Incomplete format key. Maybe a missing \'s\' after \'%%( ... )\'',
                                                  'common.submit.JobscriptAssembler') 
               except AssertionError:
                    raise ArcError('Syntax error at line %i in job_script.stubs. Missing \'%%\'.'
                                   % num, 'common.submit.JobscriptAssembler')
               except:
                    raise ArcError('Unknown key \'%s\' at line %i in job_script.stubs.'
                                   % (key, num), 'common.submit.JobscriptAssembler')

     def get_stub(stub):
          return self._stubs.get(stub, '')
