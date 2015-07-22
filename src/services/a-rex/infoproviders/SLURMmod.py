import re

import arc
from lrms.common.proc import * 
from lrms.common.common import *
from LRMSmod import LRMSInfo


def get_lrms_options_schema():
    return LRMSInfo.get_lrms_options_schema(slurm_bin_path = '*')

def get_lrms_info(options):

    si = SLURMInfo(options)

    si.read_config()
    si.read_partitions()
    si.read_jobs()
    si.read_nodes()
    si.read_cpuinfo()
    
    si.cluster_info()
    for qkey, qval in options['queues'].iteritems():
        if si.queue_info(qkey):
            si.users_info(qkey, qval['users'])
    si.jobs_info(options['jobs'])
    si.nodes_info()

    return si.lrms_info


class SLURMInfo(LRMSInfo, object):

    def __init__(self, options):
        super(SLURMInfo, self).__init__(options)
        self._path = options['slurm_bin_path'] if options.has_key('slurm_bin_path') else '/usr/bin'


    def read_config(self):
        self.config = {}
        execute = excute_local if not self._ssh else execute_remote
        handle = execute('%s/scontrol show config| grep "MaxJobCount\|SLURM_VERSION"' % (self._path))
        if handle.returncode:
            raise ArcError('scontrol error: %s' % '\n'.join(handle.stderr), 'SLURMInfo')
        for line in handle.stdout:
            try:
                conf = line.strip().split(' = ', 1)
                self.config[conf[0].rstrip()] = conf[1]
            except IndexError: # Couldn't split: blank line, header etc ..
                continue

    
    def read_partitions(self):
        self.partitions = {}
        execute = excute_local if not self._ssh else execute_remote
        handle = execute('%s/sinfo -a -h -o \'PartitionName=%%P TotalCPUs=%%C '
                           'TotalNodes=%%D MaxTime=%%l\'' % (self._path))
        if handle.returncode:
            raise ArcError('sinfo error: %s' % '\n'.join(handle.stderr), 'SLURMInfo')
        for line in handle.stdout:
            try:
                part = dict(item.split('=', 1) for item in LRMSInfo.split(line.strip()))
                part['PartitionName'] = part['PartitionName'].rstrip('*')
                part['MaxTime'] = SLURMInfo.as_period(part['MaxTime'])
                # Format of '%C' is: Number of CPUs by state in the format 'allocated/idle/other/total'
                part['AllocatedCPUs'], part['IdleCPUs'], part['OtherCPUs'], part['TotalCPUs'] = \
                    map(SLURMInfo.parse_number, part['TotalCPUs'].split('/'))
                part['TotalNodes'] = SLURMInfo.parse_number(part['TotalNodes'])
                self.partitions[part['PartitionName']] = part;
            except ValueError: # Couldn't split: blank line, header etc ..
                continue

        
    def read_jobs(self):
        self.jobs = {}
        execute = excute_local if not self._ssh else execute_remote
        handle = execute('%s/squeue -a -h -t all -o \'JobId=%%i TimeUsed=%%M Partition=%%P JobState=%%T '
                           'ReqNodes=%%D ReqCPUs=%%C TimeLimit=%%l Name=%%j NodeList=%%N\'' % (self._path))
        if handle.returncode:
            raise ArcError('squeue error: %s' % '\n'.join(handle.stderr), 'SLURMInfo')
        for line in handle.stdout:
            try:
                job = dict(item.split('=', 1) for item in LRMSInfo.split(line.strip()))
                if job.has_key('TimeUsed'):
                    job['TimeUsed'] = SLURMInfo.as_period(job['TimeUsed'])
                if job.has_key('TimeLimit'):
                    job['TimeLimit'] = SLURMInfo.as_period(job['TimeLimit'])
                self.jobs[job['JobId']] = job
            except ValueError: # Couldn't split: blank line, header etc ..
                continue


    def read_nodes(self):
        self.nodes = {}
        execute = excute_local if not self._ssh else execute_remote
        handle = execute('%s/scontrol show node --oneliner' % (self._path))
        if handle.returncode:
            raise ArcError('scontrol error: %s' % '\n'.join(handle.stderr), 'SLURMInfo')
        for line in handle.stdout:
            try:
                _ = dict(item.split('=', 1) for item in LRMSInfo.split(line.strip()))
                record = dict((k, _[k]) for k in ('NodeName', 'CPUTot', 'RealMemory', 'State', 'Sockets', 'OS', 'Arch'))
                # Node status can be followed by different symbols
                # according to it being unresponsive, powersaving, etc.
                # Get rid of them
                record['State'] = record['State'].rstrip('*~#+')
                self.nodes[record['NodeName']] = record
            except KeyError: # Node is probably down if attributes are missing, just skip it
                continue
            except ValueError: # Couldn't split: blank line, header etc ..
                continue
    

    def read_cpuinfo(self):
        self.cpuinfo = {}
        execute = excute_local if not self._ssh else execute_remote
        handle = execute('%s/sinfo -a -h -o \'%%C\'' % (self._path))
        if handle.returncode:
            raise ArcError('sinfo error: %s' % '\n'.join(handle.stderr), 'SLURMInfo')
        for line in handle.stdout:
            try:
                self.cpuinfo = dict(zip(('AllocatedCPUs', 'IdleCPUs', 'OtherCPUs', 'TotalCPUs'),
                                        map(SLURMInfo.parse_number, line.strip().split('/'))))
                break
            except IndexError: # Probably blank line
                continue
            

    def cluster_info(self):
        cluster = {}
        cluster['lrms_type'] = 'SLURM'
        cluster['lrms_version'] = self.config['SLURM_VERSION']
        cluster['totalcpus'] = sum(map(int, (node['CPUTot'] for node in self.nodes.itervalues())))
        cluster['queuedcpus'] = sum(map(int, (job['ReqCPUs'] for job in self.jobs.itervalues()
                                              if job['JobState'] == 'PENDING')))
        cluster['usedcpus'] = self.cpuinfo['AllocatedCPUs']
        cluster['queuedjobs'], cluster['runningjobs'] = self.get_jobs()
    
        # NOTE: should be on the form '8cpu:800 2cpu:40'        
        cpudist = {}
        for node in self.nodes.itervalues():
            cpudist[node['CPUTot']] = cpudist[node['CPUTot']] + 1 if node['CPUTot'] in cpudist else 1
        cluster['cpudistribution'] = ' '.join('%scpu:%i' % (key, val) for key, val in cpudist.iteritems())

        self.lrms_info['cluster'] = cluster


    def get_jobs(self, queue = ''):
        queuedjobs = runningjobs = 0
        for job in self.jobs.itervalues():
            if queue and queue != job['Partition']:
                continue
            if job['JobState'] == 'PENDING':
                queuedjobs += 1
            elif job['JobState'] in ('RUNNING', 'COMPLETING'):
                runningjobs += 1
        
        return queuedjobs, runningjobs


    def queue_info(self, qname):
        if not qname in self.partitions:
            return False
        queue = {}
        queue['status'] = queue['maxrunning'] = queue['maxqueuable'] = queue['maxuserrun'] = self.config['MaxJobCount']
        time = self.partitions[qname]['MaxTime'].GetPeriod()
        queue['maxcputime'] = queue['defaultcput'] = queue['maxwalltime'] = queue['defaultwallt'] = time if time > 0 else (2**31)-1
        queue['mincputime'] = queue['minwalltime'] = 0
        queue['queued'], queue['running'] = self.get_jobs(qname)
        queue['totalcpus'] = self.partitions[qname]['TotalCPUs']
        queue['freeslots'] = self.partitions[qname]['IdleCPUs']
        self.lrms_info['queues'][qname] = queue
        return True


    def users_info(self, queue, accts):
        queue = self.lrms_info['queues'][queue]
        queue['users'] = {}       
        for u in accts:
            queue['users'][u] = {}
            queue['users'][u]['freecpus'] = { str(self.cpuinfo['IdleCPUs']) : 0 }
            queue['users'][u]['queuelength'] = 0


    def jobs_info(self, jids):
        jobs = {}
        # Jobs can't have overlapping ID between queues in SLURM
        statemap = {'RUNNING' : 'R', 'COMPLETED' : 'E', 'CANCELLED' : 'O', 
                    'FAILED' : 'O', 'PENDING' : 'Q', 'TIMEOUT' : 'O' }
        for jid in jids:
            if jid not in self.jobs: # Lost job or invalid job id!
                jobs[jid] = { 'status' : 'O' }
                continue
            _job = self.jobs[jid]
            job = {}
            job['status'] = statemap[_job['JobState']] if job['JobState'] in statemap else 'O'
            
            # TODO: calculate rank? Probably not possible.
            job['rank'] = 0
            job['cpus'] = _job['ReqCPUs']
            
            # TODO: This gets the memory from the first node in a job
            # allocation which will not be correct on a heterogenous
            # cluster
            job['nodes'] = self.expand_nodes(_job['NodeList'])
            node = job['nodes'][0] if job['nodes'] else ' NoNode '
            # Only jobs that got the nodes can report the memory of
            # their nodes
            if node != ' NoNode ':
                job['mem'] = self.nodes[node]['RealMemory']
    
            walltime = int(_job['TimeUser'].GetPeriod())
            count = int(_job['ReqCPUs'])
            # TODO: multiply walltime by number of cores to get cputime?
            job['walltime'] = walltime
            job['cputime'] = walltime * count
            job['reqwalltime'] = walltime
            # TODO: cputime/walltime confusion again...
            job['reqcputime'] = int(_job['TimeLimit'].GetPeriod()) * count
            job['comment'] = [_job['Name']]
            jobs[jid] = job

        self.lrms_info['jobs'] = jobs


    def expand_nodes(self, nodes_expr):
        # Translates a list like n[1-2,5],n23,n[54-55] to n1,n2,n5,n23,n54,n55
        if not nodes_expr:
            return None
        nodes = []
        for node_expr in re.split(',(?=[a-zA-Z])', nodes_expr): # Lookahead for letter
            try:
                node, expr = node_expr[:-1].split('[')
                for num in expr.split(','):
                    if num.isdigit():
                        nodes.append(name + num)
                    else:
                        start, end = map(int, num.split('-'))
                        # TODO: Preserve leading zeroes in sequence, 
                        # if needed #enodes += sprintf('%s%0*d,', name, l, i)
                        nodes += [name + str(n) for n in xrange(start, end+1)]
            except:
                nodes.append(node_expr)
        return nodes


    def nodes_info(self):
        unavailable = ('DOWN', 'DRAIN', 'FAIL', 'MAINT', 'UNK')
        free = ('IDLE', 'MIXED')
        nodes = {}
        for key, _node in self.nodes.iteritems():
            node = {'isfree'      : int(_node['State'] in free),
                    'isavailable' : int(_node['State'] not in unavailable)}
            node['lcpus'] = node['slots'] = int(_node['CPUTot'])
            node['pmem'] = int(_node['RealMemory'])
            node['pcpus'] = int(_node['Sockets'])
            node['sysname'] = _node['OS']
            node['machine'] = _node['Arch']
            nodes[key] = node
        self.lrms_info['nodes'] = nodes
    

    @staticmethod
    def as_period(time):
        time = time.replace('-', ':').split(':')
        return arc.common.Period('P%sDT%sH%sM%sS' % tuple(['0']*(4 - len(time)) + time))


'''
    $jobs->{$jid}{status}
    JobState State;

    $jobs->{$jid}{rank}
    int WaitingPosition;

    $jobs->{$jid}{cpus}
    int RequestedSlots;

    $jobs->{$jid}{mem}
    int UsedMainMemory;

    $jobs->{$jid}{walltime}
    Period UsedTotalWallTime;

    $jobs->{$jid}{cputime}
    Period UsedTotalCPUTime;

    $jobs->{$jid}{reqwalltime}
    Period RequestedTotalWallTime;

    $jobs->{$jid}{reqcputime}
    Period RequestedTotalCPUTime;

    $jobs->{$jid}{nodes}
    std::list<std::string> ExecutionNode;

    std::list<std::string> OtherMessages;
    $jobs->{$jid}{comment}
'''
