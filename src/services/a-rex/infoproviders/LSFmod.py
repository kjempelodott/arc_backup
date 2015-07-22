import re

import arc
from lrms.common.proc import *
from lrms.common.common import *
from LRMSmod import LRMSInfo


def get_lrms_options_schema():
    return LRMSInfo.get_lrms_options_schema(lsf_bin_path = '*', lsf_profile_path = '*')

def get_lrms_info(options):
 
    i = LSFInfo(options)

    i.read_lsf_version()
    i.read_queues(options['queues'].keys())
    i.read_users(options['queues'].values())
    i.read_jobs(options['jobs'])
    i.read_hosts()

    i.cluster_info()    
    for qkey, qval in options['queues'].items():
        i.queue_info(qkey)
        i.users_info(qkey, qval['users'])
    i.jobs_info(options['jobs'])
    i.nodes_info()

    return i.lrms_info


class LSFInfo(LRMSInfo, object):

    host_available = ('ok', 'closed', 'closed_Full', 'closed_Excl', 'closed_Busy', 'closed_Adm')

    def __init__(self, options):
        super(LSFInfo, self).__init__(options)
        self._path = options['lsf_bin_path'] if options.has_key('lsf_bin_path') else '/usr/bin'
        if options.has_key('lsf_profile_path'):
            self._profile = 'source %s && ' % options['lsf_profile_path']
        else:
            self._profile = ''
            log(arc.WARNING, 'lsf_profile_path not set in arc.conf', 'LSFInfo')


    def read_lsf_version(self):
        execute = excute_local if not self._ssh else execute_remote
        handle = execute('%s %s/lsid -V' % (self._profile, self._path))
        try:
            self.lsf_version = re.search('IBM Platform LSF ([\d\.]+)', handle.stdout[0]).group()
        except:
            self.lsf_version = '0'


    def read_queues(self, queues):
        self.queues = {}
        execute = excute_local if not self._ssh else execute_remote
        handle = execute('%s %s/bqueues -l %s' % (self._profile, self._path, ' '.join(queues)))
        if handle.returncode:
            raise ArcError('bqueues error: %s' % '\n'.join(handle.stderr), 'LSFInfo')

        fields = ('Priority', 'Nice', 'Status', 'MaxSlots', 'MaxSlotsUser', 'MaxSlotsProc', 'MaxSlotsHost',
                  'UsedSlots', 'PendingSlots', 'RunningSlots', 'SysSuspSlots', 'UserSuspSlots', 'ReservedSlots')

        pos = 0
        while 1:
            index = handle.stdout.index(79*'-', pos) if pos else 0
            
            name = handle.stdout[index + 1].split(':')[1].strip()
            info_line = handle.stdout[handle.stdout.index('PARAMETERS/STATISTICS', index) + 2].strip().replace('-', '0')
            queue = dict(zip(fields, info_line.split()))

            try:
                index = handle.stdout.index(79*'-', pos)
            except:
                index = -1

            queue['Hosts'] = handle.stdout[index - 2].split(':')[1].strip().rstrip('/')
            self.queues[name] = queue

            if index == -1:
                break
            pos = index


    def read_users(self, queues):
        self.users = dict.fromkeys([u for q in queues for u in q['users']])
        # If remote host, lrms user != grid-user(s)
        lrms_users = self._remote_user if self._ssh else ' '.join(self.users)

        execute = excute_local if not self._ssh else execute_remote
        handle = execute('%s %s/busers -w %s' % (self._profile, self._path, lrms_users))
        if handle.returncode:
            raise ArcError('busers error: %s' % '\n'.join(handle.stderr), 'LSFInfo')

        fields = ('User', 'MaxSlotsUserProc', 'MaxSlots', 'UsedSlots', 'PendingSlots',
                  'RunningSlots', 'SysSuspSlots', 'UserSuspSlots', 'ReservedSlots')

        if self._ssh:
            # Map all grid-users to remote lrms user
            user = dict(zip(fields, handle.stdout[1].strip().split()))
            for u in self.users.iterkeys():
                self.users[u] = user
        else:
            for line in handle.stdout[1:]:
                user = dict(zip(fields, line.strip().split()))
                self.users[user['User']] = user


    def read_jobs(self, jids):
        self.jobs = {}
        # Get all running/pending/suspended jobs
        execute = excute_local if not self._ssh else execute_remote
        handle = execute('%s %s/bjobs -u all -w' % (self._profile, self._path))
        if handle.returncode:
            raise ArcError('bjobs error: %s' % '\n'.join(handle.stderr), 'LSFInfo')

        fields = ('JobID', 'User', 'State', 'Queue', 'Host', 'NodeList')

        for line in handle.stdout[1:]:
            try:
                job = dict(zip(fields, line.strip().split()))
                self.jobs[job['JobID']] = job
            except KeyError:
                pass

        # Get infosys jobs not in running/pending/suspended state
        not_running = [jid for jid in jids if jid not in self.jobs]
        execute = excute_local if not self._ssh else execute_remote
        handle = execute('%s %s/bjobs -w %s' % (self._profile, self._path, ' '.join(not_running)))
        # Some jobs may not be in LSF. These are printed to stderr. Handle this in jobs_info
        for line in handle.stdout[1:]:
            try:
                job = dict(zip(fields, line.strip().split()))
                self.jobs[job['JobID']] = job
            except KeyError:
                pass


    def read_hosts(self):
        self.hosts = {}
        execute = excute_local if not self._ssh else execute_remote
        handle = execute('%s %s/bhosts -w' % (self._profile, self._path))
        if handle.returncode:
            raise ArcError('bhosts error: %s' % '\n'.join(handle.stderr), 'LSFInfo')

        fields = ('Hostname', 'Status', 'MaxSlotsUser', 'MaxSlots', 'UsedSlots', 
                  'RunningSlots', 'SysSuspSlots', 'UserSuspSlots', 'ReservedSlots')

        for line in handle.stdout[1:]:
            try:
                line = line.replace('-', '0')
                host = dict(zip(fields, line.strip().split()))
                self.hosts[host['Hostname']] = host
            except KeyError:
                pass

        execute = excute_local if not self._ssh else execute_remote
        handle = execute('%s %s/lshosts -w' % (self._profile, self._path))
        if handle.returncode:
            raise ArcError('lshosts error: %s' % '\n'.join(handle.stderr), 'LSFInfo')

        fields = ('Hostname', 'Arch', 'Model', 'CPUFactor', 
                  'CPUTot', 'MaxMem', 'MaxSwap', 'Server', 'Resources')

        for line in handle.stdout[1:]:
            try:
                line = line.replace('-', '0')
                host = dict(zip(fields, line.strip().split()))
                self.hosts[host['Hostname']].update(host)
            except KeyError:
                pass

        execute = excute_local if not self._ssh else execute_remote
        handle = execute('%s %s/bmgroup -w' % (self._profile, self._path))
        if handle.returncode:
            raise ArcError('bmgroup error: %s' % '\n'.join(handle.stderr), 'LSFInfo')

        self.host_groups = dict((group, nodes.split()) for group, nodes in 
                                [line.strip().split(None, 1) for line in handle.stdout[1:-1]])
        

    def cluster_info(self):
        cluster = {}
        cluster['lrms_type'] = 'LSF'
        cluster['lrms_version'] = self.lsf_version
	cluster['totalcpus'] = sum(map(int, [host['CPUTot'] for host in self.hosts.itervalues()]))
        cluster['queuedjobs'] = len([None for job in self.jobs.itervalues() if job['State'] == 'PEND'])
        cluster['runningjobs'] = len([None for job in self.jobs.itervalues() if job['State'] == 'RUN'])
        cluster['queuedcpus'] = sum(map(int, [host['ReservedSlots'] for host in self.hosts.itervalues()]))
        cluster['usedcpus'] = sum(map(int, [host['RunningSlots'] for host in self.hosts.itervalues()]))

        cpudist = {}
        for host in self.hosts.itervalues():
            if host['Status'] in LSFInfo.host_available:
                cpudist[host['CPUTot']] = cpudist[host['CPUTot']] + 1 if host['CPUTot'] in cpudist else 1
        cluster['cpudistribution'] = ' '.join('%scpu:%i' % (key, val) for key, val in cpudist.iteritems())
        self.lrms_info['cluster'] = cluster


    def queue_info(self, qname):
        queue = {}
        _queue = self.queues[qname]
        queue['status'] = (0, 1)[_queue['Status'] == 'Open:Active']
        queue['maxrunning'] = int(_queue['MaxSlots'])
        queue['maxuserrun'] = int(_queue['MaxSlotsUser'])
        queue['running'] = int(_queue['RunningSlots'])
        queue['queued'] = int(_queue['PendingSlots'])
        queue['suspended'] = int(_queue['UserSuspSlots']) + int(_queue['SysSuspSlots'])
        queue['total'] = int(_queue['UsedSlots'])
        queue['totalcpus'] = sum([int(self.hosts[host]['CPUTot']) for host in self.host_groups[_queue['Hosts']]])
        if not queue['maxrunning']: 
            queue['maxrunning'] = queue['totalcpus']
        if not queue['maxuserrun']: 
            queue['maxuserrun'] = queue['maxrunning']
        self.lrms_info['queues'][qname] = queue


    def users_info(self, queue, users):
        queue = self.lrms_info['queues'][queue]
        queue['users'] = {}
        for u in users:
            queue['users'][u] = {}
            queue['users'][u]['freecpus'] = { str(int(self.users[u]['MaxSlots']) - int(self.users[u]['UsedSlots'])): 0 }
            queue['users'][u]['queuelength'] = int(self.users[u]['PendingSlots'])


    def jobs_info(self, jids):

        statemap = {'RUN'   : 'R',
                    'PEND'  : 'Q',
                    'PSUSP' : 'S',
                    'USUSP' : 'S',
                    'SSUSP' : 'S',
                    'DONE'  : 'EXECUTED', 
                    'EXIT'  : 'EXECUTED'}

        for jid in jids:
            try:
                _job = self.jobs[jid]
                job = {}
                job['status'] = statemap[_job['State']] if _job['State'] in statemap else 'O'
                # TODO: This gets the memory from the first node in a job                                                     
                # allocation which will not be correct on a heterogenous cluster                                                
                cpus, nodes = self.expand_nodes(_job['NodeList'])
                job['cpus'] = cpus
                job['nodes'] = nodes
                node = nodes[0] if nodes else None
                if node:
                    job['mem'] = LSFInfo.parse_number(self.hosts[node]['MaxMem']) >> 10 # B -> KB
                self.lrms_info['jobs'][jid] = job
            except: # Job not found in LSF
                self.lrms_info['jobs'][jid] = { 'status' : 'EXECUTED' }


    def expand_nodes(self, nodes_expr):
        if nodes_expr == '-':
            return 0, None
        cpus, nodes = zip(*[([1, node], node.split('*'))['*' in node] for node in nodes_expr.split(':')])
        return sum(map(int, cpus)), nodes


    def nodes_info(self):
        for key, _node in self.hosts.iteritems():
            node = {'isfree'      : int(_node['Status'] == 'ok'),
                    'isavailable' : int(_node['Status'] in LSFInfo.host_available)}
            node['lcpus'] = int(_node['CPUTot'])
            node['slots'] = int(_node['MaxSlots'])
            node['machine'] = _node['Arch']
            node['pmem'] = LSFInfo.parse_number(_node['MaxMem']) >> 10 # B -> KB
            node['vmem'] = LSFInfo.parse_number(_node['MaxSwap']) >> 10 # B -> KB
            self.lrms_info['nodes'][key] = node

                                      
    @staticmethod
    def lsf_time_to_minutes(time):
        days, hours, rest = time.split(':')
        minutes, seconds = [int(t) for t in rest.split('.')]
        minutes += 45*(int(days) << 5) + 15*(int(hours) << 2) + (seconds > 0)
        return minutes
        
