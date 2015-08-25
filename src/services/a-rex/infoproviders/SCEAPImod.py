import re, json

import arc

from lrms import sceapi
from lrms.common.log import ArcError, warn
from LRMSmod import LRMSInfo

APP = 'ARCpilot-ATLAS'


def get_lrms_options_schema():
    return LRMSInfo.get_lrms_options_schema()

def get_lrms_info(options):

    i = SCEAPIInfo(options)

    i.read_app(APP)
    i.read_jobs(options['jobs'])

    i.nodes_info()
    for qkey, qval in options['queues'].iteritems():
        i.queue_info(qkey)
        i.users_info(qkey, qval['users'])
    i.cluster_info()
    i.jobs_info(options['jobs'])

    return i.lrms_info


class SCEAPIInfo(LRMSInfo, object):

    def __init__(self, options):
        super(SCEAPIInfo, self).__init__(options)
        self.client = sceapi.setup_api()


    def read_app(self, app):
        query = '/resources/applications/' + app
        resp = self.client._ApiClient__processHttpGET(query)
        ret_json = None
        try:
            ret_json = json.loads(resp, 'utf8')
            self.app_info = ret_json['apps_list'][0]
        except KeyError:
            raise ArcError('\'apps_list\' or \'%s\' info missing from JSON: %s' % 
                           (sceapi.translate(handle['status_reason'].strip()), app), 'SCEAPIInfo')
        except:
            raise ArcError('App query failed. Response: %s' % str(resp), 'SCEAPIInfo')


    def read_jobs(self, jids):
        self.jobs = {}
        for jid in jids:
            query = "/jobs?offset=0&ujids=" + jid
            resp = self.client._ApiClient__processHttpGET(query)
            try:
                ret_json = json.loads(resp, 'utf8')
                self.jobs[jid] = ret_json['jobs_list'][0]
            except KeyError: # Lost job or invalid id!
                warn('\'jobs_list\' missing from JSON: %s' % 
                     sceapi.translate(handle['status_reason'].strip()), 'SCEAPIInfo')
                continue
            except:
                raise ArcError('Job query failed. Response: %s' % str(resp), 'SCEAPIInfo')


    def nodes_info(self):
        node = {}
        node['status'] = self.app_info['canused']
        node['lcpus'] = self.app_info['maxcpus']
        self.lrms_info['nodes'][self.app_info['hpcname']] = node


    def queue_info(self, qname):
        if qname != self.app_info['queuename']:
            return
        queue = {}
        queue['status'] = self.app_info['canused']
        queue['totalcpus'] = self.app_info['maxcpus']
        queue['running'] = self.app_info['runjobs']
        queue['queued'] = self.app_info['pendjobs']
        queue['total'] = self.app_info['njobs']
        queue['maxcputime'] = queue['defaultcput'] = \
            queue['maxwalltime'] = queue['defaultwallt'] = self.app_info['walltimelimit']*60
        queue['mincputime'] = queue['minwalltime'] = 0
        self.lrms_info['queues'][qname] = queue
        

    def users_info(self, queue, accts):
        queue = self.lrms_info['queues'][queue]
        queue['users'] = {}
        for u in accts:
            queue['users'][u] = {}
            queue['users'][u]['queuelength'] = queue['queued']


    def cluster_info(self):

        cluster = {}
        cluster['lrms_type'] = 'SCEAPI'
        cluster['lrms_version'] = 1
        cluster['totalcpus'] = 0
        cluster['runningjobs'] = 0
        cluster['queuedjobs'] = 0
        cluster['cpudistribution'] = ''
        
        cpudist = {}
        for node, record in self.lrms_info['nodes'].iteritems():
            if record['status'] == 0:
                record['isavailable'] = 1
                record['isfree'] = 1
                if not record['lcpus'] in cpudist:
                    cpudist[record['lcpus']] = 1
                    continue
                cpudist[record['lcpus']] += 1
            else:
                record['isavailable'] = 0
                record['isfree'] = 0  
        for ncpu, nnodes in cpudist.items():
            cluster['cpudistribution'] += ' %icpu:%i' % (ncpu, nnodes)

        for queue in self.lrms_info['queues'].itervalues():
            cluster['totalcpus'] += queue['totalcpus']
            cluster['runningjobs'] += queue['running']
            cluster['queuedjobs'] += queue['queued']
        self.lrms_info['cluster'] = cluster


    def jobs_info(self, jids):

        statemap = {1 : 'R', 2 : 'R', 4 : 'R',8 : 'R', 17 : 'R', 18 : 'R',
                    16 : 'Q', 33 : 'O', 34 : 'O',
                    20 : 'E', 24 : 'E', 32 : 'E', 38 : 'E'}

        sceapi_to_arc = {'puser'      : lambda x: ('user',     x),
                         'queue_name' : lambda x: ('queue',    x),
                         'status'     : lambda x: ('status',   statemap[x]),
                         'exec_host'  : lambda x: ('nodes',    [x]),
                         'walltime'   : lambda x: ('walltime', SCEAPIInfo.to_minutes(x)),
                         'corenum'    : lambda x: ('cpus',     x),
                         'job_name'   : lambda x: ('comment',  [x])}

        jobs = {}
        for jid in jids:
            if jid not in self.jobs: # Lost job or invalid job id!         
                jobs[jid] = { 'status' : 'O' }
                continue
            job = dict(sceapi_to_arc[k](v) for k, v in self.jobs[jid].iteritems() if k in sceapi_to_arc)
            job['cputime'] = job['walltime'] * job['cpus']
            jobs[jid] = job
        self.lrms_info['jobs'] = jobs


    @staticmethod
    def to_minutes(time):
        re_time = re.compile(r'^(?P<dd>\d\d\d)d(?P<HH>\d\d)h'
                             '(?P<MM>\d\d)m(?P<SS>\d\d)s')
        try:
            time = re_time.match(time).groupdict()
            return 45*(int(time['dd']) << 5) + 15*(int(time['HH']) << 2) + \
                int(time['MM']) + (int(time['SS']) > 0)
        except:
            return 0






