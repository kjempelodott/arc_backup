"""
Provides the ``Config`` object, with each arc.conf option as an attribute.
"""

import os
from log import debug

class _object(object):
    pass

Config = _object()


def is_conf_setter(f):
    """
    Decorator for functions that set :py:data:`~lrms.common.Config` attributes.
    """
    f.is_conf_setter = True
    return f


def configure(configfile, *func):
    """
    Parse arc.conf using :py:meth:`ConfigParser.RawConfigParser.read` and
    set all dicovered options as :py:data:`~lrms.common.Config` attributes. 
    Additional setter functions ``*func`` will only be executed if they have
    been decorated with :py:meth:`lrms.common.config.is_conf_setter`.

    The ``Config`` object will be pickled to ``/tmp/python_lrms_arc.conf``.
    In case the pickle file exists and its modification time is newer than
    that of arc.conf, the ``Config`` object will be loaded from this file.
    
    :param str configfile: path to arc.conf
    :param *func: variable length setter function (e.g. set_slurm) list
    :type *func: :py:obj:`list` [ :py:obj:`function` ... ]
    """

    import pickle
    pickle_conf = '/tmp/python_lrms_arc.conf'
    global Config

    try:
        assert(getmtime(pickle_conf) > getmtime(configfile))
        debug('Loading pickled config from ' + pickle_conf, 'common.config')
        Config = pickle.loads(pickle_conf)
    except:
        import ConfigParser
        cfg = ConfigParser.ConfigParser()
        cfg.read(configfile)

        set_common(cfg)
        set_gridmanager(cfg)
        set_cluster(cfg)
        set_queue(cfg)
        for fun in func:
            getattr(fun, 'is_conf_setter', False) and fun(cfg)
        with open(pickle_conf, 'w') as f:
            f.write(pickle.dumps(Config))
            

def set_common(cfg):
    """
    Set [common] :py:data:`~lrms.common.Config` attributes.

    :param cfg: parsed arc.conf
    :type cfg: :py:class:`ConfigParser.ConfigParser`
    """

    global Config
    Config.hostname = str(cfg.get('common', 'hostname')).strip('"') \
        if cfg.has_option('common', 'hostname') else ''


def set_gridmanager(cfg):
   """
   Set [grid-manager] :py:data:`~lrms.common.Config` attributes.

   :param cfg: parsed arc.conf
   :type cfg: :py:class:`ConfigParser.ConfigParser`
   """

   global Config
   # joboption_directory
   Config.sessiondir = str(cfg.get('grid-manager', 'sessiondir')).strip('"') \
       if cfg.has_option('grid-manager', 'sessiondir') else ''
   Config.controldir = str(cfg.get('grid-manager', 'controldir')).strip('"') \
       if cfg.has_option('grid-manager', 'controldir') else ''
   Config.runtimedir = str(cfg.get('grid-manager', 'runtimedir')).strip('"') \
       if cfg.has_option('grid-manager', 'runtimedir') else ''
   # RUNTIME_FRONTEND_SEES_NODE
   Config.shared_scratch = \
       str(cfg.get('grid-manager', 'shared_scratch')).strip('"') \
       if cfg.has_option('grid-manager', 'shared_scratch') else ''
   # RUNTIME_NODE_SEES_FRONTEND
   Config.shared_filesystem = \
       not cfg.has_option('grid-manager', 'shared_filesystem') or \
       str(cfg.get('grid-manager', 'shared_filesystem')).strip('"') != 'no'
   # RUNTIME_LOCAL_SCRATCH_DIR
   Config.scratchdir = \
       str(cfg.get('grid-manager', 'scratchdir')).strip('"') \
       if cfg.has_option('grid-manager', 'scratchdir') else ''
   Config.localtransfer = \
       str(cfg.get('grid-manager', 'localtransfer')).strip('"') == 'yes' \
       if cfg.has_option('grid-manager', 'localtransfer') else False
   Config.scanscriptlog = \
       str(cfg.get('grid-manager', 'logfile')).strip('"') \
       if cfg.has_option('grid-manager', 'logfile') \
       else '/var/log/arc/grid-manager.log'
   Config.gnu_time = \
       str(cfg.get('grid-manager', 'gnu_time')).strip('"') \
       if cfg.has_option('grid-manager', 'gnu_time') else '/usr/bin/time'
   Config.nodename = \
       str(cfg.get('grid-manager', 'nodename')).strip('"') \
       if cfg.has_option('grid-manager', 'nodename') else '/bin/hostname -f'
   # SSH
   from pwd import getpwuid
   Config.remote_host = \
       str(cfg.get('grid-manager', 'remote_host')).strip('"') \
       if cfg.has_option('grid-manager', 'remote_host') else ''
   Config.remote_user = \
       str(cfg.get('grid-manager', 'remote_user')).strip('"') \
       if cfg.has_option('grid-manager', 'remote_user') \
       else getpwuid(os.getuid()).pw_name
   Config.private_key = \
       str(cfg.get('grid-manager', 'private_key')).strip('"') \
       if cfg.has_option('grid-manager', 'private_key') \
       else os.path.join(os.getenv('HOME'), '.ssh', 'id_rsa')
   Config.remote_endpoint = \
       'ssh://%s@%s:22' % (Config.remote_user, Config.remote_host) \
       if Config.remote_host else ''
   Config.remote_sessiondir = \
       str(cfg.get('grid-manager', 'remote_sessiondir')).strip('"') \
       if cfg.has_option('grid-manager', 'remote_sessiondir') else ''
   Config.remote_runtimedir = \
       str(cfg.get('grid-manager', 'remote_runtimedir')).strip('"') \
       if cfg.has_option('grid-manager', 'remote_runtimedir') else ''
   Config.ssh_timeout = \
       int(cfg.get('grid-manager', 'ssh_timeout')).strip('"') \
       if cfg.has_option('grid-manager', 'ssh_timeout') else 10


def set_cluster(cfg):
    """
    Set [cluster] :py:data:`~lrms.common.Config` attributes.
    
    :param cfg: parsed arc.conf
    :type cfg: :py:class:`ConfigParser.ConfigParser`
    """

    global Config
    Config.gm_port = int(cfg.get('cluster', 'gm_port').strip('"')) \
        if cfg.has_option('cluster', 'gm_port') else 2811
    Config.gm_mount_point = cfg.get('cluster', 'gm_mount_point') \
        if cfg.has_option('cluster', 'gm_mount_point') else '/jobs'
    Config.defaultmemory = int(cfg.get('cluster', 'defaultmemory').strip('"')) \
        if cfg.has_option('cluster', 'defaultmemory') else 0
    Config.nodememory = int(cfg.get('cluster', 'nodememory').strip('"')) \
        if cfg.has_option('cluster', 'nodememory') else 0
    Config.hostname = str(cfg.get('cluster', 'hostname')).strip('"') \
        if cfg.has_option('cluster', 'hostname') else ''
       

def set_queue(cfg):
    """
    Set [queue] :py:data:`~lrms.common.Config` attributes.
    
    :param cfg: parsed arc.conf
    :type cfg: :py:class:`ConfigParser.ConfigParser`
    """

    global Config
    Config.queue = {}

    for section in cfg.sections():
        if section[:6] != 'queue/' or not section[6:]:
            continue
        if section[6:] not in Config.queue:
            Config.queue[section[6:]] = _object()
            if cfg.has_option(section, 'nodememory'):
                Config.queue[section[6:]].nodememory = \
                    int(cfg.get(section, 'nodememory').strip('"'))

