"""
Functions for parsing arc.conf and filling :py:data:`~lrms.common.common.Config`.
"""

from common import Object

Config = Object()


def is_conf_setter(*a):
    pass

def configure(configfile, *func):
    """
    Parse arc.conf using :py:meth:`ConfigParser.RawConfigParser.read`.

    :param str configfile: full path to arc.conf
    :return: config object
    :rtype: :py:class:`ConfigParser.ConfigParser`
    """

    import ConfigParser
    cfg = ConfigParser.ConfigParser()
    cfg.read(configfile)
    set_common(cfg)
    set_grid_manager(cfg)
    set_cluster(cfg)
    set_queue(cfg)
    for f in func:
        if f.func_dict.get('is_conf_setter', False):
            f(cfg)
            

def set_common(cfg):
    """
    Fill :py:data:`~lrms.common.common.Config` with [common] options.

    :param cfg: parsed config (from :py:meth:`~lrms.common.config.get_parsed_config`)
    :type cfg: :py:class:`ConfigParser.ConfigParser`
    """

    Config.hostname = str(cfg.get('common', 'hostname')).strip('"') \
        if cfg.has_option('common', 'hostname') else ''


def set_gridmanager(cfg):
   """
   Fill :py:data:`~lrms.common.common.Config` with [grid-manager] options.
   
   :param cfg: parsed config (from :py:meth:`~lrms.common.config.get_parsed_config`)
   :type cfg: :py:class:`ConfigParser.ConfigParser`
   """

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


def set_cluster(cfg):
    """
    Fill :py:data:`~lrms.common.common.Config` with [cluster] options.

    :param cfg: parsed config (from :py:meth:`~lrms.common.config.get_parsed_config`)
    :type cfg: :py:class:`ConfigParser.ConfigParser`
    """

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
    Fill :py:data:`~lrms.common.common.Config` with [queue] options.

    :param cfg: parsed config \
    (from :py:meth:`~lrms.common.config.get_parsed_config`)
    :type cfg: :py:class:`ConfigParser.ConfigParser`
    """

    Config.queue = {}

    for section in cfg.sections():
        if section[:6] != 'queue/' or not section[6:]:
            continue
        if section[6:] not in Config.queue:
            Config.queue[section[6:]] = Object()
            if cfg.has_option(section, 'nodememory'):
                Config.queue[section[6:]].nodememory = \
                    int(cfg.get(section, 'nodememory').strip('"'))

