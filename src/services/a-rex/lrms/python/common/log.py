import sys, time
import arc

def debug(message = '', caller = None):
    _log(arc.DEBUG, message, caller)

def vrbs(message = '', caller = None):
    _log(arc.VERBOSE, message, caller)

def info(message = '', caller = None):
    _log(arc.INFO, message, caller)

def warn(message = '', caller = None):
    _log(arc.WARNING, message, caller)

def error(message = '', caller = None):
    _log(arc.ERROR, message, caller)

def _log(level, message, caller = None):
    if caller:
        caller = 'PythonLRMS.%s' % caller
    else:
        caller = 'PythonLRMS'
    arc.Logger(arc.Logger_getRootLogger(), caller).msg(level, message)


class ArcError(Exception):
    """                                                                                                                   
    Print an Arc::ERROR to the log and exit.                                                                              
                                                                                                                          
    :param str msg: an *informative* error message                                                                        
    """

    def __init__(self, msg, caller = None):
        _log(arc.ERROR, msg, caller)
        time.sleep(10)
        sys.exit(1)
