"""
Functions for reading and writing files.
"""

from common import *
import tools


def read(path, tail = 0):
    """                               
    Read content from file.

    :param str path: path to file   
    :param int tail: number of lines to read (from end), entire file if 0
    :return: ``True`` and the file content (:py:meth:file.readlines()) if read was successful, else ``False`` and an error message
    :rtype: :py:obj:`tuple` ( :py:obj:`bool`, [ :py:obj:`str` ... ] )
    """

    try:
        if tail:
            return tools.tail(path, 1000)
        else:
            with os.fdopen(os.open(path, os.O_RDONLY | os.O_NONBLOCK)) as f:
                return f.readlines()
    except Exception as e:
        log(arc.WARNING, 'Cannot read file at %s:\n%s' % (path, str(e)), 'common.files')
        return []


def write(path, buf, mode = 0644, append = False, remote_host = None):
    """
    Write buffer to file.
  
    :param str path: path to file 
    :param str buf: string buffer to be written                          
    :param int mode: file mode        
    :param bool append: ``True`` if buffer should be appended to existing file
    :param bool remote_host: file will be opened with sftp at the specified hostname
    """

    w_or_a = 'a' if append else 'w'
    try:
        if remote_host:
            open_file = SSHSession[remote_host].open_sftp_client().file(path, w_or_a)
            open_file.chmod(mode)
            open_file.write(buf)
            open_file.close()
        else:
            with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT, mode | 0x8000), w_or_a) as f:
                f.write(buf)

    except Exception as e:
        log(arc.WARNING, 'Cannot write to file at %s:\n%s' % (path, str(e)), 'common.files')
        return False

    return True


def getmtime(path):
    """                               
    Get modification time of a file.

    :param str path: path to file
    :return str: modification time
    """

    try:
        return os.path.getmtime(path)
    except:
        raise ArcError('Failed to stat file: %s\n%s' % (path, str(e)), 'common.files')
