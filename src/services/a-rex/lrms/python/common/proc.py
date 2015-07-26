"""
Execute bash commands locally or remotely (SSH).
"""

from config import Config
from ssh import SSHSession

def execute_local(args, env = {}):
    """
    Execute a command locally. This method is a wrapper for
    :py:class:`subprocess.Popen` with stdout and stderr piped to
    temporary files and ``shell=True``.
    
    :param str args: command with arguments (e.g. 'sbatch myjob.sh')
    :param dict env: environment variables  (default: {})
    :return: handle
    :rtype: :py:class:`subprocess.Popen`
    :note: ``wait()`` is called, and ``stdout`` and ``stderr`` are read and splitted by newlines
    """
    # TODO: What if args longer than kernel ARG_MAX (e.g. scan millions of jobs)?
    from tempfile import TemporaryFile
    from subprocess import Popen
    # Note: PIPE will cause deadlock if output is larger than 65K
    stdout, stderr = TemporaryFile(), TemporaryFile()
    handle = Popen(args, stdout = stdout, stderr = stderr, env = env, shell = True)
    handle.wait()
    handle.stdout = stdout.seek(0) or stdout.readlines()
    handle.stderr = stderr.seek(0) or stderr.readlines()
    return handle


def execute_remote(args, host = None, timeout = 10):
    """
    Execute a command on the remote host using the SSH protocol.
    
    :param str args: command with arguments (e.g. 'sbatch myjob.sh')
    :return: object with attributes ``stdout``, ``stderr`` \
    and ``returncode``
    :rtype: :py:obj:`object`
    """
    # TODO: What if args longer than kernel ARG_MAX (e.g. scan millions of jobs)?
    from time import sleep

    timeout = Config.ssh_timeout
    def is_timeout(test):
        wait_time = 0
        while not test():
            sleep(0.5)
            wait_time += 0.5
            if wait_time > timeout:
                return True
        return False

    try:
        p = Object()
        if not SSHSession:
            raise ArcError('There is no active SSH session! Run lrms.common.ssh.ssh_connect', 'common.proc')
        session = SSHSession[host if host else SSHSession.keys()[-1]].open_session()
        session.exec_command(args)
        if is_timeout(session.exit_status_ready):
            warn('Session timed out. Some output might not be received. Guessing exit code from stderr.', 'common.proc')
        p.returncode = session.exit_status

        chnksz = 2 << 9

        stdout = ''
        data = session.recv(chnksz)
        while data:
            stdout += data
            data = session.recv(chnksz)
        p.stdout = stdout.split('\n')
        
        stderr = ''
        data = session.recv_stderr(chnksz)
        while data:
            stderr += data
            data = session.recv_stderr(chnksz)
        p.stderr = stderr.split('\n')

        if p.returncode == -1:
            p.returncode = len(stderr) > 0
        return p

    except Exception as e:
        raise ArcError('Failed to execute command \'%s\':\n%s' % (args.split()[0], str(e)), 'common.proc')


# class SSHSubprocess_libssh(object):
#     """
#     Execute commands and send files via SSH.

#     :param endpoint_URL: URL object with protocol *ssh*.
#     :type endpoint_URL: :py:class:`arc.URL (Swig Object of type 'Arc::URL *')`
#     """

#     def __init__(self, endpoint_URL, pkey = None):
#         if not endpoint_URL or endpoint_URL.Protocol() != 'ssh' or not endpoint_URL.Host():
#             raise ArcError('Invalid URL given to SSHSubprocess: ' + endpoint_URL.FullPath(), 
#                            'common.proc.SSHSubprocess')
#         if not pkey: 
#             pkey = Config.private_key 
#         self.sshClient = arc.ssh.SSHClient(endpoint_URL, UserConfig, pkey)
        
#     def execute(self, args):
#         """
#         Execute a command via SSH using the ``execute`` method of \  
#         :py:class:`arc.SSHChannel (Swig Object of type 'Arc::SSHChannel')`.

#         :param str args: command with arguments (e.g. 'sbatch myjob.sh')
#         :return: object with attributes ``stdout``, ``stderr`` \
#         and ``returncode``
#         :rtype: :py:class:`~lrms.common.common.Object`
#         """

#         p = Object()
#         p.sshChannel = arc.SSHChannel()
#         status = self.sshClient.openSessionChannel(p.sshChannel)
#         if not status:
#             raise ArcError('Failed to open SSH channel. Status: %i' % status,
#                            'common.proc.SSHSubprocess')
#         status = p.sshChannel.execute(args, True)
#         # First element is the exit code
#         p.returncode = p.sshChannel.getExitStatus()
#         p.stdout = p.sshChannel.readStdOut()[1].split('\n')
#         p.stderr = p.sshChannel.readStdError()[1].split('\n')
#         return p

#     def push_file(self, fname, mode, dest = None):
#         """ 
#         Similar to scp. Used for pushing jobscript file to remote endpoint.

#         :param str fname: path to local file
#         :param int mode: file mode at remote endpoint
#         :param str dest: dirname at remote endpoint (default: local dirname)
#         :return: ``True`` if successful, \
#         else raise :py:class:`~lrms.common.common.ArcError`
#         :rtype: :py:obj:`bool`
#         """

#         if not dest:
#             dest = os.path.dirname(fname)
#         if not self.sshClient.push_file(fname, mode, dest):
#             raise ArcError('Failed to copy file (%s) to remote endpoint' % 
#                            os.path.basename(fname), 'common.proc.SSHSubprocess')
#         return True
