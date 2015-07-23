SSHSession = {}

def ssh_connect(host, user, pkey, window_size = (2 << 15) - 1):
    from paramiko.transport import Transport
    from paramiko import RSAKey

    try:
        SSHSession[host] = Transport((host, 22))
        SSHSession[host].window_size = window_size
        pkey = RSAKey.from_private_key_file(pkey, '')
        SSHSession[host].connect(username = user, pkey = pkey)
    except Exception as e:
        raise ArcError('Failed to connect to %s:\n%s' % (host, str(e)), 'common.ssh')
