"""
Tools and helper functions.
"""

def lremove(string, prefix):
    """
    Remove sub-string from beginning of string.

    :param str string: string to be edited
    :param str prefix: remove this from beginning of string if exact match
    :return: string (without prefix if exact match)
    :rtype: :py:obj:`str`
    """

    if string.startswith(prefix):
        return string[len(prefix):]
    return string


def rremove(string, suffix):
    """
    Remove sub-string from end of string.

    :param str string: string to be edited
    :param str sufffix: remove this from end of string if exact match
    :return: string (without suffix if exact match)
    :rtype: :py:obj:`str`
    """

    if string.endswith(suffix):
        return string[:-len(suffix)]
    return string


def tail(path, n, BUFSIZ = 4096):
    """
    Similar to GNU tail -n [N].
    
    :param str path: path to file
    :param int n: number of lines to read
    :param int BUFSIZ: chunk size in bytes (default: 4096)
    """

    import os
    fsize = os.stat(path)[6]
    block = -1
    lines = 0
    data = ''
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NONBLOCK), 'rb') as f:
        f.seek(0,2)
        pos = f.tell()
        while pos > 0 and lines < n:
            if (pos - BUFSIZ) > 0:
                # seek back one BUFSIZ
                f.seek(block*BUFSIZ, 2)
                # read buffer
                new_data = f.read(BUFSIZ)
                data = new_data + data
                lines += new_data.count('\n')
            else:
                # file too small, start from beginning
                f.seek(0,0)
                # only read what was not read
                data = f.read(pos) + data
                break
            pos -= BUFSIZ
            block -= 1
    # return n last lines of file
    return data.split('\n')[-n:]

