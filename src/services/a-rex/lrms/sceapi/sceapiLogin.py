#!/usr/bin/python

if __name__ == "__main__":

    from lrms.sceclient import isLoginUser, login, LOGIN_JSON_FILE
    import getpass, os, sys

    user, group = None, None
    try:
        from pwd import getpwnam
        uids = getpwnam(sys.argv[1])
        user, group = uids.pw_uid, uids.pw_gid
    except:
        print('Invalid or no grid-username given. Ownership set to %i.' % os.getuid())

    username = raw_input("Username: ")
    if isLoginUser(username):
        print "User %s is already logged in" % username
        exit(0)
    password = getpass.getpass("Password: ")
    ret_code = login(username, password, 'api.scgrid.cn')
    if ret_code == 0:
        print "Login successful!"
        if user and group:
            os.chown(LOGIN_JSON_FILE, user, group)
        exit(0)
    print "Login failed!"
    exit(ret_code)
