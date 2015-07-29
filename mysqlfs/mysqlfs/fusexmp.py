#!/usr/bin/env python

import os, sys, errno, stat, time
from errno import *
from stat import *
import fcntl

import fuse
from fuse import Fuse


from mysqlfs.db.manager import DBManager
from mysqlfs.db import api as dbapi

if not hasattr(fuse, '__version__'):
    raise RuntimeError, \
        "your fuse-py doesn't know of fuse.__version__, probably it's too old."

fuse.fuse_python_api = (0, 2)

# We use a custom file class
fuse.feature_assert('stateful_files', 'has_init')


fuse_txt = 'Fuse: manual file system'

def flag2mode(flags):
    md = {os.O_RDONLY: 'r', os.O_WRONLY: 'w', os.O_RDWR: 'w+'}
    m = md[flags & (os.O_RDONLY | os.O_WRONLY | os.O_RDWR)]

#    if flags | os.O_APPEND:
#        m = m.replace('w', 'a', 1)

    return m


def path_linux_to_windows(path):
    path = path.replace('/', '\\')
    return "K:" + path


class MyStat(fuse.Stat):
    def __init__(self):
        self.st_mode = 0
        self.st_ino = 0
        self.st_dev = 0
        self.st_nlink = 0
        self.st_uid = 0
        self.st_gid = 0
        self.st_size = 0
        self.st_atime = 0
        self.st_mtime = 0
        self.st_ctime = 0


class Xmp(Fuse):

    def __init__(self, *args, **kw):

        Fuse.__init__(self, *args, **kw)

        # do stuff to set up your filesystem here, if you want
        import thread
        thread.start_new_thread(self.mythread, ())
        self.root = '/'
        self.user = 'mysqlfs'
        self.database = 'mysqlfs'
        self.password = 'password'
        self.port = '3306'
        self.host = 'localhost'
        self.file_class = self.XmpFile

    def mythread(self):

        """
        The beauty of the FUSE python implementation is that with the python interp
        running in foreground, you can have threads
        """
        print "mythread: started"
        while 1:
            time.sleep(120)
            print "mythread: ticking"

    def getattr(self, path):
        st = MyStat()
        #if path == '/' or path == '/dist1' or path == '/dist2' or path == '/*':
        #    st.st_mode = stat.S_IFDIR | 0755
        #    st.st_nlink = 2
        #elif path == '/test.txt':
        #    st.st_mode = stat.S_IFREG | 0444
        #    st.st_nlink = 1
        #    st.st_size = len(fuse_txt)
        #else:
        #    return -errno.ENOENT
        
        print "get attr for %s" % path
        is_exist, is_folder, size, link, atime, ctime, mtime = DBManager.query_getattr(path=path)
        if not is_exist:
            return -errno.ENOENT
        st.st_gid = 0
        st.st_uid = 0
        st.st_size = size
        st.st_nlink = link
        st.st_atime = atime
        st.st_ctime = ctime
        st.st_mtime = mtime
        if is_folder:
            st.st_mode = stat.S_IFDIR | 0777
        else:
            st.st_mode = stat.S_IFREG | 0777
        return st
        #return os.lstat("." + path)

    def readlink(self, path):
        return os.readlink("." + path)

    def readdir(self, path, offset):
        os.chdir(self.root)
        persist_names = ['.', '..']
        sub_names = DBManager.query_readdir(path=path)
        sub_names = persist_names + sub_names
        print sub_names
        for r in sub_names:
            yield fuse.Direntry(r)

        #for r in  '.', '..', 'test.txt', 'dist1', 'dist2':
        #    yield fuse.Direntry(r)
        #for e in os.listdir("." + path):
        #    yield fuse.Direntry(e)

    def unlink(self, path):
        DBManager.query_unlink(path=path)
        #os.unlink("." + path)

    def rmdir(self, path):
        DBManager.query_rmdir(path=path)
        #os.rmdir("." + path)

    def symlink(self, path, path1):
        os.symlink(path, "." + path1)

    def rename(self, path, path1):
        DBManager.query_rename(path_linux_to_windows(path),
                               path_linux_to_windows(path1))
        os.rename("." + path, "." + path1)

    def link(self, path, path1):
        os.link("." + path, "." + path1)

    def chmod(self, path, mode):
        os.chmod("." + path, mode)

    def chown(self, path, user, group):
        os.chown("." + path, user, group)

    def truncate(self, path, len):
        f = open("." + path, "a")
        f.truncate(len)
        f.close()

    def mknod(self, path, mode, dev):
        os.mknod("." + path, mode, dev)

    def mkdir(self, path, mode):
        DBManager.query_mkdir(path_linux_to_windows(path))
        os.mkdir("." + path, mode)

    def utime(self, path, times):
        os.utime("." + path, times)

#    The following utimens method would do the same as the above utime method.
#    We can't make it better though as the Python stdlib doesn't know of
#    subsecond preciseness in acces/modify times.
#  
#    def utimens(self, path, ts_acc, ts_mod):
#      os.utime("." + path, (ts_acc.tv_sec, ts_mod.tv_sec))

#    def access(self, path, mode):
#        if not os.access("." + path, mode):
#            return -EACCES

#    This is how we could add stub extended attribute handlers...
#    (We can't have ones which aptly delegate requests to the underlying fs
#    because Python lacks a standard xattr interface.)
#
#    def getxattr(self, path, name, size):
#        val = name.swapcase() + '@' + path
#        if size == 0:
#            # We are asked for size of the value.
#            return len(val)
#        return val
#
#    def listxattr(self, path, size):
#        # We use the "user" namespace to please XFS utils
#        aa = ["user." + a for a in ("foo", "bar")]
#        if size == 0:
#            # We are asked for size of the attr list, ie. joint size of attrs
#            # plus null separators.
#            return len("".join(aa)) + len(aa)
#        return aa

    def statfs(self):
        """
        Should return an object with statvfs attributes (f_bsize, f_frsize...).
        Eg., the return value of os.statvfs() is such a thing (since py 2.2).
        If you are not reusing an existing statvfs object, start with
        fuse.StatVFS(), and define the attributes.

        To provide usable information (ie., you want sensible df(1)
        output, you are suggested to specify the following attributes:

            - f_bsize - preferred size of file blocks, in bytes
            - f_frsize - fundamental size of file blcoks, in bytes
                [if you have no idea, use the same as blocksize]
            - f_blocks - total number of blocks in the filesystem
            - f_bfree - number of free blocks
            - f_files - total number of file inodes
            - f_ffree - nunber of free file inodes
        """

        return os.statvfs(".")

    def fsinit(self):
        os.chdir(self.root)

    class XmpFile(object):

        def __init__(self, path, flags, *mode):
            fmode = flag2mode(flags)
            #if fmode == 'w' or fmode == 'w+':
            #    DBManager.query_open(path_linux_to_windows(path))
            path = DBManager.query_open(path=path)
            self.file = os.fdopen(os.open("." + path, flags, *mode),
                                  fmode)
            self.fd = self.file.fileno()
            self.path = path

        def read(self, length, offset):
            #if self.path != '/test.txt':
            #    return -errno.ENOENT
            #slen = len(fuse_txt)
            #if offset < slen:
            #    if offset + length > slen:
            #        size = slen - offset
            #    buf = fuse_txt[offset:offset+length]
            #else:
            #    buf = ''
            #return buf
            self.file.seek(offset)
            return self.file.read(length)

        def write(self, buf, offset):
            self.file.seek(offset)
            self.file.write(buf)
            size = os.path.getsize("." + self.path)
            DBManager.query_write(path_linux_to_windows(self.path), size)
            return len(buf)

        def release(self, flags):
            self.file.close()

        def _fflush(self):
            if 'w' in self.file.mode or 'a' in self.file.mode:
                self.file.flush()

        def fsync(self, isfsyncfile):
            self._fflush()
            if isfsyncfile and hasattr(os, 'fdatasync'):
                os.fdatasync(self.fd)
            else:
                os.fsync(self.fd)

        def flush(self):
            self._fflush()
            # cf. xmp_flush() in fusexmp_fh.c
            os.close(os.dup(self.fd))

        def fgetattr(self):
            return os.fstat(self.fd)

        def ftruncate(self, len):
            self.file.truncate(len)

        def lock(self, cmd, owner, **kw):
            # The code here is much rather just a demonstration of the locking
            # API than something which actually was seen to be useful.

            # Advisory file locking is pretty messy in Unix, and the Python
            # interface to this doesn't make it better.
            # We can't do fcntl(2)/F_GETLK from Python in a platfrom independent
            # way. The following implementation *might* work under Linux. 
            #
            # if cmd == fcntl.F_GETLK:
            #     import struct
            # 
            #     lockdata = struct.pack('hhQQi', kw['l_type'], os.SEEK_SET,
            #                            kw['l_start'], kw['l_len'], kw['l_pid'])
            #     ld2 = fcntl.fcntl(self.fd, fcntl.F_GETLK, lockdata)
            #     flockfields = ('l_type', 'l_whence', 'l_start', 'l_len', 'l_pid')
            #     uld2 = struct.unpack('hhQQi', ld2)
            #     res = {}
            #     for i in xrange(len(uld2)):
            #          res[flockfields[i]] = uld2[i]
            #  
            #     return fuse.Flock(**res)

            # Convert fcntl-ish lock parameters to Python's weird
            # lockf(3)/flock(2) medley locking API...
            op = { fcntl.F_UNLCK : fcntl.LOCK_UN,
                   fcntl.F_RDLCK : fcntl.LOCK_SH,
                   fcntl.F_WRLCK : fcntl.LOCK_EX }[kw['l_type']]
            if cmd == fcntl.F_GETLK:
                return -EOPNOTSUPP
            elif cmd == fcntl.F_SETLK:
                if op != fcntl.LOCK_UN:
                    op |= fcntl.LOCK_NB
            elif cmd == fcntl.F_SETLKW:
                pass
            else:
                return -EINVAL

            fcntl.lockf(self.fd, op, kw['l_start'], kw['l_len'])



def main():

    usage = """
Userspace nullfs-alike: mirror the filesystem tree from some point on.

""" + Fuse.fusage

    server = Xmp(version="%prog " + fuse.__version__,
                 usage=usage)

    server.parser.add_option(mountopt="root", metavar="PATH", default='/',
                             help="mirror filesystem from under PATH [default: %default]")
    server.parser.add_option(mountopt="database", metavar="DATABASE", default='mysqlfs',
                             help="database name for this file system, default is :mysqlfs")
    server.parser.add_option(mountopt="host", metavar="HOST", default='localhost',
                             help="host name or ip address of database")
    server.parser.add_option(mountopt="user", metavar="USER", default='mysqlfs',
                             help="user name of database")
    server.parser.add_option(mountopt="password", metavar="PASSWORD", default='password',
                             help="password of database")
    server.parser.add_option(mountopt="port", metavar="PORT", default='3306',
                             help="port of database")
    server.parse(values=server, errex=1)

    try:
        if server.fuse_args.mount_expected():
            conn_info = "mysql://%s:%s@%s:%s/%s?charset=utf8" % (server.user,
                                                         server.password,
                                                         server.host,
                                                         server.port,
                                                         server.database)

            dbapi.init_engine(conn_info)
            os.chdir(server.root)
    except OSError:
        print >> sys.stderr, "can't enter root of underlying filesystem"
        sys.exit(1)

    server.main()


if __name__ == '__main__':
    import sys
    reload(sys)                         # 2
    sys.setdefaultencoding('utf-8')     # 3
    main()
