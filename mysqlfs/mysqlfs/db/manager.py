"""
    Database operation for Fuse Filesystem
"""

from mysqlfs.db import api
from sqlalchemy.orm.exc import NoResultFound


def path_ltow(path):
    """path linux to windows"""

    path = path.replace('/', '\\')
    return "K:" + path

def path_wrapper(call):
    """ Path wrapper for DB method"""

    def wrapper(*args, **kwargs):
        path = kwargs.pop('path')
        if path == '/':
            kwargs['depth'] = 0
            return call(*args, **kwargs)
        paths = path.split('/')
        # the first value should be magzine name
        kwargs['mag'] = paths[1]
        kwargs['depth'] = len(paths) - 1
        # the path only has magzine and slot
        if len(paths) == 3:
            if len(paths[2]) > 0:
                kwargs['slot'] = paths[2]
            else:
                kwargs['slot'] = '/'
        elif len(paths) > 3:
            kwargs['slot'] = paths[2]
            kwargs['path'] = '/' + '/'.join(paths[3:])
        return call(*args, **kwargs)

    return wrapper


class DBManager(object):

    dbapi = api.get_instance()

    @staticmethod
    def retrive_folder_and_file(path):
        """
          The method help to split absolute file path to folder and path.

          eg:  path = 'c:\folder1\folder1\file1'
          return: ('c:\folder1\folder1', 'file1')
        """
        folder, file = path.rsplit('\\', 1)
        if '\\' not in folder:
            folder = folder + '\\'
        return folder, file 

    @staticmethod
    @path_wrapper
    def query_getattr(path=None, mag=None, slot=None, depth=None):
        """
          Query the file system attribute for @path.

          @param path: The path need to query
        """
        if depth < 3:
            return True, True, 4096, 2, 1436259029, 1436259029, 1436259029
        else:
            # Judge the path is a folder or not
            dbapi = api.get_instance()
            is_folder = True
            link = 0
            path = path_ltow(path)
            magzine = dbapi.get_magzine_by_name(mag)
            slot = dbapi.get_slot_by_mag_and_name(magzine.mag_id, slot)
            try:
                folder = dbapi.get_folder_by_name(slot.slot_id, path)
                link = folder.files
            except NoResultFound:
                is_folder = False

            if is_folder is False:
                folder_name, file_name = DBManager.retrive_folder_and_file(path)
                try:
                    folder = dbapi.get_folder_by_name(slot.slot_id, folder_name)
                    file = dbapi.get_file_by_name_and_folder_id(folder.id, file_name)
                    return True, False, file.file_size, 1, file.atime, file.ctime, file.mtime
                except NoResultFound:
                    msg = "No such folder or file %s" % path
                    print msg
                    return False, None, None, None, 1436259029, 1436259029, 1436259029
            return True, True, 4096, link+2, 1436259029, 1436259029, 1436259029

    @staticmethod
    @path_wrapper
    def query_readdir(path=None, mag=None, slot=None, depth=0):
        """
          Query the folder dirs for @path.
          @param path: The path need to query
        """
        dbapi = api.get_instance()
        sub_names = []
        # For root path
        if depth == 0:
            magzines = dbapi.get_all_magzine()
            for mag in magzines:
                sub_names.append(unicode(mag.mag_name).encode('utf8'))
            return sub_names
        elif depth <= 2:
            if slot == '/' or slot is None:
                magzine = dbapi.get_magzine_by_name(mag)
                slots = dbapi.get_slots_by_mag(magzine.mag_id)
                for slot in slots:
                    sub_names.append(unicode(slot.slot_name).encode('utf8'))
                return sub_names
            else:
                path = '/'
        if depth >= 2 and path is not None:
            path = path_ltow(path)
            magzine = dbapi.get_magzine_by_name(mag)
            slot = dbapi.get_slot_by_mag_and_name(magzine.mag_id, slot)
            folders = dbapi.get_folder_like_name(slot.slot_id, path)
            parent_id = None
            for folder in folders:
                if folder.folder_name == path:
                    parent_id = folder.id
                else:
                    folder_name = folder.folder_name
                    folder_name = folder_name[len(path):]
                    if '\\' in folder_name:
                        folder_names = folder_name.split('\\')
                        if len(folder_names[0]) == 0:
                            folder_name = folder_names[1]
                        else:
                            folder_name = folder_names[0]
                    sub_names.append(unicode(folder_name).encode('utf8'))

            files = dbapi.get_files_by_folder_id(parent_id)
            for file in files:
                sub_names.append(unicode(file.file_name).encode('utf8'))
            return list(set(sub_names))

    @staticmethod
    def query_mkdir(path):
        """
          Query make folder operation.
        """
        dbapi = api.get_instance()
        values = {}
        values['folder_name'] = path
        values['files'] = 0
        folder = dbapi.create_folder(values)
        return folder

    @staticmethod
    @path_wrapper
    def query_rmdir(path=None, mag=None, slot=None, depth=0):
        """
          Query remove folder operation
        """
        if depth < 2:
            return
        dbapi = api.get_instance()
        magzine = dbapi.get_magzine_by_name(mag)
        slot = dbapi.get_slot_by_mag_and_name(magzine.mag_id, slot)
        wpath = path_ltow(path)
        folder = dbapi.get_folder_by_name(slot.slot_id, wpath)
        dbapi.remove_folder(folder)

    @staticmethod
    def query_rename(old_path, new_path):
        """
          Query rename folder
        """
        dbapi = api.get_instance()
        try:
            folder = dbapi.get_folder_by_name(old_path)
            # Update the folder name
            dbapi.update_folder_name(old_path, new_path)
        except NoResultFound:
            old_folder, old_file = DBManager.retrive_folder_and_file(old_path) 
            new_folder, new_file = DBManager.retrive_folder_and_file(new_path)
            # File rename in the same path
            if old_folder == new_folder:
                folder = dbapi.get_folder_by_name(old_folder)
                dbapi.update_file_name(folder.id, old_file, new_file)
            else:
                old_folder = dbapi.get_folder_by_name(old_folder)
                new_folder = dbapi.get_folder_by_name(new_folder)
                dbapi.update_file_folder(old_folder.id, old_file, new_folder.id, new_file)

    @staticmethod
    @path_wrapper
    def query_open(path=None, mag=None, slot=None, depth=0):
        """
          Query open a file operation
        """
        if depth < 2:
            return
        dbapi = api.get_instance()
        magzine = dbapi.get_magzine_by_name(mag)
        slot = dbapi.get_slot_by_mag_and_name(magzine.mag_id, slot)
        wpath = path_ltow(path)
        folder_name, file_name = DBManager.retrive_folder_and_file(wpath)
        try:
            folder = dbapi.get_folder_by_name(slot.slot_id, folder_name)
            file = dbapi.get_file_by_name_and_folder_id(folder.id, file_name)
        except NoResultFound:
            msg = 'create a new file %s' % wpath
            print msg
            values = {'folder_id': folder.id,
                      'file_name': file_name,
                      'file_size': 0}
            dbapi.create_file(values)
        return path

    @staticmethod
    @path_wrapper
    def query_unlink(path=None, mag=None, slot=None, depth=0):
        """
          Query delete a file operation
        """
        if depth < 2:
            return
        dbapi = api.get_instance()
        magzine = dbapi.get_magzine_by_name(mag)
        slot = dbapi.get_slot_by_mag_and_name(magzine.mag_id, slot)
        wpath = path_ltow(path)
        folder_name, file_name = DBManager.retrive_folder_and_file(wpath)
        folder = dbapi.get_folder_by_name(slot.slot_id, folder_name)
        dbapi.remove_file_by_folder_id_and_file_name(folder.id, file_name)

    @staticmethod
    def query_write(path, size):
        """
          Write a file size
        """
        dbapi = api.get_instance() 
        folder_name, file_name = DBManager.retrive_folder_and_file(path)
        folder = dbapi.get_folder_by_name(folder_name)
        dbapi.update_file_size(folder.id, file_name, size)


if __name__ == '__main__':
    import sys
    reload(sys)                         # 2
    sys.setdefaultencoding('utf-8')     # 3

    #path = 'K:\\3'
    #print DBManager.query_readdir(path)

    #path = 'K:\\001\\test'
    #new_path = 'K:\\001\\test2'
    #DBManager.query_folder_rename(path, new_path)

    #path = 'K:\\001\\xiao.txt'
    #DBManager.query_unlink(path)

    #path = 'K:\\001\'

    #path = 'K:\\001\\test2.txt'
    #new_path = 'K:\\001\\test\\test.txt'
    #DBManager.query_rename(path, new_path)

    path = "/MAG1/Disc01/"
    print DBManager.query_readdir(path=path)
