"""
    Database operation for Fuse Filesystem
"""

from mysqlfs.db import api
from sqlalchemy.orm.exc import NoResultFound

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
        print folder 
        if '\\' not in folder:
            folder = folder + '\\'
        return folder, file 

    @staticmethod
    def query_getattr(path):
        """
          Query the file system attribute for @path.

          @param path: The path need to query
        """
        # Judge the path is a folder or not
        dbapi = api.get_instance()
        is_folder = True 
        link = 0 
        try:
            folder = dbapi.get_folder_by_name(path) 
            link = folder.files
        except NoResultFound:
            is_folder = False 

        if is_folder is False:
            folder_name, file_name = DBManager.retrive_folder_and_file(path)
            try:
                folder = dbapi.get_folder_by_name(folder_name)
                file = dbapi.get_file_by_name_and_folder_id(folder.id, file_name)
                return True, False, file.file_size, 1
            except NoResultFound:
                msg = "No such folder or file %s" % path
                print msg
                return False, None, None, None
        return True, True, 4096, link+2

    @staticmethod
    def query_readdir(path):
        """
          Query the folder dirs for @path.
          @param path: The path need to query
        """
        dbapi = api.get_instance()
        folders = dbapi.get_folder_like_name(path) 
        parent_id = None
        sub_names = []
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
    def query_rmdir(path):
        """
          Query remove folder operation
        """
        dbapi = api.get_instance()
        folder = dbapi.get_folder_by_name(path)
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
    def query_open(path):
        """
          Query open a file operation
        """
        dbapi = api.get_instance()
        folder_name, file_name = DBManager.retrive_folder_and_file(path)
        try:
            folder = dbapi.get_folder_by_name(folder_name)
            file = dbapi.get_file_by_name_and_folder_id(folder.id, file_name)
        except NoResultFound:
            msg = 'create a new file %s' % path
            print msg
            values = {'folder_id': folder.id,
                      'file_name': file_name,
                      'file_size': 0}
            dbapi.create_file(values)

    @staticmethod
    def query_unlink(path):
        """
          Query delete a file operation
        """
        dbapi = api.get_instance()
        folder_name, file_name = DBManager.retrive_folder_and_file(path)
        folder = dbapi.get_folder_by_name(folder_name)
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

    path = "K:\\06-01-001-000001-0003"
    print DBManager.query_readdir(path)
