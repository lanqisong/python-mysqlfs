# -*- coding:utf-8 -*-
"""
SQLALchemy api for Fuse file system
"""

import sqlalchemy
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import sessionmaker

from mysqlfs.db import models

_ENGINE = None
_DB_CONN = None

DB_CONNECT_STRING = 'mysql://mysqlfs:password@localhost/mysqlfs?charset=utf8'

def init_engine(conn_info):
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = create_engine(conn_info, echo=False, max_overflow=2000, pool_size=2000)

def get_engine():
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = create_engine(DB_CONNECT_STRING, echo=False, max_overflow=2000, pool_size=2000)
    return _ENGINE

def get_session():
    engine = get_engine()
    session = sessionmaker(bind=engine)
    return session()

def model_query(model, *args, **kwargs):
    """Query helper for simpler session usage."""

    session = get_session()
    query = session.query(model, *args)
    return query

def get_instance():
    global _DB_CONN
    if _DB_CONN is None:
        _DB_CONN = Connection()
    return _DB_CONN

def db_call(call):
    def wrapper(*args, **kwargs):
        try:
            session = get_session()
            kwargs['session'] = session
            result = call(*args, **kwargs)
            session.close()
            return result
        except Exception as e:
            print "Database exception when call%s, error msg: %s" %(call, e)
            raise e

    return wrapper


class Connection(object):
    """SqlAlchemy connection. """

    def __init__(self):
        pass

    """ Magzine database operation """
    @db_call
    def get_all_magzine(self, session=None):
        query = session.query(models.Magzine)
        return query.all()

    @db_call
    def get_magzine_by_name(self, mag_name, session=None):
        query = session.query(models.Magzine)
        query = query.filter_by(mag_name=mag_name)
        return query.one()

    """ Slot database operation """
    @db_call
    def get_slots_by_mag(self, mag_id, session=None):
        query = session.query(models.Slot)
        query = query.filter_by(mag_id=mag_id)
        return query.all()

    @db_call
    def get_slot_by_mag_and_name(self, mag_id, slot_name, session=None):
        query = session.query(models.Slot)
        query = query.filter_by(mag_id=mag_id, slot_name=slot_name)
        return query.one()

    """ Folder database operation """
    @db_call
    def create_folder(self, values, session=None):
        folder = models.Folder()
        folder.update(values)
        session.add(folder)
        session.commit()
        return folder

    @db_call
    def remove_folder(self, folder, session=None):
        query = session.query(models.File)
        #query.filter_by(folder_id=folder.id).delete()
        query.filter_by(folder_id=folder.id).update({'deleted': 1})
        folder.deleted = 1
        session.query(models.Folder).filter_by(id=folder.id).update({'deleted': 1})
        #session.delete(folder)
        session.commit()

    @db_call
    def update_folder_name(self, old_name, new_name, session=None):
        query = session.query(models.Folder)
        query.filter_by(folder_name=old_name).update({'folder_name':new_name})
        session.commit()

    @db_call
    def get_folder_by_name(self, slot_id, folder_name, session=None):
        query = session.query(models.Folder) 
        query = query.filter_by(slot_id=slot_id, folder_name=folder_name, deleted=0)
        return query.one()

    @db_call
    def get_folder_like_name(self, slot_id, folder_name, session=None):
        folder_name = folder_name.replace('\\', '\\\\')
        query = session.query(models.Folder)
        query = query.filter(models.Folder.folder_name.like(folder_name + '%'))
        query = query.filter_by(slot_id=slot_id, deleted=0)
        return query.all()

    """ File database operation"""
    @db_call
    def create_file(self, values, session=None):
        file = models.File()
        file.update(values)
        session.add(file)
        session.commit()
        return file

    @db_call
    def remove_file(self, file_id, session=None):
        query = session.query(models.File)
        query.filter(id=file_id).update({'deleted': 1})
        #query.filter(id=file_id).delete()
        session.commit()

    @db_call
    def remove_file_by_folder_id_and_file_name(self, folder_id, file_name, session=None):
        query = session.query(models.File)
        query.filter_by(folder_id=folder_id, file_name=file_name).update({'deleted': 1})
        session.commit()

    @db_call
    def update_file_size(self, folder_id, file_name, size, session=None):
        query = session.query(models.File)
        query.filter_by(folder_id=folder_id, file_name=file_name).update({'file_size': size})
        session.commit()

    @db_call
    def update_file_name(self, folder_id, old_name, new_name, session=None):
        query = session.query(models.File)
        query.filter_by(folder_id=folder_id, file_name=old_name).update({'file_name': new_name})
        session.commit()

    @db_call
    def update_file_folder(self, old_folder, old_name, new_folder, new_name, session=None):
        query = session.query(models.File)
        query.filter_by(folder_id=old_folder, file_name=old_name).update({'folder_id': new_folder,
                                                                          'file_name': new_name})
        session.commit()

    @db_call
    def get_file_by_name_and_folder_id(self, folder_id, file_name, session=None):
        query = session.query(models.File)
        query = query.filter_by(folder_id=folder_id, file_name=file_name)
        return query.one()

    @db_call
    def get_files_by_folder_id(self, folder_id, session=None):
        query = session.query(models.File)
        query = query.filter_by(folder_id=folder_id, deleted=0)
        return query.all()


if __name__ == '__main__':
    conn = get_instance() 
    #print conn.get_folder_by_name("K:\\")

    folders = conn.get_folder_like_name(1, "K:\\")
    for folder in folders:
        print folder.id

    conn.remove_folder(folders[0])
    folders = conn.get_folder_like_name(slot_id=1, folder_name="K:\\")
    for folder in folders:
        print folder.id
    #magzines = conn.get_all_magzine()
    #for mag in magzines:
    #    print mag.mag_name
