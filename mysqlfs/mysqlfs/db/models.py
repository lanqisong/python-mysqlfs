"""
SQLAlchemy models for mysql file system
"""

import six

from sqlalchemy import Column
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Integer, String, BLOB
from sqlalchemy import schema


def table_args():
    return {'mysql_engine': 'InnoDB',
            'mysql_charset': 'utf8'}


class FuseBase(object):

    metadata = None

    def update(self, values):
        """Make the model object behave like a dict."""
        for k, v in six.iteritems(values):
            setattr(self, k, v)


BaseModel = declarative_base(cls=FuseBase)


class Magzine(BaseModel):
    """Represents a magzine"""

    __tablename__ = 'tbl_magzines'
    __table_args__ = (
        schema.UniqueConstraint('mag_id', name='uniq_mag0id'),
        table_args()
    )
    mag_id = Column(Integer, primary_key=True)
    mag_name = Column(String(50))
    RFID = Column(String(50))
    drawer_order = Column(Integer)
    mag_order = Column(Integer)


class Slot(BaseModel):
    """Represents a slot"""

    __tablename__ = 'tbl_slots'
    __table_args__ = (
        schema.UniqueConstraint('slot_id', name='uniq_slot0id'),
        table_args()
    )
    slot_id = Column(Integer, primary_key=True)
    mag_id = Column(Integer)
    slot_order = Column(Integer)
    slot_name = Column(String(50))
    disc_type = Column(Integer)
    rest_cap = Column(Integer)
    disc_type_1 = Column(Integer)
    rest_cap_1 = Column(Integer)
    disc_side = Column(Integer)
    comment = Column(String(200))


class Folder(BaseModel):
    """Represents a folder"""

    __tablename__ = 'tbl_folder'
    __table_args__ = (
        schema.UniqueConstraint('id', name='uniq_folder0id'),
        table_args()
    )
    id = Column(Integer, primary_key=True)
    disc_id = Column(Integer)
    folder_name = Column(String(500, convert_unicode=False))
    disc_dir = Column(String(500))
    sum_files = Column(Integer)
    files = Column(Integer)
    archive = Column(Integer)
    task_id = Column(Integer)
    disc_num = Column(String(50))
    slot_id = Column(Integer)
    deleted = Column(Integer)


class File(BaseModel):
    """Represent a file """

    __tablename__ = 'tbl_file'
    __table_args__ = (
        schema.UniqueConstraint('id', name='uniq_file0id'),
        table_args()
    )

    id = Column(Integer, primary_key=True)
    folder_id = Column(Integer)
    file_name = Column(String(500))
    file_size = Column(Integer)
    file_md5 = Column(String(32))
    file_thumb = Column(BLOB)
    archive = Column(Integer)
    disc_num = Column(String(50))
    slot_id = Column(Integer)
    atime = Column(Integer)
    ctime = Column(Integer)
    mtime = Column(Integer)
    deleted = Column(Integer)
