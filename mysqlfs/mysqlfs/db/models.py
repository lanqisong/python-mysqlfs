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


class Folder(BaseModel):
    """Represents a folder"""

    __tablename__ = 'tbl_folder'
    __table_args__ = (
        schema.UniqueConstraint('id', name='uniq_folder0id'),
        table_args()
    )
    id = Column(Integer, primary_key=True)
    disc_id = Column(Integer)
    folder_name = Column(String(500))
    disc_dir = Column(String(500))
    sum_files = Column(Integer)
    files = Column(Integer)
    archive = Column(Integer)
    task_id = Column(Integer)
    disc_num = Column(String(50))
    slot_id = Column(Integer)


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
