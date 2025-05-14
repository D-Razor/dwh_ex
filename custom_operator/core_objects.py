from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import MetaData, Column, String, TIMESTAMP, BOOLEAN, INTEGER, ForeignKey, CHAR

Base = declarative_base()
metadata = MetaData()


class DBFile(Base):
    __tablename__ = "DBFile"
    # uid = Column(INTEGER, nullable=False, primary_key=True, autoincrement=True)
    id = Column(String, primary_key=True, nullable=False)
    filename = Column(String, nullable=False)
    description = Column(String, nullable=True)
    folder_id = Column(String, ForeignKey("DBFolder.id"), nullable=False)
    create_date = Column(TIMESTAMP, nullable=False)
    modify_date = Column(TIMESTAMP, nullable=True)

    # adjust naming to specify that it's used for a custom dataframe processing
    def as_dict(self):
        return {"id": self.id,
                "name": self.filename,
                "description": self.description,
                "is_dir": False,
                "parent_folder_id": self.folder_id,
                "create_date": self.create_date,
                "modify_date": self.modify_date
                }


class DBFolder(Base):
    __tablename__ = "DBFolder"
    # uid = Column(INTEGER, nullable=False, primary_key=True, autoincrement=True)
    id = Column(String, primary_key=True, nullable=False)
    foldername = Column(String, nullable=False)
    description = Column(String, nullable=True)
    parent_id = Column(String, ForeignKey("DBFolder.id"), nullable=True)
    create_date = Column(TIMESTAMP, nullable=False)
    modify_date = Column(TIMESTAMP, nullable=True)

    # adjust naming to specify that it's used for a custom dataframe processing
    def as_dict(self):
        return {"id": self.id,
                "name": self.foldername,
                "description": self.description,
                "is_dir": True,
                "parent_folder_id": self.parent_id,
                "create_date": self.create_date,
                "modify_date": self.modify_date
                }


class DBFileVersion(Base):
    __tablename__ = "DBFileVersion"
    id = Column(INTEGER, primary_key=True, autoincrement=True, nullable=False)
    file_id = Column(String, nullable=False)
    filename = Column(String, nullable=False)
    description = Column(String, nullable=True)
    folder_id = Column(String, nullable=True)
    create_date = Column(TIMESTAMP, nullable=False)
    modify_date = Column(TIMESTAMP, nullable=False)
    is_active = Column(BOOLEAN, nullable=False)
    op_type = Column(CHAR, nullable=False)
    version = Column(INTEGER, nullable=False)
    version_start = Column(TIMESTAMP, nullable=False)
    version_end = Column(TIMESTAMP, nullable=False)
