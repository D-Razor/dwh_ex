import pandas
from datetime import datetime
from custom_operator.filesystem_parser import HOME_FOLDER, populate_struct, populate_db_list
from custom_operator.core_objects import Base, DBFile, DBFileVersion, DBFolder

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from typing import List, Dict

from airflow.models.baseoperator import BaseOperator


LOCAL_SQLITE_URL = "sqlite:////opt/airflow/dags/local_database.db"
project_engine = create_engine(LOCAL_SQLITE_URL)


def struct_list_initialization():
    root_struct: Dict = {}
    populate_struct(HOME_FOLDER, root_struct)

    struct_frame = pandas.DataFrame.from_dict(root_struct, orient="index",
                                              columns=["ID", "FileName", "IsDirectory", "CreateDate", "ModifyDate"])

    slash_list: List = []
    for index, row in struct_frame.iterrows():
        slash_list.append(row["FileName"].count("/"))
    struct_frame["SlashCount"] = slash_list

    root_dir_frame = struct_frame[struct_frame["SlashCount"] == struct_frame["SlashCount"].min()]
    if root_dir_frame.shape[0] > 1:
        raise Exception("Multiple root entries")
    root_dir_row = root_dir_frame.iloc[0]

    db_list: List = []
    populate_db_list(struct_frame, root_dir_row, None, int(root_dir_row["SlashCount"]),
                     int(root_dir_row["SlashCount"]), db_list)

    return db_list


def file_version_data(local_struct_list):
    data_list: List = []
    
    ver_start = datetime.now()
    for item in local_struct_list:
        if isinstance(item, DBFile):
            ver_entry = DBFileVersion(file_id=item.id, filename=item.filename,
                                      description=item.description, folder_id=item.folder_id,
                                      create_date=item.create_date, modify_date=item.modify_date,
                                      is_active=True, version=1, op_type='i',
                                      version_start=ver_start, version_end=pandas.Timestamp.max)
        else:
            ver_entry = DBFileVersion(file_id=item.id, filename=item.foldername,
                                      description=item.description, folder_id=item.parent_id,
                                      create_date=item.create_date, modify_date=item.modify_date,
                                      is_active=True, version=1, op_type='i',
                                      version_start=ver_start, version_end=pandas.Timestamp.max)
        data_list.append(ver_entry)

    return data_list


def model_creation():
    Base.metadata.create_all(project_engine)


def db_data_load(data_objects):
    s = sessionmaker(bind=project_engine)()
    try:
        s.bulk_save_objects(data_objects)
        s.commit()
    except Exception as e:
        print(f"Error inserting data: {e}")
        s.rollback()
    finally:
        s.close()


class DBInitOperator(BaseOperator):
    def __init__(self, name: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = name

    def execute(self, context):
        model_creation()
        print("Model creation finished")
        local_file_list = struct_list_initialization()
        print("struct_list_initialization finished")
        version_list = file_version_data(local_file_list)
        print("file_version_data finished")
        db_data_load(local_file_list)
        print("db_data_load finished")
        db_data_load(version_list)
        print("db_data_load finished")
