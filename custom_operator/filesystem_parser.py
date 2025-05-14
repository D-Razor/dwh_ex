import os
from uuid import uuid4
from datetime import datetime
from custom_operator.core_objects import DBFile, DBFolder

HOME_FOLDER = os.path.abspath("/opt/airflow/root_folder")


def populate_struct(cur_dir, out_struct):
    """
    Retrieve the structure of the directory and save it as dictionary
    :param cur_dir: full name of the current directory
    :param out_struct: output dictionary
    :return: None
    """
    abs_dir = os.path.abspath(cur_dir)

    # something wrong, not a directory
    if not os.path.isdir(abs_dir):
        return
    out_struct[abs_dir] = {
        "ID": str(uuid4()),
        "FileName": abs_dir,
        "IsDirectory": os.path.isdir(abs_dir),
        "CreateDate": os.path.getctime(abs_dir),
        "ModifyDate": os.path.getmtime(abs_dir)
    }
    for item in os.listdir(abs_dir):
        abs_item = os.path.join(abs_dir, item)
        if os.path.isdir(abs_item) and all(exc_name not in abs_item for exc_name in ["venv", ".idea", "__pycache__"]):
            populate_struct(abs_item, out_struct)
        else:
            out_struct[abs_item] = {
                "ID": str(uuid4()),
                "FileName": abs_item,
                "IsDirectory": os.path.isdir(abs_item),
                "CreateDate": os.path.getctime(abs_item),
                "ModifyDate": os.path.getmtime(abs_item)
            }


def populate_db_list(input_frame, cur_row, parent_id, cur_slash_count, root_slash_count, out_list):
    """
    Convert frame of directory structure into list of sqlalchemy Base objects
    :param input_frame: DataFrame of generated file tree
    :param cur_row: row of current directory
    :param parent_id: ID of the parent directory (None if root)
    :param cur_slash_count: amount of slash symbols contained in filename on the current level
    :param root_slash_count: amount of slash symbols contained in filename on the root level
    :param out_list: list of Base objects
    :return: None
    """
    out_list.append(
        DBFolder(id=cur_row["ID"],
                 foldername=cur_row["FileName"],
                 description=None,
                 parent_id=parent_id,
                 create_date=datetime.fromtimestamp(cur_row["CreateDate"]),
                 modify_date=datetime.fromtimestamp(cur_row["ModifyDate"]))
    )

    # select entries with slash count of the lower level
    next_level = input_frame.loc[input_frame["SlashCount"] == cur_slash_count + 1]

    # select entries that contain the name of the current folder
    # disable regex to use plain string compare
    next_level = next_level.loc[next_level["FileName"].str.contains(cur_row["FileName"], regex=False)]

    for index, row in next_level.iterrows():
        if row["IsDirectory"] == 1:
            populate_db_list(input_frame, row, cur_row["ID"], cur_slash_count + 1, root_slash_count, out_list)
        else:
            out_list.append(
                DBFile(id=row["ID"],
                       filename=row["FileName"],
                       description=None,
                       folder_id=cur_row["ID"],
                       create_date=datetime.fromtimestamp(row["CreateDate"]),
                       modify_date=datetime.fromtimestamp(row["ModifyDate"]))
            )
