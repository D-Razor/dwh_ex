import pandas
from datetime import datetime
from typing import List, Dict

from custom_operator.core_objects import DBFile, DBFolder, DBFileVersion
from custom_operator.database_initialization import project_engine
from custom_operator.filesystem_parser import populate_struct, populate_db_list, HOME_FOLDER
from custom_operator.decorator_helpers import sql_decorator_factory

from airflow.models.baseoperator import BaseOperator

from sqlalchemy import update, delete, and_


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


@sql_decorator_factory(op_type="insert")
def tables_insert(rows_to_insert, **kwargs):
    # print("Tables insert call")

    s = kwargs["session"]
    try:
        s.bulk_save_objects(rows_to_insert)
        s.commit()
    except Exception as e:
        print(f"Failed to insert rows: {e}")
        s.rollback()
    finally:
        s.close()


@sql_decorator_factory(op_type="update")
def tables_update(rows_to_update, **kwargs):
    # print("Tables update call")

    s = kwargs["session"]
    if rows_to_update is not None:
        try:
            for ur in rows_to_update:
                upd = None
                # check if the item is directory
                # new values are stored by the "columns" key
                if ur["is_dir"] == 1:
                    upd = update(DBFolder) \
                        .values(ur["columns"]) \
                        .where(DBFolder.foldername == ur["name"])
                else:
                    upd = update(DBFile) \
                        .values(ur["columns"]) \
                        .where(DBFile.filename == ur["name"])
                s.execute(upd)
            s.commit()
        except Exception as e:
            print(f"Failed to update rows: {e}")
            s.rollback()
        finally:
            s.close()


@sql_decorator_factory(op_type="delete")
def tables_delete(rows_to_delete, **kwargs):
    # print("Tables delete call")

    s = kwargs["session"]
    if rows_to_delete is not None:
        try:
            folder_ids: List = []
            file_ids: List = []
            for del_id in rows_to_delete:
                if s.query(DBFolder.id).filter_by(id=del_id).first() is not None:
                    # ID is in the DBFolder
                    folder_ids.append(del_id)
                else:
                    # ID is in the DBFile
                    file_ids.append(del_id)
            s.execute(delete(DBFile).where(DBFile.id.in_(file_ids)))
            s.commit()
            s.execute(delete(DBFolder).where(DBFolder.id.in_(folder_ids)))
            s.commit()
        except Exception as e:
            print(f"Failed to delete rows: {e}")
            s.rollback()
        finally:
            s.close()


def current_version_update(v_filename, cur_date):
    """
    Disables current version of a file and calculates the next version label
    :param v_filename: filename key for the version
    :param cur_date: current date to set in version
    :return: next version label of a file
    """
    print("Update version call")

    conn = project_engine.connect()
    # read the row by the filename and is_active state
    cur_active_version = pandas.read_sql("SELECT * FROM DBFileVersion "
                                         "WHERE filename='{0}' AND is_active=TRUE".format(v_filename), conn)

    if cur_active_version.shape[0] > 1:
        # something wrong, more than one active version
        raise Exception("Many active versions of file")
    elif cur_active_version.shape[0] == 0:
        # brand new file, return initial version
        return 1

    # perform update of the current version of the file
    with project_engine.connect() as conn:
        upd = update(DBFileVersion) \
            .values({"is_active": False, "version_end": cur_date}) \
            .where(and_(DBFileVersion.filename == v_filename, DBFileVersion.is_active == 1))
        conn.execute(upd)
        conn.commit()

    # return increased next version
    return int(cur_active_version.iloc[0]["version"] + 1)


def struct_changes_discovery():
    cur_struct_list = struct_list_initialization()

    with open("../sql/files_folders_hierarchy.sql", "r") as f:
        query = f.read()

    conn = project_engine.connect()
    db_struct_frame = pandas.read_sql(query, conn)
    db_struct_frame = db_struct_frame.astype({"create_date": "datetime64[ns]", "modify_date": "datetime64[ns]"})

    cur_struct_frame = pandas.DataFrame(x.as_dict() for x in cur_struct_list)

    cur_struct_frame = cur_struct_frame.merge(cur_struct_frame,
                                              left_on="parent_folder_id", right_on="id", suffixes=("_c", "_p"))

    cur_struct_frame = cur_struct_frame.drop(
        columns=["id_p", "is_dir_p", "parent_folder_id_p", "create_date_p", "modify_date_p"])

    diff_frame = cur_struct_frame.merge(db_struct_frame, how="outer",
                                        left_on="name_c", right_on="name",
                                        suffixes=["_left", "_right"], indicator=True)

    added_entries = diff_frame[diff_frame["_merge"] == "left_only"]
    deleted_entries = diff_frame[diff_frame["_merge"] == "right_only"]
    modified_entries = diff_frame[diff_frame["_merge"] == "both"]
    modified_entries = modified_entries[modified_entries["modify_date_c"] != modified_entries["modify_date"]]

    return added_entries, modified_entries, deleted_entries


def added_entries_handling(added_entries_frame, cur_date):
    data_to_add: List = []

    for index, entry in added_entries_frame.iterrows():
        conn = project_engine.connect()
        db_parent_entry = pandas.read_sql(
            "SELECT * FROM DBFolder WHERE foldername='{0}'".format(entry["name_p"]), conn)

        if db_parent_entry.shape[0] > 1:
            raise Exception("Many parent folders")

        parent_id = 0
        if db_parent_entry.shape[0] == 0:
            # new parent folder
            parent_id = entry["parent_folder_id_c"]
        else:
            parent_id = db_parent_entry.iloc[0]["id"]

        if entry["is_dir_c"] == 1:
            data_to_add.append(DBFolder(id=entry["id_c"], foldername=entry["name_c"],
                                        description=entry["description_c"], parent_id=parent_id,
                                        create_date=entry["create_date_c"], modify_date=entry["modify_date_c"]))
        else:
            data_to_add.append(DBFile(id=entry["id_c"], filename=entry["name_c"],
                                      description=entry["description_c"], folder_id=parent_id,
                                      create_date=entry["create_date_c"], modify_date=entry["modify_date_c"]))

        next_version = current_version_update(entry["name_c"], cur_date)

        data_to_add.append(DBFileVersion(file_id=entry["id_c"], filename=entry["name_c"],
                                         description=entry["description_c"], folder_id=parent_id,
                                         create_date=entry["create_date_c"], modify_date=entry["modify_date_c"],
                                         is_active=True, version=next_version, op_type='c',
                                         version_start=cur_date, version_end=pandas.Timestamp.max))
    return data_to_add


def modified_entries_handling(modified_entries_frame, cur_date):
    data_to_update: List = []
    data_to_add: List = []

    for index, entry in modified_entries_frame.iterrows():
        data_to_update.append({"name": entry["name"],
                               "is_dir": entry["is_dir_c"],
                               "columns": {"create_date": entry["create_date_c"],
                                           "modify_date": entry["modify_date_c"]
                                           }
                               })

        next_version = current_version_update(entry["name"], cur_date)

        data_to_add.append(DBFileVersion(file_id=entry["id"], filename=entry["name"],
                                         description=entry["description_c"], folder_id=entry["parent_id"],
                                         create_date=entry["create_date_c"], modify_date=entry["modify_date_c"],
                                         is_active=True, version=next_version, op_type='m',
                                         version_start=cur_date, version_end=pandas.Timestamp.max))

    return data_to_update, data_to_add


def deleted_entries_handling(deleted_entries_frame, cur_date):
    data_to_delete: List = []
    data_to_add: List = []

    for index, entry in deleted_entries_frame.iterrows():
        data_to_delete.append(entry["id"])

        next_version = current_version_update(entry["name"], cur_date)

        data_to_add.append(DBFileVersion(file_id=entry["id"], filename=entry["name"],
                                         description=entry["description_c"], folder_id=entry["parent_id"],
                                         create_date=entry["create_date"], modify_date=entry["modify_date"],
                                         is_active=True, version=next_version, op_type='d',
                                         version_start=cur_date, version_end=pandas.Timestamp.max))
    return data_to_delete, data_to_add


class StructureMonitoringOperator(BaseOperator):
    def __init__(self, name: str, **kwargs):
        super().__init__(**kwargs)
        self.name = name
        
    def execute(self, context):
        added_frame, modified_frame, deleted_frame = struct_changes_discovery()

        cur_date = datetime.now()
        data_to_add = added_entries_handling(added_frame, cur_date)
        tables_insert(rows_to_insert=data_to_add)

        data_to_modify, data_to_add = modified_entries_handling(modified_frame, cur_date)
        tables_update(rows_to_update=data_to_modify)
        tables_insert(rows_to_insert=data_to_add)

        data_to_delete, data_to_add = deleted_entries_handling(deleted_frame, cur_date)
        tables_delete(rows_to_delete=data_to_delete)
        tables_insert(rows_to_insert=data_to_add)

# if __name__ == "__main__":
    # added_frame, modified_frame, deleted_frame = struct_changes_discovery()

    # cur_date = datetime.now()
    # data_to_add = added_entries_handling(added_frame, cur_date)
    # tables_insert(rows_to_insert=data_to_add)

    # data_to_modify, data_to_add = modified_entries_handling(modified_frame, cur_date)
    # tables_update(rows_to_update=data_to_modify)
    # tables_insert(rows_to_insert=data_to_add)

    # data_to_delete, data_to_add = deleted_entries_handling(deleted_frame, cur_date)
    # tables_delete(rows_to_delete=data_to_delete)
    # tables_insert(rows_to_insert=data_to_add)
