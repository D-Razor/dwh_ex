WITH RECURSIVE
    folders(id, name, parent_id, parent_name, create_date, modify_date, level) AS (
        SELECT
            dbf.id,
            foldername,
            dbf.parent_id,
            NULL parent_name,
            dbf.create_date,
            dbf.modify_date,
            0
        FROM DBFolder dbf
        WHERE dbf.parent_id is NULL
        UNION ALL
        SELECT
            DBFolder.id,
            DBFolder.foldername,
            DBFolder.parent_id,
            folders.name,
            DBFolder.create_date,
            DBFolder.modify_date,
            folders.level + 1
        FROM DBFolder, folders
        WHERE DBFolder.parent_id = folders.id
    )
--select DISTINCT
--    sorted_fitems.parent_id,
--     sorted_fitems.parent_name,
--     group_concat(sorted_fitems.name, ';') OVER
--         (partition by sorted_fitems.parent_name ORDER BY sorted_fitems.parent_name) AS hash_concat
select * from (
    SELECT * from (
        select *
        from folders
        UNION ALL
        SELECT
            dbfl.id,
            dbfl.filename,
            dbfl.folder_id,
            pdbf.foldername,
            dbfl.create_date,
            dbfl.modify_date,
            length(dbfl.filename) - length(replace(dbfl.filename, '\', '')) - 2
        FROM DBFile dbfl
             JOIN DBFolder pdbf on dbfl.folder_id = pdbf.id
    ) AS files_and_folders
    ORDER BY files_and_folders.name
) as sorted_fitems
--exclude root project folder
WHERE parent_id is not NULL;
---
---