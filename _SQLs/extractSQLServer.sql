--  Copyright 2025 TecOnca Data Solutions.
--  Extract from SQLServer.

SELECT
    info.TABLE_NAME
    , info.COLUMN_NAME
    , info.DATA_TYPE
    , const.CONSTRAINT_TYPE
    , CAST(ep.value AS VARCHAR(MAX)) AS column_description
FROM INFORMATION_SCHEMA.COLUMNS info
LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE use_name
    ON use_name.TABLE_NAME = info.TABLE_NAME
    AND use_name.COLUMN_NAME = info.COLUMN_NAME
LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS const
    ON use_name.CONSTRAINT_NAME = const.CONSTRAINT_NAME
LEFT JOIN sys.extended_properties ep
    ON ep.major_id = OBJECT_ID(info.TABLE_SCHEMA + '.' + info.TABLE_NAME)
    AND ep.minor_id = info.ORDINAL_POSITION
    AND ep.name = 'MS_Description'
    AND ep.class_desc = 'OBJECT_OR_COLUMN'
where info.TABLE_NAME not in ('');
