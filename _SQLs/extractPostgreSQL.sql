--  Copyright 2025 TecOnca Data Solutions.
--  Extract from PostgreSQL.

use {{ database_name }};

SELECT
    info.TABLE_NAME
    , info.COLUMN_NAME
    , info.DATA_TYPE
    , const.CONSTRAINT_TYPE
    , col_description(
        (info.TABLE_SCHEMA || '.' || info.TABLE_NAME)::regclass,
        info.ORDINAL_POSITION
      ) AS column_description
FROM INFORMATION_SCHEMA.COLUMNS info
LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE use_name
    ON use_name.TABLE_SCHEMA = info.TABLE_SCHEMA
    AND use_name.TABLE_NAME = info.TABLE_NAME
    AND use_name.COLUMN_NAME = info.COLUMN_NAME
LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS const
    ON use_name.CONSTRAINT_NAME = const.CONSTRAINT_NAME
    AND use_name.TABLE_SCHEMA = const.TABLE_SCHEMA
where info.TABLE_NAME in (
    ${tables}
);
