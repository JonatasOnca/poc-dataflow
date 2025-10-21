--  Copyright 2025 TecOnca Data Solutions.
--  Extract from MySQL.

select
    info.TABLE_NAME as TABLE_NAME
    , info.COLUMN_NAME as COLUMN_NAME
    , info.DATA_TYPE as DATA_TYPE
    , const.CONSTRAINT_TYPE as CONSTRAINT_TYPE
    , info.COLUMN_COMMENT  as COLUMN_COMMENT
from INFORMATION_SCHEMA.COLUMNS info
left join INFORMATION_SCHEMA.KEY_COLUMN_USAGE use_name
ON
    use_name.TABLE_NAME = info.TABLE_NAME
    and use_name.COLUMN_NAME = info.COLUMN_NAME
left join INFORMATION_SCHEMA.TABLE_CONSTRAINTS const
ON
    info.TABLE_NAME = const.TABLE_NAME
    and use_name.COLUMN_NAME = info.COLUMN_NAME
    and use_name.CONSTRAINT_NAME = const.CONSTRAINT_NAME
where
    info.TABLE_SCHEMA = 'testdb'
    and info.TABLE_NAME not in ('')
order by
    info.TABLE_NAME, info.ORDINAL_POSITION;
