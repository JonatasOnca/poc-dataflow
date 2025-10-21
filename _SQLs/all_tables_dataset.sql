--  Copyright 2025 TecOnca Data Solutions.


DECLARE PROJETO STRING DEFAULT 'abemcomum-saev-prod';
DECLARE DATASET STRING DEFAULT 'bronze';

EXECUTE IMMEDIATE FORMAT("""
    SELECT
        table_name,
        table_type,
        creation_time
    FROM
        `%s.%s`.INFORMATION_SCHEMA.TABLES
    ORDER BY
        table_name
""", PROJETO, DATASET);