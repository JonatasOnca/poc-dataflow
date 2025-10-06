--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(id AS SIGNED) AS id,
    CAST(name AS CHAR) AS name,
    CAST(timestamp AS SIGNED) AS timestamp
FROM Saev.migrations limit 1