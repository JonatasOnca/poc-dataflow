--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(abbreviation AS CHAR) AS abbreviation,
    CAST(active AS SIGNED) AS active,
    DATE_FORMAT(createdAt, "%Y-%m-%d %H:%M:%S.%f") AS createdAt,
    CAST(id AS SIGNED) AS id,
    CAST(name AS CHAR) AS name,
    DATE_FORMAT(updatedAt, "%Y-%m-%d %H:%M:%S.%f") AS updatedAt
FROM Saev.estados limit 1