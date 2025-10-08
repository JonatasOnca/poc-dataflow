--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(abbreviation AS CHAR) AS abbreviation,
    CAST(active AS CHAR) AS active,
    CAST(createdAt AS CHAR) AS createdAt,
    CAST(id AS CHAR) AS id,
    CAST(name AS CHAR) AS name,
    CAST(updatedAt AS CHAR) AS updatedAt
FROM Saev.estados limit 100