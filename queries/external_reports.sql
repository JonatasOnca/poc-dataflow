--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(active AS SIGNED) AS active,
    CAST(category AS CHAR) AS category,
    CAST(createdAt AS CHAR) AS createdAt,
    CAST(description AS CHAR) AS description,
    CAST(id AS SIGNED) AS id,
    CAST(link AS CHAR) AS link,
    CAST(name AS CHAR) AS name,
    CAST(role AS CHAR) AS role,
    CAST(updatedAt AS CHAR) AS updatedAt
FROM Saev.external_reports limit 1