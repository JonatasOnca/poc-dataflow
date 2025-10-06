--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(active AS SIGNED) AS active,
    CAST(category AS CHAR) AS category,
    DATE_FORMAT(createdAt, "%Y-%m-%d %H:%i:%s.%f") AS createdAt,
    CAST(description AS CHAR) AS description,
    CAST(id AS SIGNED) AS id,
    CAST(link AS CHAR) AS link,
    CAST(name AS CHAR) AS name,
    CAST(role AS CHAR) AS role,
    DATE_FORMAT(updatedAt, "%Y-%m-%d %H:%i:%s.%f") AS updatedAt
FROM Saev.external_reports
