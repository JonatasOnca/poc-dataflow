--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(active AS SIGNED) AS active,
    DATE_FORMAT(createdAt, "%Y-%m-%dT%H:%M:%S.%f") AS createdAt,
    CAST(id AS SIGNED) AS id,
    DATE_FORMAT(updatedAt, "%Y-%m-%dT%H:%M:%S.%f") AS updatedAt
FROM Saev.avaliacao_online limit 1