--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(active AS CHAR) AS active,
    CAST(createdAt AS CHAR) AS createdAt,
    CAST(id AS CHAR) AS id,
    CAST(updatedAt AS CHAR) AS updatedAt
FROM Saev.avaliacao_online limit 100