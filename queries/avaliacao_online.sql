--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(active AS SIGNED) AS active,
    DATE_FORMAT(createdAt, "%Y-%m-%d %H:%i:%s.%f") AS createdAt,
    CAST(id AS SIGNED) AS id,
    DATE_FORMAT(updatedAt, "%Y-%m-%d %H:%i:%s.%f") AS updatedAt
FROM Saev.avaliacao_online
