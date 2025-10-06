--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(content AS CHAR) AS content,
    CAST(createdAt AS CHAR) AS createdAt,
    CAST(filters AS json) AS filters,
    CAST(id AS SIGNED) AS id,
    CAST(schoolId AS SIGNED) AS schoolId,
    CAST(title AS CHAR) AS title,
    CAST(updatedAt AS CHAR) AS updatedAt
FROM Saev.tutor_mensagens limit 1