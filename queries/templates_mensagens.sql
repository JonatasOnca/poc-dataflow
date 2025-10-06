--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(content AS CHAR) AS content,
    DATE_FORMAT(createdAt, "%Y-%m-%d %H:%M:%S.%f") AS createdAt,
    CAST(id AS SIGNED) AS id,
    CAST(schoolId AS SIGNED) AS schoolId,
    CAST(title AS CHAR) AS title,
    DATE_FORMAT(updatedAt, "%Y-%m-%d %H:%M:%S.%f") AS updatedAt
FROM Saev.templates_mensagens limit 1