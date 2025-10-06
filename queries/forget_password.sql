--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(createdAt AS CHAR) AS createdAt,
    CAST(id AS SIGNED) AS id,
    CAST(isValid AS SIGNED) AS isValid,
    CAST(token AS CHAR) AS token,
    CAST(updatedAt AS CHAR) AS updatedAt,
    CAST(userUSUID AS SIGNED) AS userUSUID
FROM Saev.forget_password limit 1