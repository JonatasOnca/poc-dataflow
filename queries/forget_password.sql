--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    DATE_FORMAT(createdAt, "%Y-%m-%d %H:%i:%s.%f") AS createdAt,
    CAST(id AS SIGNED) AS id,
    CAST(isValid AS SIGNED) AS isValid,
    CAST(token AS CHAR) AS token,
    DATE_FORMAT(updatedAt, "%Y-%m-%d %H:%i:%s.%f") AS updatedAt,
    CAST(userUSUID AS SIGNED) AS userUSUID,
    CAST(userUSUID AS SIGNED) AS userUSUID
FROM Saev.forget_password
