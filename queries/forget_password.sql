--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(createdAt AS CHAR) AS createdAt,
    CAST(id AS CHAR) AS id,
    CAST(isValid AS CHAR) AS isValid,
    CAST(token AS CHAR) AS token,
    CAST(updatedAt AS CHAR) AS updatedAt,
    CAST(userUSUID AS CHAR) AS userUSUID
FROM Saev_Final.forget_password limit 1