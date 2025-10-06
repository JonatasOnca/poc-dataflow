--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(createdAt AS CHAR) AS createdAt,
    CAST(id AS CHAR) AS id,
    CAST(method AS CHAR) AS method,
    CAST(stateFinal AS CHAR) AS stateFinal,
    CAST(stateInitial AS CHAR) AS stateInitial,
    CAST(updatedAt AS CHAR) AS updatedAt,
    CAST(userUSUID AS SIGNED) AS userUSUID
FROM Saev.system_logs limit 1