--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    DATE_FORMAT(createdAt, "%Y-%m-%d %H:%M:%S.%f") AS createdAt,
    CAST(id AS CHAR) AS id,
    CAST(method AS CHAR) AS method,
    CAST(stateFinal AS CHAR) AS stateFinal,
    CAST(stateInitial AS CHAR) AS stateInitial,
    DATE_FORMAT(updatedAt, "%Y-%m-%d %H:%M:%S.%f") AS updatedAt,
    CAST(userUSUID AS SIGNED) AS userUSUID
FROM Saev.system_logs limit 1