--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(countyMUNID AS SIGNED) AS countyMUNID,
    DATE_FORMAT(createdAt, "%Y-%m-%d %H:%M:%S.%f") AS createdAt,
    CAST(file AS CHAR) AS file,
    CAST(id AS SIGNED) AS id,
    CAST(stateId AS SIGNED) AS stateId,
    CAST(status AS CHAR) AS status,
    CAST(type AS CHAR) AS type,
    CAST(typeSchool AS CHAR) AS typeSchool,
    DATE_FORMAT(updatedAt, "%Y-%m-%d %H:%M:%S") AS updatedAt,
    CAST(userUSUID AS SIGNED) AS userUSUID
FROM Saev.microdata limit 1