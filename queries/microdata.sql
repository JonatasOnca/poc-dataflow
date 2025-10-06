--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(countyMUNID AS SIGNED) AS countyMUNID,
    CAST(createdAt AS CHAR) AS createdAt,
    CAST(file AS CHAR) AS file,
    CAST(id AS SIGNED) AS id,
    CAST(stateId AS SIGNED) AS stateId,
    CAST(status AS CHAR) AS status,
    CAST(type AS CHAR) AS type,
    CAST(typeSchool AS CHAR) AS typeSchool,
    CAST(updatedAt AS CHAR) AS updatedAt,
    CAST(userUSUID AS SIGNED) AS userUSUID
FROM Saev.microdata limit 1