--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(active AS SIGNED) AS active,
    CAST(countyId AS SIGNED) AS countyId,
    DATE_FORMAT(createdAt, "%Y-%m-%d %H:%M:%S.%f") AS createdAt,
    CAST(id AS SIGNED) AS id,
    CAST(name AS CHAR) AS name,
    CAST(stateId AS SIGNED) AS stateId,
    CAST(type AS CHAR) AS type,
    DATE_FORMAT(updatedAt, "%Y-%m-%d %H:%M:%S") AS updatedAt
FROM Saev.regionais limit 1