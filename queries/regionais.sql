--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(active AS CHAR) AS active,
    CAST(countyId AS CHAR) AS countyId,
    CAST(createdAt AS CHAR) AS createdAt,
    CAST(id AS CHAR) AS id,
    CAST(name AS CHAR) AS name,
    CAST(stateId AS CHAR) AS stateId,
    CAST(type AS CHAR) AS type,
    CAST(updatedAt AS CHAR) AS updatedAt
FROM Saev.regionais limit 100