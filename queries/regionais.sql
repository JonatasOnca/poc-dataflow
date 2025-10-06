--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(active AS SIGNED) AS active,
    CAST(countyId AS SIGNED) AS countyId,
    CAST(createdAt AS CHAR) AS createdAt,
    CAST(id AS SIGNED) AS id,
    CAST(name AS CHAR) AS name,
    CAST(stateId AS SIGNED) AS stateId,
    CAST(type AS CHAR) AS type,
    CAST(updatedAt AS CHAR) AS updatedAt
FROM Saev.regionais limit 1