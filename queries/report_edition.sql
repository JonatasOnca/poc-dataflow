--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(countyMUNID AS CHAR) AS countyMUNID,
    CAST(createdAt AS CHAR) AS createdAt,
    CAST(editionAVAID AS CHAR) AS editionAVAID,
    CAST(id AS CHAR) AS id,
    CAST(regionalId AS CHAR) AS regionalId,
    CAST(schoolClassTURID AS CHAR) AS schoolClassTURID,
    CAST(schoolESCID AS CHAR) AS schoolESCID,
    CAST(type AS CHAR) AS type,
    CAST(updatedAt AS CHAR) AS updatedAt
FROM Saev.report_edition limit 100