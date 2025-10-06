--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(countyMUNID AS SIGNED) AS countyMUNID,
    DATE_FORMAT(createdAt, "%Y-%m-%dT%H:%M:%S.%f") AS createdAt,
    CAST(editionAVAID AS SIGNED) AS editionAVAID,
    CAST(id AS SIGNED) AS id,
    CAST(regionalId AS SIGNED) AS regionalId,
    CAST(schoolClassTURID AS SIGNED) AS schoolClassTURID,
    CAST(schoolESCID AS SIGNED) AS schoolESCID,
    CAST(type AS CHAR) AS type,
    DATE_FORMAT(updatedAt, "%Y-%m-%dT%H:%M:%S.%f") AS updatedAt
FROM Saev.report_edition limit 1