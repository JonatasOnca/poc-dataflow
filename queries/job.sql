--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(assessmentId AS SIGNED) AS assessmentId,
    CAST(bullId AS CHAR) AS bullId,
    CAST(countyId AS SIGNED) AS countyId,
    DATE_FORMAT(createdAt, "%Y-%m-%d %H:%M:%S.%f") AS createdAt,
    DATE_FORMAT(endDate, "%Y-%m-%d %H:%M:%S") AS endDate,
    CAST(id AS SIGNED) AS id,
    CAST(jobType AS CHAR) AS jobType,
    DATE_FORMAT(startDate, "%Y-%m-%d %H:%M:%S") AS startDate,
    DATE_FORMAT(updatedAt, "%Y-%m-%d %H:%M:%S") AS updatedAt
FROM Saev.job limit 1