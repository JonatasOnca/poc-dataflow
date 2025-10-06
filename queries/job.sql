--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(assessmentId AS SIGNED) AS assessmentId,
    CAST(bullId AS CHAR) AS bullId,
    CAST(countyId AS SIGNED) AS countyId,
    DATE_FORMAT(createdAt, "%Y-%m-%d %H:%i:%s.%f") AS createdAt,
    DATE_FORMAT(endDate, "%Y-%m-%d %H:%i:%s.%f") AS endDate,
    CAST(id AS SIGNED) AS id,
    CAST(jobType AS CHAR) AS jobType,
    DATE_FORMAT(startDate, "%Y-%m-%d %H:%i:%s.%f") AS startDate,
    DATE_FORMAT(updatedAt, "%Y-%m-%d %H:%i:%s.%f") AS updatedAt
FROM Saev.job
