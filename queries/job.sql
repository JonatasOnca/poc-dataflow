--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(assessmentId AS CHAR) AS assessmentId,
    CAST(bullId AS CHAR) AS bullId,
    CAST(countyId AS CHAR) AS countyId,
    CAST(createdAt AS CHAR) AS createdAt,
    CAST(endDate AS CHAR) AS endDate,
    CAST(id AS CHAR) AS id,
    CAST(jobType AS CHAR) AS jobType,
    CAST(startDate AS CHAR) AS startDate,
    CAST(updatedAt AS CHAR) AS updatedAt
FROM Saev.job 