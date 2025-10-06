--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    DATE_FORMAT(createdAt, "%Y-%m-%dT%H:%M:%S.%f") AS createdAt,
    CAST(descriptorMTIID AS SIGNED) AS descriptorMTIID,
    CAST(id AS SIGNED) AS id,
    CAST(reportEditionId AS SIGNED) AS reportEditionId,
    CAST(testTESID AS SIGNED) AS testTESID,
    CAST(total AS SIGNED) AS total,
    CAST(totalCorrect AS SIGNED) AS totalCorrect,
    DATE_FORMAT(updatedAt, "%Y-%m-%dT%H:%M:%S.%f") AS updatedAt
FROM Saev.report_descriptor limit 1