--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(createdAt AS CHAR) AS createdAt,
    CAST(descriptorMTIID AS SIGNED) AS descriptorMTIID,
    CAST(id AS SIGNED) AS id,
    CAST(reportEditionId AS SIGNED) AS reportEditionId,
    CAST(testTESID AS SIGNED) AS testTESID,
    CAST(total AS SIGNED) AS total,
    CAST(totalCorrect AS SIGNED) AS totalCorrect,
    CAST(updatedAt AS CHAR) AS updatedAt
FROM Saev.report_descriptor limit 1