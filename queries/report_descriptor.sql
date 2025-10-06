--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(createdAt AS CHAR) AS createdAt,
    CAST(descriptorMTIID AS CHAR) AS descriptorMTIID,
    CAST(id AS CHAR) AS id,
    CAST(reportEditionId AS CHAR) AS reportEditionId,
    CAST(testTESID AS CHAR) AS testTESID,
    CAST(total AS CHAR) AS total,
    CAST(totalCorrect AS CHAR) AS totalCorrect,
    CAST(updatedAt AS CHAR) AS updatedAt
FROM Saev_Final.report_descriptor limit 1