--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(abandono AS SIGNED) AS abandono,
    CAST(ausencia AS SIGNED) AS ausencia,
    CAST(countPresentStudents AS SIGNED) AS countPresentStudents,
    CAST(countStudentsLaunched AS SIGNED) AS countStudentsLaunched,
    CAST(countTotalStudents AS SIGNED) AS countTotalStudents,
    DATE_FORMAT(createdAt, "%Y-%m-%dT%H:%M:%S.%f") AS createdAt,
    CAST(deficiencia AS SIGNED) AS deficiencia,
    CAST(id AS SIGNED) AS id,
    CAST(idStudents AS CHAR) AS idStudents,
    CAST(name AS CHAR) AS name,
    CAST(nao_participou AS SIGNED) AS nao_participou,
    CAST(recusa AS SIGNED) AS recusa,
    CAST(reportEditionId AS SIGNED) AS reportEditionId,
    CAST(testTESID AS SIGNED) AS testTESID,
    CAST(transferencia AS SIGNED) AS transferencia,
    CAST(type AS CHAR) AS type,
    DATE_FORMAT(updatedAt, "%Y-%m-%dT%H:%M:%S.%f") AS updatedAt
FROM Saev.report_not_evaluated limit 1