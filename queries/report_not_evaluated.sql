--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(abandono AS CHAR) AS abandono,
    CAST(ausencia AS CHAR) AS ausencia,
    CAST(countPresentStudents AS CHAR) AS countPresentStudents,
    CAST(countStudentsLaunched AS CHAR) AS countStudentsLaunched,
    CAST(countTotalStudents AS CHAR) AS countTotalStudents,
    CAST(createdAt AS CHAR) AS createdAt,
    CAST(deficiencia AS CHAR) AS deficiencia,
    CAST(id AS CHAR) AS id,
    CAST(idStudents AS CHAR) AS idStudents,
    CAST(name AS CHAR) AS name,
    CAST(nao_participou AS CHAR) AS nao_participou,
    CAST(recusa AS CHAR) AS recusa,
    CAST(reportEditionId AS CHAR) AS reportEditionId,
    CAST(testTESID AS CHAR) AS testTESID,
    CAST(transferencia AS CHAR) AS transferencia,
    CAST(type AS CHAR) AS type,
    CAST(updatedAt AS CHAR) AS updatedAt
FROM Saev.report_not_evaluated limit 1