--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(countPresentStudents AS CHAR) AS countPresentStudents,
    CAST(countStudentsLaunched AS CHAR) AS countStudentsLaunched,
    CAST(countTotalStudents AS CHAR) AS countTotalStudents,
    CAST(createdAt AS CHAR) AS createdAt,
    CAST(fluente AS CHAR) AS fluente,
    CAST(frases AS CHAR) AS frases,
    CAST(id AS CHAR) AS id,
    CAST(idStudents AS CHAR) AS idStudents,
    CAST(name AS CHAR) AS name,
    CAST(nao_avaliado AS CHAR) AS nao_avaliado,
    CAST(nao_fluente AS CHAR) AS nao_fluente,
    CAST(nao_informado AS CHAR) AS nao_informado,
    CAST(nao_leitor AS CHAR) AS nao_leitor,
    CAST(palavras AS CHAR) AS palavras,
    CAST(reportEditionId AS CHAR) AS reportEditionId,
    CAST(silabas AS CHAR) AS silabas,
    CAST(testTESID AS CHAR) AS testTESID,
    CAST(totalGradesStudents AS CHAR) AS totalGradesStudents,
    CAST(type AS CHAR) AS type,
    CAST(updatedAt AS CHAR) AS updatedAt
FROM Saev.report_subject