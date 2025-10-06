--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(countPresentStudents AS SIGNED) AS countPresentStudents,
    CAST(countStudentsLaunched AS SIGNED) AS countStudentsLaunched,
    CAST(countTotalStudents AS SIGNED) AS countTotalStudents,
    CAST(createdAt AS CHAR) AS createdAt,
    CAST(fluente AS SIGNED) AS fluente,
    CAST(frases AS SIGNED) AS frases,
    CAST(id AS SIGNED) AS id,
    CAST(idStudents AS CHAR) AS idStudents,
    CAST(name AS CHAR) AS name,
    CAST(nao_avaliado AS SIGNED) AS nao_avaliado,
    CAST(nao_fluente AS SIGNED) AS nao_fluente,
    CAST(nao_informado AS SIGNED) AS nao_informado,
    CAST(nao_leitor AS SIGNED) AS nao_leitor,
    CAST(palavras AS SIGNED) AS palavras,
    CAST(reportEditionId AS SIGNED) AS reportEditionId,
    CAST(silabas AS SIGNED) AS silabas,
    CAST(testTESID AS SIGNED) AS testTESID,
    CAST(totalGradesStudents AS SIGNED) AS totalGradesStudents,
    CAST(type AS CHAR) AS type,
    CAST(updatedAt AS CHAR) AS updatedAt
FROM Saev.report_subject limit 1