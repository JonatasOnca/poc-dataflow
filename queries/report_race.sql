--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(countPresentStudents AS SIGNED) AS countPresentStudents,
    CAST(countStudentsLaunched AS SIGNED) AS countStudentsLaunched,
    CAST(countTotalStudents AS SIGNED) AS countTotalStudents,
    DATE_FORMAT(createdAt, "%Y-%m-%d %H:%i:%s.%f") AS createdAt,
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
    CAST(racePELID AS SIGNED) AS racePELID,
    CAST(reportSubjectId AS SIGNED) AS reportSubjectId,
    CAST(silabas AS SIGNED) AS silabas,
    CAST(totalGradesStudents AS SIGNED) AS totalGradesStudents,
    DATE_FORMAT(updatedAt, "%Y-%m-%d %H:%i:%s.%f") AS updatedAt
FROM Saev.report_race
