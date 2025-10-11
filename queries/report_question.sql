--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(fluente AS CHAR) AS fluente,
    CAST(frases AS CHAR) AS frases,
    CAST(id AS CHAR) AS id,
    CAST(nao_avaliado AS CHAR) AS nao_avaliado,
    CAST(nao_fluente AS CHAR) AS nao_fluente,
    CAST(nao_informado AS CHAR) AS nao_informado,
    CAST(nao_leitor AS CHAR) AS nao_leitor,
    CAST(option_correct AS CHAR) AS option_correct,
    CAST(palavras AS CHAR) AS palavras,
    CAST(questionTEGID AS CHAR) AS questionTEGID,
    CAST(reportSubjectId AS CHAR) AS reportSubjectId,
    CAST(silabas AS CHAR) AS silabas,
    CAST(total_a AS CHAR) AS total_a,
    CAST(total_b AS CHAR) AS total_b,
    CAST(total_c AS CHAR) AS total_c,
    CAST(total_d AS CHAR) AS total_d,
    CAST(total_null AS CHAR) AS total_null
FROM Saev.report_question