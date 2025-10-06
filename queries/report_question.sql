--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(fluente AS SIGNED) AS fluente,
    CAST(frases AS SIGNED) AS frases,
    CAST(id AS SIGNED) AS id,
    CAST(nao_avaliado AS SIGNED) AS nao_avaliado,
    CAST(nao_fluente AS SIGNED) AS nao_fluente,
    CAST(nao_informado AS SIGNED) AS nao_informado,
    CAST(nao_leitor AS SIGNED) AS nao_leitor,
    CAST(option_correct AS CHAR) AS option_correct,
    CAST(palavras AS SIGNED) AS palavras,
    CAST(questionTEGID AS SIGNED) AS questionTEGID,
    CAST(reportSubjectId AS SIGNED) AS reportSubjectId,
    CAST(silabas AS SIGNED) AS silabas,
    CAST(total_a AS SIGNED) AS total_a,
    CAST(total_b AS SIGNED) AS total_b,
    CAST(total_c AS SIGNED) AS total_c,
    CAST(total_d AS SIGNED) AS total_d,
    CAST(total_null AS SIGNED) AS total_null
FROM Saev.report_question
