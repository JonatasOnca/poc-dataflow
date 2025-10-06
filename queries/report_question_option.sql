--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    DATE_FORMAT(createdAt, "%Y-%m-%d %H:%i:%s.%f") AS createdAt,
    CAST(fluente AS SIGNED) AS fluente,
    CAST(frases AS SIGNED) AS frases,
    CAST(id AS SIGNED) AS id,
    CAST(nao_avaliado AS SIGNED) AS nao_avaliado,
    CAST(nao_fluente AS SIGNED) AS nao_fluente,
    CAST(nao_informado AS SIGNED) AS nao_informado,
    CAST(nao_leitor AS SIGNED) AS nao_leitor,
    CAST(option AS CHAR) AS option,
    CAST(palavras AS SIGNED) AS palavras,
    CAST(reportQuestionId AS SIGNED) AS reportQuestionId,
    CAST(silabas AS SIGNED) AS silabas,
    CAST(totalCorrect AS SIGNED) AS totalCorrect
FROM Saev.report_question_option
