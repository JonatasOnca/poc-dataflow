--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(createdAt AS CHAR) AS createdAt,
    CAST(description AS CHAR) AS description,
    CAST(id AS CHAR) AS id,
    CAST(image AS CHAR) AS image,
    CAST(`option` AS  CHAR) AS `option`,
    CAST(questionId AS CHAR) AS questionId,
    CAST(updatedAt AS CHAR) AS updatedAt
FROM Saev.avaliacao_online_question_alternative limit 100