--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    DATE_FORMAT(createdAt, "%Y-%m-%dT%H:%M:%S.%f") AS createdAt,
    CAST(description AS CHAR) AS description,
    CAST(id AS SIGNED) AS id,
    CAST(image AS CHAR) AS image,
    CAST(option AS CHAR) AS option,
    CAST(questionId AS SIGNED) AS questionId,
    DATE_FORMAT(updatedAt, "%Y-%m-%dT%H:%M:%S.%f") AS updatedAt
FROM Saev.avaliacao_online_question_alternative limit 1