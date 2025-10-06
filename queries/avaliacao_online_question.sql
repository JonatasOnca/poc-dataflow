--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    DATE_FORMAT(createdAt, "%Y-%m-%d %H:%i:%s.%f") AS createdAt,
    CAST(description AS CHAR) AS description,
    CAST(id AS SIGNED) AS id,
    CAST(order AS SIGNED) AS order,
    CAST(pageId AS SIGNED) AS pageId,
    CAST(questionTemplateId AS SIGNED) AS questionTemplateId,
    CAST(questionTemplateId AS SIGNED) AS questionTemplateId,
    DATE_FORMAT(updatedAt, "%Y-%m-%d %H:%i:%s.%f") AS updatedAt
FROM Saev.avaliacao_online_question
