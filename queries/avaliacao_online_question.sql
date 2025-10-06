--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(createdAt AS CHAR) AS createdAt,
    CAST(description AS CHAR) AS description,
    CAST(id AS SIGNED) AS id,
    CAST(order AS SIGNED) AS order,
    CAST(pageId AS SIGNED) AS pageId,
    CAST(questionTemplateId AS SIGNED) AS questionTemplateId,
    CAST(updatedAt AS CHAR) AS updatedAt
FROM Saev.avaliacao_online_question limit 1