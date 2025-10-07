--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(createdAt AS CHAR) AS createdAt,
    CAST(description AS CHAR) AS description,
    CAST(id AS CHAR) AS id,
    CAST(`order` AS CHAR) AS `order`,
    CAST(pageId AS CHAR) AS pageId,
    CAST(questionTemplateId AS CHAR) AS questionTemplateId,
    CAST(updatedAt AS CHAR) AS updatedAt
FROM Saev.avaliacao_online_question limit 1