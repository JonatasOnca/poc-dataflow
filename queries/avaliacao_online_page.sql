--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(assessmentOnlineId AS CHAR) AS assessmentOnlineId,
    CAST(createdAt AS CHAR) AS createdAt,
    CAST(id AS CHAR) AS id,
    CAST(image AS CHAR) AS image,
    CAST(`order` AS CHAR) AS `order`,
    CAST(title AS CHAR) AS title,
    CAST(updatedAt AS CHAR) AS updatedAt
FROM Saev.avaliacao_online_page limit 100