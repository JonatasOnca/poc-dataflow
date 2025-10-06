--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(assessmentOnlineId AS SIGNED) AS assessmentOnlineId,
    DATE_FORMAT(createdAt, "%Y-%m-%d %H:%i:%s.%f") AS createdAt,
    CAST(id AS SIGNED) AS id,
    CAST(image AS CHAR) AS image,
    CAST(order AS SIGNED) AS order,
    CAST(title AS CHAR) AS title,
    DATE_FORMAT(updatedAt, "%Y-%m-%d %H:%i:%s.%f") AS updatedAt
FROM Saev.avaliacao_online_page
