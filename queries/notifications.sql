--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(createdAt AS CHAR) AS createdAt,
    CAST(id AS SIGNED) AS id,
    CAST(isReading AS SIGNED) AS isReading,
    CAST(message AS CHAR) AS message,
    CAST(title AS CHAR) AS title,
    CAST(updateAt AS CHAR) AS updateAt,
    CAST(userUSUID AS SIGNED) AS userUSUID
FROM Saev.notifications limit 1