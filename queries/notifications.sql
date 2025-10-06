--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    DATE_FORMAT(createdAt, "%Y-%m-%d %H:%M:%S.%f") AS createdAt,
    CAST(id AS SIGNED) AS id,
    CAST(isReading AS SIGNED) AS isReading,
    CAST(message AS CHAR) AS message,
    CAST(title AS CHAR) AS title,
    DATE_FORMAT(updateAt, "%Y-%m-%d %H:%M:%S") AS updateAt,
    CAST(userUSUID AS SIGNED) AS userUSUID
FROM Saev.notifications limit 1