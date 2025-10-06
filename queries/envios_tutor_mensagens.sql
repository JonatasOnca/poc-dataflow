--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    DATE_FORMAT(createdAt, "%Y-%m-%d %H:%M:%S.%f") AS createdAt,
    CAST(id AS SIGNED) AS id,
    CAST(statusEmail AS CHAR) AS statusEmail,
    CAST(statusWhatsapp AS CHAR) AS statusWhatsapp,
    CAST(studentId AS SIGNED) AS studentId,
    CAST(tutorMessageId AS SIGNED) AS tutorMessageId
FROM Saev.envios_tutor_mensagens limit 1