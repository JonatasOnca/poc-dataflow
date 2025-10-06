--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    DATE_FORMAT(createdAt, "%Y-%m-%d %H:%i:%s.%f") AS createdAt,
    DATE_FORMAT(endDate, "%Y-%m-%d %H:%i:%s.%f") AS endDate,
    CAST(id AS SIGNED) AS id,
    CAST(schoolClassTURID AS SIGNED) AS schoolClassTURID,
    DATE_FORMAT(startDate, "%Y-%m-%d %H:%i:%s.%f") AS startDate,
    CAST(studentALUID AS SIGNED) AS studentALUID,
    DATE_FORMAT(updatedAt, "%Y-%m-%d %H:%i:%s.%f") AS updatedAt
FROM Saev.turma_aluno
