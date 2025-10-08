--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(createdAt AS CHAR) AS createdAt,
    CAST(endDate AS CHAR) AS endDate,
    CAST(id AS CHAR) AS id,
    CAST(schoolClassTURID AS CHAR) AS schoolClassTURID,
    CAST(startDate AS CHAR) AS startDate,
    CAST(studentALUID AS CHAR) AS studentALUID,
    CAST(updatedAt AS CHAR) AS updatedAt
FROM Saev.turma_aluno limit 100