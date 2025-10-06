--  Copyright 2025 TecOnca Data Solutions.

SELECT 
    CAST(createdAt AS CHAR) AS createdAt,
    CAST(endDate AS CHAR) AS endDate,
    CAST(id AS SIGNED) AS id,
    CAST(schoolClassTURID AS SIGNED) AS schoolClassTURID,
    CAST(startDate AS CHAR) AS startDate,
    CAST(studentALUID AS SIGNED) AS studentALUID,
    CAST(updatedAt AS CHAR) AS updatedAt
FROM Saev.turma_aluno limit 1