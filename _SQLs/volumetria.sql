--  Copyright 2025 TecOnca Data Solutions.


SELECT 
    TABLE_NAME AS `Tabela`,
    -- (DATA_LENGTH + INDEX_LENGTH) é o tamanho total em bytes
    ROUND(((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024), 2) AS `Tamanho Total (MB)`,
    ROUND((DATA_LENGTH / 1024 / 1024), 2) AS `Dados (MB)`,
    ROUND((INDEX_LENGTH / 1024 / 1024), 2) AS `Índices (MB)`,
    TABLE_ROWS AS `Total de Linhas`
FROM 
    information_schema.TABLES
WHERE 
    TABLE_SCHEMA = 'banco'
--    AND TABLE_NAME IN ('tabela1', 'tabela2', 'tabela3', '...') -- Sua lista de tabelas
ORDER BY 
    (DATA_LENGTH + INDEX_LENGTH) DESC;