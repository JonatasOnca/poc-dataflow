-- Calcula o tamanho médio em bytes para a tabela 'orders'
SELECT AVG(
    COALESCE(LENGTH(order_id), 0) + 
    COALESCE(LENGTH(customer_id), 0) + 
    COALESCE(LENGTH(order_status), 0) + 
    8 + -- Para um BIGINT
    4 + -- Para um DECIMAL/FLOAT
    6   -- Para um DATETIME
    -- Some o tamanho estimado de cada coluna
) as avg_row_size_bytes
FROM orders LIMIT 100000; -- Use um sample grande