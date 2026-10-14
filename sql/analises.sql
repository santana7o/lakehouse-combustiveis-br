-- Consultas prontas para o Databricks SQL Editor / Dashboard AI/BI.
-- Em dev o bundle prefixa o schema com seu usuário; em prod o schema é `combustiveis`.
USE CATALOG workspace;
USE SCHEMA dev_felipe_dhesantana_combustiveis_dev;

-- 1. Evolução do preço médio da gasolina por região
SELECT semana, regiao, ROUND(AVG(preco_medio), 3) AS preco_medio
FROM gold_preco_semanal_uf
WHERE produto = 'GASOLINA'
GROUP BY ALL
ORDER BY semana, regiao;

-- 2. Onde o etanol compensa na semana mais recente
SELECT uf, preco_etanol, preco_gasolina, paridade, etanol_compensa
FROM gold_paridade_etanol
WHERE semana = (SELECT MAX(semana) FROM gold_paridade_etanol)
ORDER BY paridade;

-- 3. UFs mais caras e mais baratas (gasolina, última semana)
SELECT uf, preco_medio, preco_minimo, preco_maximo, postos_pesquisados
FROM gold_preco_semanal_uf
WHERE produto = 'GASOLINA'
  AND semana = (SELECT MAX(semana) FROM gold_preco_semanal_uf)
ORDER BY preco_medio DESC;

-- 4. Bandeira mais barata em cada UF
SELECT uf, produto, bandeira, preco_medio, postos
FROM gold_ranking_bandeiras
WHERE ranking = 1 AND produto = 'GASOLINA'
ORDER BY preco_medio;

-- 5. Qualidade de dados: quantas linhas cada expectativa descartou
SELECT
  details:flow_progress:data_quality:expectations AS expectativas,
  timestamp
FROM event_log(TABLE(silver_precos))
WHERE event_type = 'flow_progress'
  AND details:flow_progress:data_quality IS NOT NULL
ORDER BY timestamp DESC
LIMIT 5;
