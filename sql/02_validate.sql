SELECT COUNT(*) AS n_rows
FROM supply_chain_risk;

SELECT COUNT(*) AS n_rows
FROM commodity_prices;

SELECT
    COUNT(*) AS total_rows,
    COUNT(copper__usd_per_mt) AS non_null_copper
FROM supply_chain_enriched;

SELECT COUNT(*) AS n_rows
FROM supply_chain_enriched_overlap;

SELECT MIN(month) AS min_supply_month, MAX(month) AS max_supply_month
FROM supply_chain_risk_v;

SELECT MIN(month) AS min_commodity_month, MAX(month) AS max_commodity_month
FROM commodity_prices_v
WHERE copper__usd_per_mt IS NOT NULL;