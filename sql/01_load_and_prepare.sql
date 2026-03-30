CREATE OR REPLACE TABLE supply_chain_risk AS
SELECT *
FROM read_csv('data/processed/supply_chain_risk_clean.csv');

CREATE OR REPLACE TABLE commodity_prices AS
SELECT *
FROM read_csv('data/processed/commodity_prices_clean.csv');

CREATE OR REPLACE VIEW commodity_prices_v AS
SELECT
    period_code,
    CAST(strptime(replace(period_code, 'M', '-') || '-01', '%Y-%m-%d') AS DATE) AS month,
    * EXCLUDE (period_code)
FROM commodity_prices;

CREATE OR REPLACE VIEW supply_chain_risk_v AS
SELECT
    *,
    CAST(date_trunc('month', date) AS DATE) AS month
FROM supply_chain_risk;

CREATE OR REPLACE TABLE supply_chain_enriched AS
SELECT
    s.shipment_id,
    s.date,
    s.month,
    s.origin_port,
    s.destination_port,
    s.transport_mode,
    s.product_category,
    s.distance_km,
    s.weight_mt,
    s.fuel_price_index,
    s.geopolitical_risk_score,
    s.weather_condition,
    s.carrier_reliability_score,
    s.lead_time_days,
    s.disruption_occurred,
    c.copper__usd_per_mt
FROM supply_chain_risk_v s
LEFT JOIN commodity_prices_v c
    ON s.month = c.month;

CREATE OR REPLACE TABLE supply_chain_enriched_overlap AS
SELECT *
FROM supply_chain_enriched
WHERE copper__usd_per_mt IS NOT NULL;