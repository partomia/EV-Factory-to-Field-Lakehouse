-- Certified KPI: field incident rate within 90 days in service, per 1,000 matured vehicles, by build week.
SELECT build_week,
       count(*) AS vehicles_matured,
       sum(battery_claim_90d) AS vehicles_failed,
       round(1000 * sum(battery_claim_90d) / count(*), 2) AS fir90_per_1000
FROM ${semantic}.v_vehicle_fir90
WHERE matured = 1
GROUP BY build_week
