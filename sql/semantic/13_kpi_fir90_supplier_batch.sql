-- Certified KPI: FIR90 per 1,000 matured vehicles by the supplier batch of the pack fitted at EOL.
SELECT supplier_batch, supplier_id, vendor_batch, batch_status,
       count(*) AS vehicles_matured,
       sum(battery_claim_90d) AS vehicles_failed,
       round(1000 * sum(battery_claim_90d) / count(*), 2) AS fir90_per_1000
FROM ${semantic}.v_vehicle_fir90
WHERE matured = 1
GROUP BY supplier_batch, supplier_id, vendor_batch, batch_status
