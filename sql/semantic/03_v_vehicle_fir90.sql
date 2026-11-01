-- One row per vehicle. In service from delivery (EOL date when the dealer system has no delivery). Matured: 90 days
-- in service by the data's as-of date (last telematics date loaded). Failed: a battery-related warranty claim,
-- resolved to this VIN through master.xref, with a failure date within 90 days of entering service.
-- Attributed to the build week of vehicle EOL and the supplier batch of the pack fitted at EOL.
SELECT v.vin, v.vehicle_line, v.model, v.eol_pack_serial,
       CAST(v.eol_ts AS DATE) AS eol_date, d.build_week,
       coalesce(v.delivery_date, CAST(v.eol_ts AS DATE)) AS in_service_date, a.as_of_date,
       CASE WHEN datediff(a.as_of_date, coalesce(v.delivery_date, CAST(v.eol_ts AS DATE))) >= 90 THEN 1 ELSE 0 END
         AS matured,
       CASE WHEN c.vin IS NOT NULL THEN 1 ELSE 0 END AS battery_claim_90d,
       p.charg AS supplier_batch, mb.lifnr AS supplier_id, mb.vendor_batch, mb.quality_status AS batch_status
FROM ${gold}.dim_vehicle v
CROSS JOIN (SELECT max(_business_date) AS as_of_date FROM ${gold}.fact_vehicle_daily) a
LEFT JOIN ${gold}.dim_date d ON d.`date` = CAST(v.eol_ts AS DATE)
LEFT JOIN (
  SELECT DISTINCT w.resolved_vin AS vin
  FROM ${gold}.fact_warranty_claim w
  JOIN ${gold}.dim_vehicle s ON s.vin = w.resolved_vin AND s.is_current
  WHERE w.battery_related
    AND datediff(w.failure_date, coalesce(s.delivery_date, CAST(s.eol_ts AS DATE))) BETWEEN 0 AND 90
) c ON c.vin = v.vin
LEFT JOIN ${semantic}.v_pack_supplier_batch p ON p.pack_serial = v.eol_pack_serial
LEFT JOIN ${gold}.dim_material_batch mb ON mb.charg = p.charg AND mb.is_current
WHERE v.is_current
