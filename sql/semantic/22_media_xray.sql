-- X-ray images with gold QC fields (extract joined at gold build) and pack risk context.
SELECT q.cell_id, q.verdict, q.score, q.defect_code, q.overhang_box, q.image_ref,
       g.pack_serial, v.vin, s.`decision`, s.score AS pack_risk_score
FROM ${gold}.fact_qc_inspection q
LEFT JOIN ${gold}.bridge_genealogy g ON g.cell_id = q.cell_id
LEFT JOIN ${gold}.dim_vehicle v ON v.eol_pack_serial = g.pack_serial AND v.is_current
LEFT JOIN ${semantic}.dash_pack_risk s ON s.vin = v.vin
WHERE q.inspection = 'XRAY' AND q.image_ref IS NOT NULL AND q.image_ref <> ''
