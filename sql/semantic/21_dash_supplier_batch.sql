-- Powder lot to cells, packs and vehicles, with how many of those vehicles the champion put on the
-- worklist or the safety floor. Same grain as the workbench's Batch containment page.
SELECT b.charg AS supplier_batch, mb.quality_status AS batch_status, mb.lifnr AS supplier_id,
       count(DISTINCT g.cell_id) AS cells,
       count(DISTINCT g.pack_serial) AS packs,
       count(DISTINCT v.vin) AS vehicles,
       count(DISTINCT CASE WHEN s.`decision` = 'WORKLIST' THEN v.vin END) AS worklist_vehicles,
       count(DISTINCT CASE WHEN s.`decision` = 'SAFETY_FLOOR' THEN v.vin END) AS safety_floor_vehicles
FROM ${gold}.bridge_genealogy g
JOIN ${semantic}.v_pack_supplier_batch b ON b.pack_serial = g.pack_serial
LEFT JOIN ${gold}.dim_material_batch mb ON mb.charg = b.charg AND mb.is_current
LEFT JOIN ${gold}.dim_vehicle v ON v.eol_pack_serial = g.pack_serial AND v.is_current
LEFT JOIN ${semantic}.dash_pack_risk s ON s.vin = v.vin
GROUP BY b.charg, mb.quality_status, mb.lifnr
