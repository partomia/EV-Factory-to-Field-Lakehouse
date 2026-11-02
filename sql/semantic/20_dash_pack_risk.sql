-- One row per scored vehicle on the newest score date. The workbench and the Traceability dashboard both read this.
SELECT vin, pack_serial, score, `decision`, reason, priority, `action` AS model_action,
       safety_floor, genealogy_open, CASE WHEN ood THEN 1 ELSE 0 END AS ood_flag
FROM ${gold}.uc01_pack_risk_score
WHERE _business_date = (SELECT max(_business_date) FROM ${gold}.uc01_pack_risk_score)
