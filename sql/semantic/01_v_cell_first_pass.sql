-- One row per graded cell. First pass: not graded REJECT and no X-ray FAIL (cells are never reworked, so the
-- first pass is the only pass). Cells whose mixing step is not loaded (production day unknown) are left out.
SELECT g.cell_id, g.batch_id, g.line, g.shift, g.production_date, g.formation_station, g.grading_station,
       g.grade, x.verdict AS xray_verdict,
       CASE WHEN g.grade <> 'REJECT' AND coalesce(x.verdict, 'PASS') = 'PASS' THEN 1 ELSE 0 END AS first_pass
FROM ${gold}.fact_cell_grading g
LEFT JOIN ${gold}.fact_qc_inspection x ON x.cell_id = g.cell_id AND x.inspection = 'XRAY'
WHERE g.production_date IS NOT NULL
