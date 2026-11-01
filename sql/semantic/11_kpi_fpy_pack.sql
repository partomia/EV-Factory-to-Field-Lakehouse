-- Certified KPI: pack first-pass yield (EOL test) by production day, pack line, shift and wire-bond station.
SELECT production_date, pack_line, shift, wb_station,
       count(*) AS packs_tested,
       sum(CASE WHEN eol_result = 'PASS' THEN 1 ELSE 0 END) AS packs_first_pass,
       round(sum(CASE WHEN eol_result = 'PASS' THEN 1 ELSE 0 END) / count(*), 4) AS fpy,
       sum(CASE WHEN genealogy_status <> 'COMPLETE' THEN 1 ELSE 0 END) AS packs_genealogy_open
FROM ${gold}.fact_pack_eol
GROUP BY production_date, pack_line, shift, wb_station
