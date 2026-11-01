-- Certified KPI: cell first-pass yield by production day, line, shift and formation station.
SELECT production_date, line, shift, formation_station,
       count(*) AS cells_graded,
       sum(first_pass) AS cells_first_pass,
       round(sum(first_pass) / count(*), 4) AS fpy,
       sum(CASE WHEN xray_verdict IS NULL THEN 1 ELSE 0 END) AS cells_without_xray
FROM ${semantic}.v_cell_first_pass
GROUP BY production_date, line, shift, formation_station
