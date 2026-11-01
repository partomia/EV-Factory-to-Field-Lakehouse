-- The supplier batch of a pack: the cathode powder lot (SAP batch) that most of its cells were made from.
SELECT pack_serial, powder_lot AS charg, n_cells
FROM (
  SELECT pack_serial, powder_lot, n_cells,
         row_number() OVER (PARTITION BY pack_serial ORDER BY n_cells DESC, powder_lot) AS rn
  FROM (SELECT pack_serial, powder_lot, count(*) AS n_cells
        FROM ${gold}.bridge_genealogy WHERE powder_lot IS NOT NULL
        GROUP BY pack_serial, powder_lot) c
) r
WHERE rn = 1
