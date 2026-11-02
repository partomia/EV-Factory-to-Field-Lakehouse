"""
uc01 pages for the Quality & Reliability Workbench.

The app discovers PAGES. Every figure comes from Impala (gold and semantic), the same tables
the dashboards read. action is always NONE: the worklist is a priority, not a command.
"""
from __future__ import annotations


def lit(value) -> str:
    """A SQL string literal. Quotes are removed so a VIN or batch cannot close the literal."""
    return "'" + str(value).replace("'", "") + "'"


def _show_frame(st, frame) -> None:
    """st.dataframe needs a pyarrow build that matches the runtime numpy; st.table always works."""
    try:
        st.dataframe(frame, use_container_width=True, hide_index=True)
    except ImportError:
        st.table(frame)


def latest_scores(gold: str) -> str:
    """The newest score date. Impala rejects a scalar subquery in a JOIN's ON clause."""
    return (f"(SELECT * FROM {gold}.uc01_pack_risk_score "
            f"WHERE _business_date = (SELECT max(_business_date) FROM {gold}.uc01_pack_risk_score))")


def worklist_sql(gold: str, semantic: str, order: str, decision: str | None, limit: int = 200) -> str:
    """Risk order puts the safety floor first, then the worklist by score. Claim order puts a
    90-day battery claim first. The two orders are the comparison; neither one changes a decision."""
    where = ""
    if decision:
        where = f" AND s.`decision` = {lit(decision)}"
    if order == "claim":
        ranking = "coalesce(f.battery_claim_90d, 0) DESC, s.`score` DESC"
    else:
        ranking = ("CASE s.`decision` WHEN 'SAFETY_FLOOR' THEN 0 WHEN 'WORKLIST' THEN 1 "
                   "WHEN 'ABSTAIN' THEN 2 ELSE 3 END, s.`score` DESC")
    return f"""
SELECT s.vin, s.pack_serial, s.`score`, s.`decision`, s.reason, s.priority, s.`action`,
       s.safety_floor, s.genealogy_open, s.ood, coalesce(f.battery_claim_90d, 0) AS battery_claim_90d
FROM {latest_scores(gold)} s
LEFT JOIN {semantic}.v_vehicle_fir90 f ON f.vin = s.vin
WHERE TRUE{where}
ORDER BY {ranking}
LIMIT {int(limit)}
""".strip()


def batches_sql(gold: str, semantic: str, limit: int = 40) -> str:
    """One row per supplier powder lot: how many vehicles sit on the worklist or the safety floor."""
    return f"""
SELECT b.charg AS supplier_batch, mb.quality_status AS batch_status, mb.lifnr AS supplier_id,
       count(DISTINCT g.cell_id) AS cells, count(DISTINCT g.pack_serial) AS packs,
       count(DISTINCT v.vin) AS vehicles,
       count(DISTINCT CASE WHEN s.`decision` = 'WORKLIST' THEN v.vin END) AS worklist_vehicles,
       count(DISTINCT CASE WHEN s.`decision` = 'SAFETY_FLOOR' THEN v.vin END) AS safety_floor_vehicles
FROM {gold}.bridge_genealogy g
JOIN {semantic}.v_pack_supplier_batch b ON b.pack_serial = g.pack_serial
LEFT JOIN {gold}.dim_material_batch mb ON mb.charg = b.charg AND mb.is_current
LEFT JOIN {gold}.dim_vehicle v ON v.eol_pack_serial = g.pack_serial AND v.is_current
LEFT JOIN {latest_scores(gold)} s ON s.vin = v.vin
GROUP BY b.charg, mb.quality_status, mb.lifnr
ORDER BY safety_floor_vehicles DESC, worklist_vehicles DESC, vehicles DESC
LIMIT {int(limit)}
""".strip()


def explain_batch(row: dict) -> str:
    return (f"Powder lot {row['supplier_batch']} ({row.get('batch_status') or 'status unknown'}): "
            f"{int(row['cells'])} cells, {int(row['packs'])} packs, {int(row['vehicles'])} vehicles. "
            f"{int(row['worklist_vehicles'])} are on the worklist and "
            f"{int(row['safety_floor_vehicles'])} are safety-floor P1 from a thermal event. "
            "Nothing here is an automated action.")


def batch_vehicles_sql(gold: str, semantic: str, batch: str) -> str:
    return f"""
SELECT v.vin, v.eol_pack_serial AS pack_serial, s.`score`, s.`decision`, s.reason, s.priority
FROM {gold}.dim_vehicle v
JOIN {semantic}.v_pack_supplier_batch b ON b.pack_serial = v.eol_pack_serial
LEFT JOIN {latest_scores(gold)} s ON s.vin = v.vin
WHERE v.is_current AND b.charg = {lit(batch)}
ORDER BY CASE s.`decision` WHEN 'SAFETY_FLOOR' THEN 0 WHEN 'WORKLIST' THEN 1 ELSE 2 END, s.`score` DESC
LIMIT 200
""".strip()


def vehicle_sql(gold: str, vin: str) -> str:
    return f"""
SELECT v.vin, v.model, v.vehicle_line, v.eol_pack_serial AS pack_serial,
       CAST(v.eol_ts AS STRING) AS eol_ts, CAST(v.delivery_date AS STRING) AS delivery_date,
       s.`score`, s.`decision`, s.reason, s.priority, s.`action`, s.safety_floor, s.genealogy_open, s.ood
FROM {gold}.dim_vehicle v
LEFT JOIN {latest_scores(gold)} s ON s.vin = v.vin
WHERE v.is_current AND v.vin = {lit(vin)}
""".strip()


def genealogy_sql(gold: str, pack: str) -> str:
    return f"""
SELECT link_status, count(*) AS cells
FROM {gold}.bridge_genealogy
WHERE pack_serial = {lit(pack)}
GROUP BY link_status
ORDER BY link_status
""".strip()


def grading_sql(gold: str, pack: str) -> str:
    return f"""
SELECT count(*) AS cells,
       round(min(g.capacity_ah), 3) AS cap_min, round(avg(g.capacity_ah), 3) AS cap_mean,
       round(max(g.capacity_ah), 3) AS cap_max,
       round(min(g.ir_mohm), 2) AS ir_min, round(avg(g.ir_mohm), 2) AS ir_mean, round(max(g.ir_mohm), 2) AS ir_max
FROM {gold}.fact_cell_grading g
JOIN {gold}.bridge_genealogy b ON b.cell_id = g.cell_id
WHERE b.pack_serial = {lit(pack)}
""".strip()


def xray_sql(gold: str, pack: str) -> str:
    """The extracted defect box, not the image file. The image stays in the landing zone."""
    return f"""
SELECT q.verdict, q.defect_code, q.overhang_box, count(*) AS cells
FROM {gold}.fact_qc_inspection q
JOIN {gold}.bridge_genealogy b ON b.cell_id = q.cell_id
WHERE b.pack_serial = {lit(pack)} AND q.inspection = 'XRAY'
GROUP BY q.verdict, q.defect_code, q.overhang_box
ORDER BY cells DESC
LIMIT 15
""".strip()


def claims_sql(gold: str, vin: str) -> str:
    return f"""
SELECT claim_id, CAST(failure_date AS STRING) AS failure_date, category, complaint_code,
       battery_related, amount_inr
FROM {gold}.fact_warranty_claim
WHERE resolved_vin = {lit(vin)}
ORDER BY failure_date
LIMIT 20
""".strip()


def edr_sql(gold: str, vin: str) -> str:
    return f"""
SELECT event_id, CAST(trigger_ts AS STRING) AS trigger_ts, safety_floor
FROM {gold}.fact_edr_event
WHERE vin = {lit(vin)}
ORDER BY trigger_ts DESC
LIMIT 15
""".strip()


def telemetry_sql(gold: str, vin: str) -> str:
    return f"""
SELECT CAST(summary_date AS STRING) AS summary_date, dist_km, soc_min, soc_max, cell_t_max_c, fast_chg_sessions
FROM {gold}.fact_vehicle_daily
WHERE vin = {lit(vin)}
ORDER BY summary_date
LIMIT 90
""".strip()


def _render_worklist(q, gold: str, semantic: str) -> None:
    import streamlit as st

    order = st.radio("Order", ["risk", "claim"], horizontal=True,
                     format_func=lambda o: "Risk order (safety floor, then score)" if o == "risk"
                     else "Claim order (90-day battery claim, then score)")
    choice = st.selectbox("Decision", ["WORKLIST", "SAFETY_FLOOR", "ABSTAIN", "CLEAR", "all"])
    frame = q(worklist_sql(gold, semantic, order, None if choice == "all" else choice))
    st.caption(f"{len(frame)} vehicles. action is NONE on every row.")
    _show_frame(st, frame)


def _render_containment(q, gold: str, semantic: str) -> None:
    import streamlit as st

    batches = q(batches_sql(gold, semantic))
    _show_frame(st, batches)
    if batches.empty:
        return
    batch = st.selectbox("Powder lot", list(batches["supplier_batch"]))
    row = batches.loc[batches["supplier_batch"] == batch].iloc[0].to_dict()
    st.info(explain_batch(row))
    _show_frame(st, q(batch_vehicles_sql(gold, semantic, batch)))


def _render_pack(q, gold: str, semantic: str) -> None:
    import streamlit as st

    sample = q(worklist_sql(gold, semantic, "risk", "SAFETY_FLOOR", limit=1))
    default = "" if sample.empty else str(sample.iloc[0]["vin"])
    vin = st.text_input("VIN", value=default).strip()
    if not vin:
        return
    vehicle = q(vehicle_sql(gold, vin))
    if vehicle.empty:
        st.warning("No current vehicle with that VIN.")
        return
    _show_frame(st, vehicle)
    pack = str(vehicle.iloc[0]["pack_serial"])
    left, right = st.columns(2)
    left.subheader("Genealogy")
    _show_frame(left, q(genealogy_sql(gold, pack)))
    right.subheader("Cell grading spread")
    _show_frame(right, q(grading_sql(gold, pack)))
    st.subheader("X-ray verdicts and the extracted defect box")
    st.caption("The box is the field extracted from the image. The PNG stays in the landing zone.")
    _show_frame(st, q(xray_sql(gold, pack)))
    st.subheader("Warranty claims")
    _show_frame(st, q(claims_sql(gold, vin)))
    st.subheader("EDR events")
    _show_frame(st, q(edr_sql(gold, vin)))
    trend = q(telemetry_sql(gold, vin))
    st.subheader("Daily telemetry")
    if not trend.empty:
        st.line_chart(trend, x="summary_date", y=["cell_t_max_c", "soc_max"])
    _show_frame(st, trend)


def pages(gold: str, semantic: str):
    return [
        ("Vehicle risk worklist", lambda q: _render_worklist(q, gold, semantic)),
        ("Batch containment", lambda q: _render_containment(q, gold, semantic)),
        ("Pack 360", lambda q: _render_pack(q, gold, semantic)),
    ]
