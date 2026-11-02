"""
Unstructured analytics pages for the vision workbench: images and EDR replay with structured context.
"""
from __future__ import annotations

from app.media import load_object, png_with_box


def lit(value) -> str:
    return "'" + str(value).replace("'", "") + "'"


def xray_index_sql(semantic: str, verdict: str | None, limit: int = 100) -> str:
    where = f" AND verdict = {lit(verdict)}" if verdict else ""
    return f"""
SELECT cell_id, verdict, score, defect_code, image_ref, overhang_box,
       pack_serial, vin, `decision`, pack_risk_score
FROM {semantic}.media_xray
WHERE TRUE{where}
ORDER BY CASE verdict WHEN 'FAIL' THEN 0 ELSE 1 END, score DESC
LIMIT {int(limit)}
""".strip()


def object_path_sql(bronze: str, source: str, file_name: str) -> str:
    return f"""
SELECT raw_path, source_path, doc_id, ingest_status
FROM {bronze}.doc_object
WHERE source = {lit(source)} AND file_name = {lit(file_name)}
  AND ingest_status IN ('VALID', 'DUPLICATE')
ORDER BY _business_date DESC
LIMIT 1
""".strip()


def film_sql(gold: str, silver: str, limit: int = 80) -> str:
    return f"""
SELECT q.cell_id, q.batch_id, q.verdict, q.score, q.image_ref,
       m.streak_cols, m.defect_patch
FROM {gold}.fact_qc_inspection q
JOIN {silver}.qc_image_metric m
  ON m.subject_id = q.cell_id AND m.image_kind = 'FILM' AND m.status = 'EXTRACTED'
WHERE q.inspection = 'FILM' AND q.image_ref IS NOT NULL AND q.image_ref <> ''
ORDER BY q.score DESC
LIMIT {int(limit)}
""".strip()


def edr_sql(gold: str, silver: str, limit: int = 40) -> str:
    return f"""
SELECT e.event_id, e.vin, CAST(e.trigger_ts AS STRING) AS trigger_ts, e.safety_floor,
       d.doc_id, d.file_name
FROM {gold}.fact_edr_event e
LEFT JOIN {silver}.edr_event d ON d.event_id = e.event_id
ORDER BY e.trigger_ts DESC
LIMIT {int(limit)}
""".strip()


def _show_frame(st, frame) -> None:
    try:
        st.dataframe(frame, use_container_width=True, hide_index=True)
    except ImportError:
        st.table(frame)


def _render_xray(q, gold: str, semantic: str, bronze: str, landing: str) -> None:
    import streamlit as st

    verdict = st.selectbox("Verdict filter", ["FAIL", "PASS", "all"])
    rows = q(xray_index_sql(semantic, None if verdict == "all" else verdict))
    st.caption(f"{len(rows)} X-ray images with structured QC rows and pack context.")
    _show_frame(st, rows)
    if rows.empty:
        return
    cell = st.selectbox("Cell", list(rows["cell_id"]))
    row = rows.loc[rows["cell_id"] == cell].iloc[0]
    file_name = str(row["image_ref"]).rsplit("/", 1)[-1]
    paths = q(object_path_sql(bronze, "qc_xray", file_name))
    raw = paths.iloc[0]["raw_path"] if not paths.empty else None
    data = load_object(str(row["image_ref"]), str(raw) if raw else None, landing)
    if data is None:
        st.warning("Could not load image bytes from landing/raw in this session. Metadata above is from Impala.")
        return
    img = png_with_box(data, row.get("overhang_box"))
    if img is not None:
        st.image(img, caption=f"{cell} — box from extract (white outline)", use_container_width=True)
    else:
        st.image(data, caption=cell, use_container_width=True)
    st.info(f"Pack {row.get('pack_serial')} / VIN {row.get('vin')} — champion {row.get('decision')} "
            f"(score {row.get('pack_risk_score')}). action is always NONE.")


def _render_film(q, gold: str, silver: str, landing: str) -> None:
    import streamlit as st

    rows = q(film_sql(gold, silver))
    st.caption("Electrode film PNGs (defect or sample) with streak and patch metrics from extract.")
    _show_frame(st, rows)
    if rows.empty:
        return
    ref = st.selectbox("Image", list(rows["image_ref"]))
    row = rows.loc[rows["image_ref"] == ref].iloc[0]
    data = load_object(str(ref), None, landing)
    if data is None:
        st.warning("Image bytes not reachable here; structured verdict and score are still valid.")
        return
    st.image(data, caption=f"{row['cell_id']} score={row['score']}", use_container_width=True)


def _render_edr(q, gold: str, silver: str, landing: str) -> None:
    import streamlit as st

    rows = q(edr_sql(gold, silver))
    st.caption("Thermal/crash EDR events. The binary dump is parsed in silver; replay is the buffer extremes.")
    _show_frame(st, rows)
    if rows.empty:
        return
    event = st.selectbox("Event", list(rows["event_id"]))
    st.write(rows.loc[rows["event_id"] == event].iloc[0].to_dict())


def pages(gold: str, semantic: str, bronze: str, landing: str, silver: str):
    return [
        ("Cell X-ray viewer", lambda q: _render_xray(q, gold, semantic, bronze, landing)),
        ("Film defect gallery", lambda q: _render_film(q, gold, silver, landing)),
        ("EDR event replay", lambda q: _render_edr(q, gold, silver, landing)),
    ]
