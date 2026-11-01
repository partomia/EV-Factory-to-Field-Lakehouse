"""
Supply sources (driver side, small): SAP S/4HANA-style OData v2 pages (full load on the first business date,
then daily delta pages with delta tokens), supplier certificates of analysis (PDF), XRF spectra per cathode lot
(JCAMP-DX) and 8D reports for the contaminated lots. Standard library only; all values are synthetic.

Planted: the A_MaterialDocument page of the previous day is re-sent on `faults.sap_delta_resend`; the field
ZZ_MOISTURE_PPM appears in A_InspectionLot from `faults.sap_schema_change` (schema evolution).
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

from generators.core import CATHODE, MATERIALS, Plan, d, rng, stable_int
from generators.docs import jcamp, text_pdf, xrf_spectrum

VENDORS = {
    "V-CATH-01": ("Synthetic Cathode Materials Co", "KR", "Ulsan"),
    "V-CATH-02": ("Synthetic Advanced Powders Ltd", "IN", "Chennai"),
    "V-ANOD-01": ("Synthetic Graphite Works", "IN", "Nagpur"),
    "V-ELYT-01": ("Synthetic Electrolyte Chem", "JP", "Osaka"),
    "V-SEP-01": ("Synthetic Separator Films", "CN", "Changzhou"),
    "V-CAN-01": ("Synthetic Precision Cans", "IN", "Pune"),
}
PLANT = "EV01"
CELL_MATNR = "MAT-CELL-4680"


def odata_date(day: date) -> str:
    ms = int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp() * 1000)
    return f"/Date({ms})/"


def _pages(entity: str, day: date, rows: list[dict], page_size: int, token: str) -> list[tuple[str, bytes]]:
    out = []
    n = max(1, (len(rows) + page_size - 1) // page_size)
    for i in range(n):
        chunk = rows[i * page_size:(i + 1) * page_size]
        body = {"d": {"results": chunk, "__count": str(len(rows))}}
        if i + 1 < n:
            body["d"]["__next"] = f"{entity}?$skiptoken={i + 1}"
        else:
            body["d"]["__delta"] = f"{entity}?!deltatoken='{token}'"
        out.append((f"sap_odata/{day}/{entity}_{i + 1:03d}.json", json.dumps(body, separators=(",", ":")).encode()))
    return out


def generate_supply(plan: Plan):
    cfg = plan.cfg
    faults = cfg.get("faults", {})
    page = cfg["volumes"]["sap_page_size"]
    resend = d(faults["sap_delta_resend"]) if faults.get("sap_delta_resend") else None
    schema_change = d(faults["sap_schema_change"]) if faults.get("sap_schema_change") else None
    start, end = plan.start, plan.end
    per_day: dict[date, dict[str, list[dict]]] = {}

    def add(day: date, entity: str, row: dict):
        per_day.setdefault(day, {}).setdefault(entity, []).append(row)

    for matnr, (desc, _, _) in list(MATERIALS.items()) + [(CELL_MATNR, ("Cell 4680 dry electrode", [], 0))]:
        add(start, "A_Material", {"MATNR": matnr, "MAKTX": desc, "MTART": "FERT" if matnr == CELL_MATNR else "ROH",
                                  "MEINS": "EA" if matnr in (CELL_MATNR, "MAT-CAN-4680") else "KG", "WERKS": PLANT})
    for lifnr, (name, land, city) in VENDORS.items():
        add(start, "A_Supplier", {"LIFNR": lifnr, "NAME1": name, "LAND1": land, "ORT01": city})

    for lot in sorted(plan.lots.values(), key=lambda x: (x.first_day, x.lot_id)):
        r = rng("lot", lot.lot_id)
        gr_day = lot.first_day - timedelta(days=2) if lot.first_day - timedelta(days=2) >= start else start
        vendor_batch = f"{lot.vendor[-2:]}{r.randint(10**6, 10**7 - 1)}"
        add(gr_day, "A_Batch", {"CHARG": lot.lot_id, "MATNR": lot.material, "LIFNR": lot.vendor, "WERKS": PLANT,
                                "HSDAT": odata_date(gr_day - timedelta(days=r.randint(5, 20))),
                                "VFDAT": odata_date(gr_day + timedelta(days=365)), "LICHA": vendor_batch})
        qty = round(r.uniform(800, 1400), 1)
        add(gr_day, "A_MaterialDocument", {"MBLNR": f"49{stable_int('gr', lot.lot_id) % 10**8:08d}", "MJAHR": str(gr_day.year),
                                           "ZEILE": "0001", "BWART": "101", "MATNR": lot.material, "CHARG": lot.lot_id,
                                           "LIFNR": lot.vendor, "MENGE": str(qty), "MEINS": "KG",
                                           "BUDAT": odata_date(gr_day), "AUFNR": ""})
        d50 = r.gauss(9.5, 0.3)
        fe = r.gauss(17.0, 2.0) if lot.contaminated else r.gauss(8.0, 1.5)
        moist = r.gauss(180, 25)
        insp = {"PRUEFLOS": f"89{lot.first_day:%y%m%d}{lot.lot_id[-5:]}", "CHARG": lot.lot_id, "MATNR": lot.material,
                "ART": "01", "ENSTEHDAT": odata_date(gr_day), "QM_D50_UM": f"{d50:.2f}",
                "QM_IMPURITY_FE_PPM": f"{fe:.1f}", "VCODE": "A"}
        if schema_change and gr_day >= schema_change:
            insp["ZZ_MOISTURE_PPM"] = f"{moist:.0f}"
        add(gr_day, "A_InspectionLot", insp)
        # certificate of analysis from the supplier
        yield ("file", f"supplier_coa/{gr_day}/{lot.lot_id}.pdf", text_pdf(
            "Certificate of analysis",
            [("Certificate no", f"COA-{vendor_batch}"), ("Supplier", lot.vendor), ("Supplier name", VENDORS[lot.vendor][0]),
             ("Material", lot.material), ("Vendor batch", vendor_batch), ("SAP batch", lot.lot_id),
             ("Date", gr_day.isoformat()), ("D50 um", f"{d50:.2f}"), ("Fe impurity ppm", f"{fe:.1f}"),
             ("Moisture ppm", f"{moist:.0f}"), ("Specification", "Fe <= 25 ppm; moisture <= 300 ppm; D50 8.5-10.5 um"),
             ("Result", "CONFORMS")],
            [("Statement", "Values measured on a composite sample of the batch by the supplier laboratory.")]))
        if lot.material == CATHODE:
            xs, ys = xrf_spectrum(r, lot.contaminated)
            yield ("file", f"spectra/{lot.first_day}/{lot.lot_id}.jdx",
                   jcamp(f"Incoming XRF {lot.lot_id}", lot.lot_id, xs, ys).encode())
            if lot.contaminated:
                day8d = lot.first_day + timedelta(days=75)
                if day8d <= end:
                    yield ("file", f"supplier_8d/{day8d}/8D-{lot.lot_id}.pdf", text_pdf(
                        "8D problem solving report",
                        [("Report no", f"8D-{lot.lot_id}"), ("Report date", day8d.isoformat()), ("Report type", "8D"),
                         ("Supplier", lot.vendor), ("SAP batch", lot.lot_id), ("Material", lot.material),
                         ("Root cause category", "CATHODE CONTAMINATION")],
                        [("D2 problem", "Field battery events on packs built from cells made with this cathode powder lot. "
                                        "Teardowns show metallic particles on the cathode surface."),
                         ("D4 root cause", "Wear debris from a classifier mill at the supplier entered the powder during "
                                           "one production campaign."),
                         ("D5 corrective action", "Magnetic separation added after milling; lot quarantined."),
                         ("D7 prevention", "Incoming XRF screen for Fe and Cu lines on every cathode lot.")]))

    for b in plan.batches:
        n = b.n_cells
        aufnr = f"10{b.day:%y%m%d}{b.line[-1]}{'ABC'.index(b.shift)}"
        add(b.day, "A_ProductionOrder", {"AUFNR": aufnr, "MATNR": CELL_MATNR, "WERKS": PLANT, "FEVOR": b.line,
                                         "GSTRP": odata_date(b.day), "GAMNG": str(n), "CHARG": b.batch_id})
        for i, (matnr, lot) in enumerate(((CATHODE, b.powder_lot), ("MAT-ANOD-GR", b.anode_lot),
                                          ("MAT-ELYT-LPF6", b.electrolyte_lot), ("MAT-SEP-PE12", b.separator_lot),
                                          ("MAT-CAN-4680", b.can_lot))):
            qty = n if matnr == "MAT-CAN-4680" else round(n * {CATHODE: 0.062, "MAT-ANOD-GR": 0.045,
                                                                "MAT-ELYT-LPF6": 0.098, "MAT-SEP-PE12": 0.004}[matnr], 2)
            add(b.day, "A_MaterialDocument", {"MBLNR": f"4{aufnr}{i}", "MJAHR": str(b.day.year), "ZEILE": f"{i + 1:04d}",
                                              "BWART": "261", "MATNR": matnr, "CHARG": lot, "LIFNR": "",
                                              "MENGE": str(qty), "MEINS": "EA" if matnr == "MAT-CAN-4680" else "KG",
                                              "BUDAT": odata_date(b.day), "AUFNR": aufnr})

    prev_doc_pages = None
    for day in plan.business_dates():
        ents = per_day.get(day, {})
        for entity in ("A_Material", "A_Supplier", "A_Batch", "A_InspectionLot", "A_ProductionOrder", "A_MaterialDocument"):
            rows = ents.get(entity, [])
            if not rows and entity in ("A_Material", "A_Supplier"):
                continue
            pages = _pages(entity, day, rows, page, f"D{day:%Y%m%d}{entity[2:6].upper()}")
            for p, data in pages:
                yield ("file", p, data)
            if entity == "A_MaterialDocument":
                if resend and day == resend and prev_doc_pages:
                    p, data = prev_doc_pages[-1]
                    yield ("file", f"sap_odata/{day}/A_MaterialDocument_resent_{p.rsplit('_', 1)[1]}", data)
                prev_doc_pages = pages
