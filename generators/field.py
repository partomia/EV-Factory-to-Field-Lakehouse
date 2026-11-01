"""
Field and service generator for the vehicles of one partition (standard library only).

For each delivered vehicle: rider (PII, synthetic), usage profile, the latent battery-event risk from the pack's
factory DNA plus usage (planted causes, logistic model in config `label`), then what the source systems record:
legacy telematics daily summaries (through the history end), DTC history, EDR dumps (late), service job cards
(PDF), warranty claims, teardown reports (PDF). Live-date telemetry and DTCs come from the stream producer.

Records: ("row", table, tuple) for the tabular legacy exports written by Spark per date, ("file", relpath, bytes),
("truth_pack", dict), ("fleet", dict) for the stream producer. All values are synthetic.
"""
from __future__ import annotations

import math
from datetime import date, datetime, timedelta

from generators.core import IST, Plan, d, logistic, rng, stable_int
from generators.docs import edr_dump, scanned_pdf, text_pdf

FIRST = ["Aarav", "Vihaan", "Ishaan", "Ananya", "Diya", "Kavya", "Rohan", "Arjun", "Meera", "Sana", "Kabir",
         "Nisha", "Rahul", "Priya", "Aditya", "Tara", "Neel", "Riya", "Dev", "Zara"]
LAST = ["Sharma", "Iyer", "Reddy", "Nair", "Patel", "Gupta", "Rao", "Menon", "Joshi", "Khan", "Das", "Bose",
        "Kulkarni", "Pillai", "Verma", "Singh", "Mehta", "Chopra", "Shetty", "Naidu"]
STREETS = ["MG Road", "Ring Road", "Station Road", "Lake View", "Park Street", "Temple Road", "Main Bazaar"]
CITY_LATLON = {"Bengaluru": (12.97, 77.59), "Pune": (18.52, 73.86), "Chennai": (13.08, 80.27),
               "Hyderabad": (17.39, 78.49), "Delhi": (28.61, 77.21), "Mumbai": (19.08, 72.88),
               "Ahmedabad": (23.02, 72.57)}
RTO = {"Bengaluru": "KA01", "Pune": "MH12", "Chennai": "TN09", "Hyderabad": "TS09", "Delhi": "DL03",
       "Mumbai": "MH02", "Ahmedabad": "GJ01"}
BATTERY_DTC = {"thermal": ("BMS-T01", "Cell over-temperature"), "imbalance": ("BMS-I02", "Cell voltage imbalance")}
OTHER_DTC = [("MCU-T10", "Motor controller over-temperature"), ("MOT-E21", "Motor encoder fault"),
             ("CHG-C05", "Charger communication timeout"), ("ABS-W30", "ABS wheel-speed sensor")]
ROOT_CAUSE_TEXT = {
    "contaminated": ("CATHODE CONTAMINATION",
                     "Metallic particles found on the cathode surface of the examined cells; elevated self-discharge "
                     "and local heating at the particle sites. Cells trace to one cathode powder supplier lot."),
    "drift": ("CAPACITY MISMATCH",
              "Capacity mismatch within parallel groups; electrode film thickness outside the normal band in the "
              "affected cells. Imbalance grew with cycling."),
    "overhang": ("ELECTRODE OVERHANG",
                 "Anode-cathode overhang at the jelly-roll edge close to the X-ray limit; local lithium plating "
                 "observed at the edge."),
    "weak_bond": ("WEAK WIRE BOND",
                  "Low pull strength and partial lift-off of wire bonds at several cell tabs; intermittent cell "
                  "disconnection caused imbalance faults."),
    "usage": ("USAGE / NO DEFECT FOUND",
              "No manufacturing defect found. Pattern consistent with frequent fast charging in high ambient "
              "temperature."),
}

TELEMATICS_COLS = ("VIN", "SUMMARY_DATE", "ODO_KM", "DIST_KM", "RIDE_MIN", "SOC_MIN", "SOC_MAX", "PACK_V_AVG",
                   "CELL_T_MAX_C", "PACK_T_AVG_C", "CELL_DV_MAX_MV", "CHG_SESSIONS", "FAST_CHG_SESSIONS",
                   "AGGR_MIN", "GEOHASH5", "FW_BMS", "READINGS")
DTC_COLS = ("EVENT_ID", "VIN", "EVENT_TS", "DTC_CODE", "DTC_DESC", "SEVERITY", "ODO_KM", "FW_BMS", "SOURCE")
CLAIM_COLS = ("CLAIM_ID", "VIN", "CLAIM_DATE", "FAILURE_DATE", "ODO_KM", "CATEGORY", "COMPLAINT_CODE",
              "PART_NUMBER", "PART_DESC", "LABOUR_HOURS", "AMOUNT_INR", "STATUS", "DEALER_CODE")
RIDER_COLS = ("RIDER_ID", "FULL_NAME", "PHONE", "EMAIL", "ADDRESS_LINE", "CITY", "PINCODE", "HOME_LAT",
              "HOME_LON", "GEOHASH5", "VIN", "REGISTRATION_NO", "DELIVERY_DATE", "DEALER_CODE", "CONSENT_MARKETING")


def _swap2(vin: str) -> str:
    """The planted VIN typo: two adjacent serial characters transposed."""
    s = list(vin)
    s[-3], s[-2] = s[-2], s[-3]
    if s == list(vin):
        s[-2] = "8" if s[-2] != "8" else "3"
    return "".join(s)


def latent(plan: Plan, pack: dict, profile: dict) -> tuple[float, dict]:
    e = plan.cfg["label"]["effects"]
    contrib = {
        "contaminated": e["contaminated"] * (1.0 if pack["contaminated"] else 0.0),
        "drift": e["drift_spread"] * (1.0 if pack["drift"] > 0 else 0.0),
        "overhang": e["subthreshold_overhang_per_cell"] * pack["n_subthreshold"],
        "weak_bond": e["weak_bond"] * (1.0 if pack["weak_bond"] else 0.0),
        "usage": (e["fast_charge_share"] * profile["fast_charge_share"] + e["hot_city"] * profile["hot"]
                  + e["aggressive"] * profile["aggressive"]),
        "spread": (e["capacity_spread_z"] * (pack["cap_spread"] - 0.55) / 0.12
                   + e["k_tail_z"] * (pack["k_p95"] - 0.07) / 0.015),
    }
    x = plan.cfg["label"]["logit_base"] + sum(contrib.values())
    return logistic(x), contrib


def generate_vehicles(plan: Plan, packs: list[dict]):
    cfg = plan.cfg
    u, v, lab = cfg["universe"], cfg["volumes"], cfg["label"]
    faults = cfg.get("faults", {})
    end, hist_end = plan.end, plan.history_end
    cities = u["cities"]
    live_lo, live_hi = (d(x) for x in u["live_subset_delivered"])
    live_p = cfg.get("_live_subset_p", 0.33)
    vin_typo_dates = {d(x) for x in faults.get("vin_typo", [])}
    image_only_dates = {d(x) for x in faults.get("image_only_job_card", [])}
    swap = faults.get("pack_swap")
    late_edr = faults.get("edr_late", {})
    typo_done, image_only_done = set(), set()
    swap_pair = [p for p in packs if swap and p.get("vin") and p["pack_line"] == swap["line"]
                 and p["assembly_date"] == d(swap["packs_assembled"])][:2]
    swap_vins = {p["vin"] for p in swap_pair}

    for pack in packs:
        vin = pack.get("vin")
        if not vin:
            continue
        r = rng("field", vin)
        dispatch = pack["vehicle_eol_date"] + timedelta(days=r.randint(*u["dispatch_days"]))
        delivery = dispatch + timedelta(days=r.randint(*u["delivery_days"]))
        if delivery > end:
            yield ("truth_pack", _truth(pack, None, delivery, None))
            continue
        # rider and profile
        x = r.random()
        acc = 0.0
        city = cities[-1]
        for c in cities:
            acc += c["share"]
            if x <= acc:
                city = c
                break
        lat0, lon0 = CITY_LATLON[city["city"]]
        profile = {
            "city": city["city"], "geohash5": city["geohash5"], "hot": 1.0 if city["hot"] else 0.0,
            "fast_charge_share": r.betavariate(cfg["causes"]["fast_charge_share"]["alpha"],
                                               cfg["causes"]["fast_charge_share"]["beta"]),
            "aggressive": 1.0 if r.random() < cfg["causes"]["aggressive_share"] else 0.0,
            "daily_km": max(5.0, r.gauss(28, 8)),
        }
        rider_id = f"RD{stable_int('rider', vin) % 10**9:09d}"
        reg = f"{RTO[city['city']]}EV{stable_int('reg', vin) % 10**4:04d}"
        dealer = f"DL-{city['city'][:3].upper()}-{stable_int('dealer', vin) % 7 + 1:02d}"
        dms_vin = vin
        if delivery in vin_typo_dates and delivery not in typo_done and pack["pack_line"] == "PL1":
            dms_vin = _swap2(vin)
            typo_done.add(delivery)
        name = f"{r.choice(FIRST)} {r.choice(LAST)}"
        yield ("row", "dms_riders", (
            rider_id, name, f"+91-9{stable_int('ph', vin) % 10**9:09d}",
            f"{name.split()[0].lower()}.{stable_int('em', vin) % 10**4}@example.com",
            f"{r.randint(1, 450)} {r.choice(STREETS)}", city["city"], f"{560000 + r.randint(1, 99):06d}",
            round(lat0 + r.uniform(-0.08, 0.08), 6), round(lon0 + r.uniform(-0.08, 0.08), 6), city["geohash5"],
            dms_vin, reg, delivery.isoformat(), dealer, "Y" if r.random() < 0.4 else "N"))

        p90, contrib = latent(plan, pack, profile)
        positive = r.random() < p90
        event = None
        if positive:
            t = r.randint(1, 14) if r.random() < lab["early_event_share"] else 15 + int(75 * r.betavariate(1.3, 1.0))
            t = min(max(t, 1), 90)
            cause = max(("contaminated", "drift", "overhang", "weak_bond", "usage"), key=lambda k: contrib[k])
            p_thermal = {"contaminated": 0.7, "drift": 0.15, "overhang": 0.8, "weak_bond": 0.15, "usage": 0.5}[cause]
            kind = "thermal" if r.random() < p_thermal else "imbalance"
            event = {"day": t, "date": delivery + timedelta(days=t), "cause": cause, "kind": kind,
                     "edr": kind == "thermal" and r.random() < 0.5, "claim": r.random() < v["battery_claim_share_of_events"]}
        live = live_lo <= delivery <= live_hi and (stable_int("live", vin) % 10000) / 10000 < live_p
        yield ("truth_pack", _truth(pack, profile, delivery, event, p90=p90, contrib=contrib, live=live))
        yield ("fleet", {"vin": vin, "pack_id": pack["pack_id"], "delivery_date": delivery.isoformat(),
                         "city": profile["city"], "geohash5": profile["geohash5"],
                         "fast_charge_share": round(profile["fast_charge_share"], 4),
                         "aggressive": bool(profile["aggressive"]), "daily_km": round(profile["daily_km"], 2),
                         "v_spread_mv": round(pack["v_spread_mv"], 3), "contaminated": pack["contaminated"],
                         "event_date": event["date"].isoformat() if event else None,
                         "event_kind": event["kind"] if event else None, "live_subset": live,
                         "bms_fw": pack["bms_fw"]})

        # legacy telematics daily summaries through the history end
        odo = 0.0
        base_dv = pack["v_spread_mv"] * 0.8
        for k in range((min(hist_end, end) - delivery).days + 1):
            day = delivery + timedelta(days=k)
            rd = rng("tday", vin, k)
            dist = max(0.0, rd.gauss(profile["daily_km"], 6)) if rd.random() > 0.12 else 0.0
            odo += dist
            prec = 0.0
            if event and event["date"] - timedelta(days=25) <= day:
                prec = min(1.0, max(0.0, 1 - (event["date"] - day).days / 25))
            dv = base_dv + rd.gauss(0, 1.5) + (25 * prec if event and event["kind"] == "imbalance" else 6 * prec)
            t_max = (31 if profile["hot"] else 27) + rd.gauss(0, 2) + 8 * profile["fast_charge_share"] \
                + (6 * prec if event and event["kind"] == "thermal" else 1.5 * prec) \
                + (1.2 if pack["contaminated"] else 0.0)
            chg = 1 if dist > 0 and rd.random() < 0.6 else 0
            fast = 1 if chg and rd.random() < profile["fast_charge_share"] else 0
            ride_min = int(dist / 22 * 60)
            yield ("row", "telematics_daily", (
                vin, day.isoformat(), round(odo, 1), round(dist, 1), ride_min, round(max(5, rd.gauss(35, 12)), 1),
                round(min(100, rd.gauss(92, 5)), 1), round(rd.gauss(47.2, 0.6), 2), round(t_max, 1),
                round(t_max - rd.uniform(3, 6), 1), round(max(1.0, dv), 2), chg, fast,
                int(ride_min * (0.4 if profile["aggressive"] else 0.08)), profile["geohash5"], pack["bms_fw"],
                ride_min * 2))

        # DTC history (through the history end; live dates come from the stream)
        dtcs = []
        if event:
            code, desc = BATTERY_DTC[event["kind"]]
            for j in range(1 + r.randint(0, 3)):
                when = datetime.combine(event["date"] + timedelta(days=2 * j), datetime.min.time(), IST) \
                    + timedelta(hours=r.uniform(7, 21))
                dtcs.append((when, code, desc, "HIGH" if event["kind"] == "thermal" else "MEDIUM"))
        months = max(0.0, ((min(hist_end, end) - delivery).days + 1) / 30.0)
        for _ in range(_poisson(r, v["non_battery_dtc_per_vehicle_month"] * months)):
            when = datetime.combine(delivery, datetime.min.time(), IST) + timedelta(
                days=r.uniform(0, max(1.0, (min(hist_end, end) - delivery).days)), hours=r.uniform(7, 21))
            code, desc = r.choice(OTHER_DTC)
            dtcs.append((when, code, desc, "LOW"))
        for j, (when, code, desc, sev) in enumerate(sorted(dtcs)):
            if when.date() > hist_end:
                continue
            yield ("row", "dtc_history", (f"DTC-{vin[-9:]}-{j:03d}", vin, when.isoformat(timespec="seconds"),
                                          code, desc, sev, round(profile["daily_km"] * (when.date() - delivery).days, 1),
                                          pack["bms_fw"], "LEGACY"))

        # EDR dumps: thermal trigger for the event, plus non-battery fault / crash triggers; uploaded late
        edrs = []
        if event and event["edr"]:
            edrs.append(("thermal", event["date"]))
        if r.random() < v["non_battery_edr_rate_90d"] * max(1.0, (end - delivery).days / 90):
            edrs.append((r.choice(("fault", "crash")), delivery + timedelta(days=r.randint(1, max(1, (end - delivery).days)))))
        for j, (trig, day) in enumerate(edrs):
            if day > end:
                continue
            t_ev = datetime.combine(day, datetime.min.time(), IST) + timedelta(hours=r.uniform(7, 21))
            delay = timedelta(hours=r.uniform(*v["edr_delay_hours"]))
            if late_edr and day == d(late_edr["event_date"]):
                arrival = datetime.combine(d(late_edr["arrival_date"]), datetime.min.time(), IST) + timedelta(hours=9)
            else:
                arrival = t_ev + delay
            if arrival.date() > end:
                continue
            eid = f"EDR{vin[-9:]}{j:02d}"
            yield ("file", f"edr/{arrival.date()}/{vin}_{eid}.edr",
                   edr_dump(vin, eid, trig, int(t_ev.timestamp() * 1000), _edr_samples(r, trig, pack, profile)))

        # service: periodic jobs + the battery inspection after the event
        jobs = []
        k = 1
        while True:
            day = delivery + timedelta(days=v["service_interval_days"] * k + r.randint(-v["service_jitter_days"], v["service_jitter_days"]))
            if day > end:
                break
            jobs.append((day, "PERIODIC", "Scheduled service", "Routine checks completed. No fault found.", ""))
            k += 1
        if event:
            day = event["date"] + timedelta(days=r.randint(1, 8))
            code = BATTERY_DTC[event["kind"]][0]
            jobs.append((day, "BATTERY INSPECTION", f"Battery warning {code} on dashboard",
                         f"BMS log shows {code}. Pack inspected; cell voltage spread measured. Pack quarantined pending warranty.",
                         "PACK INSPECTION KIT"))
        if vin in swap_vins and swap:
            other = [p for p in swap_pair if p["vin"] != vin][0]
            jobs.append((d(swap["date"]), "PACK REPLACEMENT", "Pack swap during workshop rework",
                         f"Removed pack {pack['pack_id']}. Installed pack {other['pack_id']}.", f"PACK {other['pack_id']}"))
        for j, (day, stype, complaint, diag, parts) in enumerate(sorted(jobs)):
            if day > end:
                continue
            job_id = f"JC{vin[-9:]}{j:02d}"
            fields = [("Job card no", job_id), ("Date", day.isoformat()), ("VIN", dms_vin), ("Registration", reg),
                      ("Odometer km", f"{profile['daily_km'] * (day - delivery).days:.0f}"), ("Service type", stype),
                      ("Service centre", dealer), ("Technician", f"T-{stable_int('tech', job_id) % 90 + 10}"),
                      ("Parts replaced", parts or "NONE")]
            sections = [("Complaint", complaint), ("Diagnosis", diag)]
            image_only = day in image_only_dates and day not in image_only_done and pack["pack_line"] == "PL1"
            if image_only:
                image_only_done.add(day)
                data = scanned_pdf("Service job card", fields, sections, seed=stable_int(job_id) % 10**6)
            else:
                data = text_pdf("Service job card", fields, sections)
            yield ("file", f"dms_jobcards/{day}/{job_id}.pdf", data)

        # warranty claims
        claims = []
        if event and event["claim"]:
            claims.append((event["date"] + timedelta(days=r.randint(3, 20)), event["date"], "BATTERY",
                           BATTERY_DTC[event["kind"]][0], "PN-PACK-47", "Battery pack assembly", 4.0, 98000))
        if r.random() < v["non_battery_claim_rate_90d"] * max(1.0, (end - delivery).days / 90):
            f_day = delivery + timedelta(days=r.randint(1, max(1, (end - delivery).days)))
            part = r.choice([("MOTOR", "MOT-E21", "PN-MOT-01", "Hub motor", 2.5, 18000),
                             ("ELECTRICAL", "CHG-C05", "PN-CHG-02", "Charger port", 1.0, 3500),
                             ("BODY", "BODY-01", "PN-BDY-07", "Side panel", 0.5, 1800)])
            claims.append((f_day + timedelta(days=r.randint(1, 10)), f_day) + part)
        for j, (c_day, f_day, cat, code, pn, pdesc, hrs, amt) in enumerate(claims):
            if c_day > end:
                continue
            claim_id = f"WC{vin[-9:]}{j:02d}"
            yield ("row", "warranty_claims", (claim_id, vin, c_day.isoformat(), f_day.isoformat(),
                                              round(profile["daily_km"] * (f_day - delivery).days, 1), cat, code, pn,
                                              pdesc, hrs, round(amt * r.uniform(0.9, 1.1)), "SUBMITTED", dealer))
            if cat == "BATTERY" and r.random() < v["teardown_share_of_battery_claims"]:
                t_day = c_day + timedelta(days=r.randint(15, 35))
                if t_day <= end:
                    cat_txt, findings = ROOT_CAUSE_TEXT[event["cause"]]
                    doc_id = f"TD{vin[-9:]}{j:02d}"
                    yield ("file", f"supplier_8d/{t_day}/{doc_id}.pdf", text_pdf(
                        "Field return teardown report",
                        [("Report no", doc_id), ("Report date", t_day.isoformat()), ("Report type", "TEARDOWN"),
                         ("Claim no", claim_id), ("VIN", vin), ("Pack serial", pack["pack_id"]),
                         ("Cell batch", pack["majority_batch"]), ("Root cause category", cat_txt)],
                        [("Findings", findings),
                         ("Containment", "Quarantine remaining packs built from the same cell batch pending review.")]))


def _truth(pack, profile, delivery, event, p90=None, contrib=None, live=False) -> dict:
    return {
        "pack_id": pack["pack_id"], "vin": pack.get("vin"), "assembly_date": pack["assembly_date"].isoformat(),
        "delivery_date": delivery.isoformat() if delivery else None, "majority_batch": pack["majority_batch"],
        "n_batches": pack["n_batches"], "contaminated": pack["contaminated"], "drift": round(pack["drift"], 3),
        "n_subthreshold": pack["n_subthreshold"], "weak_bond": pack["weak_bond"], "eol_pass": pack["eol_pass"],
        "p90": round(p90, 5) if p90 is not None else None,
        "positive": event is not None, "event_day": event["day"] if event else None,
        "event_date": event["date"].isoformat() if event else None, "event_kind": event["kind"] if event else None,
        "event_cause": event["cause"] if event else None, "live_subset": live,
        "contrib": {k: round(x, 4) for k, x in (contrib or {}).items()},
    }


def _poisson(r, lam: float) -> int:
    if lam <= 0:
        return 0
    L, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= r.random()
        if p <= L:
            return k
        k += 1


def _edr_samples(r, trig: str, pack: dict, profile: dict, n_pre: int = 300, n_post: int = 300) -> list[list[float]]:
    out = []
    t_base = 34 if profile["hot"] else 30
    for i in range(n_pre + n_post):
        post = i >= n_pre
        heat = (i - n_pre) / n_post * 18 if (post and trig == "thermal") else 0.0
        speed = max(0.0, 35 - (i - n_pre) * 0.4) if (post and trig == "crash") else 35 + r.gauss(0, 2)
        cv = 3.62 + r.gauss(0, 0.004)
        out.append([47.1 + r.gauss(0, 0.2), -18 + r.gauss(0, 3), cv - pack["v_spread_mv"] / 1000 - (0.05 if heat else 0),
                    cv + 0.01, t_base + heat + r.gauss(0, 0.3), 45 + r.gauss(0, 1), speed, 61.0])
    return out
