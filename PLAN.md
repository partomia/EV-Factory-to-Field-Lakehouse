# EV Factory-to-Field Lakehouse (`evx`) — Build Plan

2026-11-01 · @rsingh · prompt `EV_CURSOR_PROMPT.md` v3.3 · skill `cloudera-usecase-builder`

All data in this project is **synthetic** (seeded generators). It is never presented as the customer's data. The
customer's display name comes only from the git-ignored `.env` (`EVX_BRAND_NAME`); the neutral default is
"EVX Mobility".

Every departure from the prompt is marked **[CHANGE]** with its reason. Values marked *to verify* are checked
read-only or in Phase 0 before they are relied on.

---

## 1. Story and decision

A two-wheeler EV OEM makes its own 4680 dry-electrode cells (gigafactory), builds packs and scooters in a vehicle
factory, and runs a connected fleet with service and warranty. Today a bad cell batch is found from warranty claims,
months after the cells left the plant. The lakehouse joins **factory DNA** (supplier batch → material lot → cell →
module → pack → VIN) to **field signals** (telematics, DTCs, late EDR dumps, service, claims), so that quality
engineers can act weeks earlier.

**The one claim the demo proves:** on one governed lakehouse, a risk ranking built from factory data alone finds the
packs that will have a battery event in the field, and traces them back to the supplier batches, stations and shifts
that caused them, faster than waiting for claims (TTC, risk-ranked vs claim-driven, on the same events).

**Decisions supported (workflow prioritisation only):**
- (a) **Vehicles** to call in for a proactive battery inspection, ranked by risk.
- (b) **Material and supplier batches** to investigate, where packs built from them show elevated risk.

**Not decided by the model:** no recall, no OTA derate, no supplier rejection, no warranty decision. A quality
engineer decides; the app records the decision in `gold.review_feedback`.

**Backbone first.** Use case 1 is the first consumer of a lakehouse built as data products for EV use cases that
do not exist yet (section 11).

## 2. Scale (default; `--scale` multiplies volumes; CI is `--scale 0.05`)

| Quantity | Default | How it is set |
|---|---|---|
| History | **270 days**, 2026-02-09 .. 2026-11-05 | section 15 |
| Pack build rate | linear ramp **45 → 90 packs/day** (ramp-phase slice, not the 1M/year nameplate) | `lakehouse.json` `universe.ramp` |
| Packs built in history | **≈ 18,200** | ramp integral |
| **Cells per pack** | **52** (13s4p), ≈ 90 Wh per cell, ≈ 4.7 kWh, ≈ 47 V nominal (synthetic, from public 4680 specs) | **[CHANGE]** below |
| Cells produced | **≈ 1.0M** (946k in packs + graded-out ≈ 4 % + end-of-history WIP) | |
| Cell process rows | **≈ 10M** (10 process steps per cell) | |
| Vehicles delivered | ≈ 17,600 (pack EOL → vehicle EOL → dispatch, 3–10 days) | |
| Matured deliveries (delivered ≤ 2026-08-07) | **≈ 10,800** | 90-day label |
| Incident rate (matured packs positive) | target **9 %** (band 8–10 %), `label.target_rate` | Phase 0 calibration |
| Live subset streaming | **2,000 vehicles**, one reading per 30 s while riding (≈ 1.2 h/day) → ≈ 290k telemetry messages per date | |
| Live business dates | **5**, 2026-11-06 .. 2026-11-10 | section 15 |

**[CHANGE] Cells per pack 150 → 52.** The prompt sizes the pack at about 150 cells. A 4680 cell holds about
83–95 Wh (public teardown and type-approval data: 22.5–26 Ah at 3.7 V), so 150 cells is a 13–14 kWh pack, which is a
car-sized pack (the 4680 car pack has 828 cells), not a scooter's. Two-wheeler packs are about 3–5 kWh, so a
13s4p, 52-cell, ≈ 4.7 kWh pack is the realistic choice. Consequence: about 1.0M cells and ≈ 10M cell-process rows,
instead of 1.7M and 15M+. Default scale still runs on CDE only. The value lives in config (`pack.cells_per_pack`)
and is marked synthetic.

**[CHANGE] Volume: ≈ 18k packs and ≈ 10.8k matured packs, instead of ≈ 11k and ≈ 7.5k.** At 7.5k matured packs and
a 9 % rate the label gives about 675 positives, below the ≥ 800 floor; it only passes at the top of the 8–10 % band.
The 45 → 90 ramp gives about 970 expected positives with margin for TEST ≥ 100.

## 3. Requirements map

| # | Requirement (prompt step) | Where it is met |
|---|---|---|
| R1 | 10 synthetic source systems + `evx.control` (S2) | `generators/sources/*`, `stream/producer/`, CDE `rsingh-evx-generate` |
| R2 | Seeded, partition-deterministic generation on CDE (S2) | `generators/partitioned.py`, `tests/test_generator_determinism.py` |
| R3 | Planted causes recur; positives in TRAIN and TEST (S2) | `generators/truth/`, `tests/test_cause_split_coverage.py` |
| R4 | Planted data faults caught where planted (S2) | `generators/faults.py`, `config/expected_recon.json`, `tests/test_planted_faults.py` |
| R5 | Expected reconciliation (S2) | `cde/jobs/recon.py`, `config/expected_recon.json` |
| R6 | Data products, contracts, owners, SLAs, glossary (S3) | `contracts/products/*.json`, `docs/DATA_PRODUCTS.md`, `config/governance.json` |
| R7 | Databases `rsingh_evx_{bronze,silver,master,gold,features,semantic,ref}` (S3) | `config/lakehouse.json`, `cde/jobs/evx_common.py` |
| R8 | Genealogy, SCD2, MDM golden records + xref (S3) | `cde/jobs/build_master.py`, `cde/jobs/build_genealogy.py` |
| R9 | Gold dims and facts (S3) | `cde/jobs/gold_*.py` through `lakehouse/publish.py` |
| R10 | Idempotent increments, backfill (S3) | `overwritePartitions` per date, `scripts/backfill.py` |
| R11 | Late data reprocessing + `ref.reprocess_log` (S3) | `cde/jobs/reprocess_late.py` |
| R12 | Schema evolution from contracts (S3) | `cde/jobs/ingest_bronze.py`, `ref.schema_change` |
| R13 | Iceberg maintenance `rsingh-evx-maintenance` (S3) | `cde/jobs/maintenance.py` |
| R14 | WAP spike then `publish()` (S3) | `cde/jobs/wap_spike.py`, `docs/WAP_SPIKE.md`, `lakehouse/publish.py` |
| R15 | Freshness SLAs (S3) | `ref.freshness`, DAG SLA-miss callback, CDV tile |
| R16 | Failed-batch drill (S3, GDL) | `--mode fail-during/resume`, `docs/FAILED_BATCH_DEMO.md` |
| R17 | Recon per layer and for streams (S3) | `cde/jobs/recon.py`, `evx.control` |
| R18 | Privacy (DPDP framing), masks, geohash-5, retention (S3) | `config/governance.json`, `scripts/governance.py` |
| R19 | 4 streams + aggregation + line-cycle window (S3) | `cde/jobs/stream_*.py` |
| R20 | NiFi alert router (+ optional EDR ingest) (S3) | `nifi/flows/evx_alert_router.json`, `nifi/deploy_flow.py` |
| R21 | One Airflow DAG per business date, 11 stages (S3) | `cde/dags/evx_dag.py` |
| R22 | `pack_risk_eol` champion (S4) | `usecases/uc01_pack_risk/` + `mlcore/` |
| R23 | `pack_risk_field` silent trial + promotion `evx-uc01-07` (S4) | `mlcore/promote.py`, `config/kpi.json` `go_live` |
| R24 | Point-in-time features, leakage tests, field-grain parity (S4) | `features/pit.py`, `tests/test_pit_*.py` |
| R25 | Public battery dataset calibrates degradation (S4) | `generators/calibration/severson.py` (section 17) |
| R26 | Model choice per task (S4) | section 17 |
| R27 | LLM summaries, nightly, grounded, guarded (S4, M3) | CAI `evx-uc01-09-summarise`, `gold.doc_summary`, `mlcore/llm_guard.py` |
| R28 | Guardrails (S4) | `config/guardrails.yaml`, `mlcore/guardrails/`, `ref.guardrail_event` |
| R29 | KPIs FPY, FIR90, TTC + consistency check (S4) | `sql/semantic/10_kpi_*.sql`, `run_semantic.py --steps kpi_check` |
| R30 | App: 7 tabs (S4) | `app/app.py` + `usecases/*/app_page.py` |
| R31 | 6 CDV dashboards as code (S4) | `dataviz/build_dashboard.py`, `dataviz/evx_*.json` |
| R32 | Core / use-case boundary (S5) | `usecases/`, `tests/test_boundary.py` |
| R33 | Versioned contracts + consumer registry CI test (S5) | `contracts/products/`, `config/consumers.json`, `tests/test_consumers.py` |
| R34 | Config-driven DAG, app pages, dashboards (S5) | `usecases/*/config.yaml` `enabled` |
| R35 | `mlcore/`, `_template`, `new_usecase.py`, uc02 scaffold, backlog with readiness (S5) | `usecases/_template/`, `scripts/new_usecase.py`, `docs/USE_CASE_BACKLOG.md` |
| R36 | Design rules 1–14 (S6) | `tests/test_cai_contract.py`, `tests/test_impala_sql.py`, `.cursor/rules/` |
| R37 | Ops: `platform_ctl.py`, start/stop rule, self-stop job (S6.12) | `scripts/platform_ctl.py`, `config/platform.json`, CAI `stop-evx-project` |
| R38 | Secrets + brand hooks (S6.13) | `scripts/hooks/`, `scripts/install_hooks.sh` |
| R39 | Resource increases with ceilings (S6.14) | section 18, `config/platform.json` `resources`, `docs/PROJECT_LOG.md` |
| R40 | Skill enhancement (S10) | OGX `tools/cursor-skills/cloudera-usecase-builder/` |
| R41 | Docs listed in S7 | `docs/*.md` |
| R42 | Definition of done | phase 12 checklist |

## 4. Platform mapping (environment `federal-cdp-env`, AWS `ap-south-1`; discovered read-only 2026-11-01)

| Layer | Service | What runs there |
|---|---|---|
| Landing / raw | S3 `s3a://federal-buk-574bcea0/data/IB/rsingh_evx/{landing,raw,checkpoints,reports}/` | generator output, `_manifest.json` per source and date |
| Batch + streaming compute | CDE `federal-cde` / VC `federal-cde-vc` (Spark 3.5.4, Iceberg 1.5.2; quota 20 CPU / 80 GiB shared; r5.4xlarge nodes, max 50) | generators, bronze → gold, features, streams, maintenance, WAP spike |
| Orchestration | CDE Airflow (same VC) | `rsingh-evx-orchestration` |
| Streams | Data Hub `federal-kafka` (Streams Messaging Light Duty 7.3.2, SASL_SSL PLAIN, Schema Registry, SMM) | `evx.*` topics |
| Flow routing | Data Hub `federal-nifi` (NiFi 2) | `EVX - Alert Router` |
| SQL / BI | CDW `federal-impala-1` (Impala, 2 × r5d.4xlarge per executor group, autoscale 1–5) | semantic views, KPIs, app reads |
| Dashboards | Data Visualization (the instance OGX used; connection `federal-impala-1`) | 6 dashboards, keys 15000+ |
| ML + app | CAI `federal-cml-ws` (CPU groups m5.2xlarge 1–10 nodes, now 2; m5.8xlarge 4–8, now 4; GPU group max 1 at 0, **not used**) | project `rsingh-ev-factory-to-field`, jobs, model, app |
| Governance | SDX data lake `federal-aw-dl` (Atlas, Ranger `cm_tag`, masked users `federal01` / `federal07`) | classifications, glossary, masks |

Not used: `federal-flink` (documented as the sub-second alternative), `federal-hive-1`, `federal-trino-1`.

## 5. Data products (domains)

| Domain | Owner (synthetic role) | Bronze → silver → gold | Freshness SLA | Glossary term |
|---|---|---|---|---|
| `supply` | Supplier Quality | SAP materials, vendors, batches, BOMs, production orders, goods movements, QM lots, CoA PDFs, spectra → `dim_supplier`, `dim_material_batch` (SCD2) | daily, by 06:00 IST | EVX Supply Batch |
| `cell` | Cell Quality | MES step records, formation summaries + curves, X-ray / film verdicts + images → `dim_cell`, `fact_cell_process`, `fact_cell_grading`, `fact_qc_inspection` | per shift + 2 h | EVX Cell Grade |
| `pack_vehicle` | Pack & Vehicle Quality | pack docs, wire-bond tests, pack EOL, vehicle EOL → `dim_pack` (SCD2), `dim_vehicle` (SCD2), `fact_pack_eol`, `fact_vehicle_eol`, `bridge_genealogy` | daily | EVX Pack Genealogy |
| `field` | Fleet Reliability | telemetry, DTC, EDR → `fact_telemetry_window`, `fact_vehicle_daily`, `fact_dtc`, `fact_edr_event` | streams 15 min; EDR 72 h (late by design) | EVX Field Event |
| `service` | Service & Warranty | riders (PII), job cards (PDF), claims, parts → `dim_rider` (masked), `fact_service_job`, `fact_warranty_claim` | daily | EVX Warranty Claim |
| `twin` | Manufacturing Engineering | twin scenarios, line events → `dim_station`, `fact_line_cycle`, `fact_twin_scenario` | line 15 min; twin weekly | EVX Line Cycle |

Shared: `dim_date`; `features` (point-in-time, versioned, any use case); `semantic` (KPI and dashboard views);
`ref` (audit, recon, freshness, reprocess log, guardrail / model events, schema changes, business calendar).
`docs/DATA_PRODUCTS.md` is generated from the contracts and `config/consumers.json`.

## 6. Names

| Object | Name |
|---|---|
| Databases | `rsingh_evx_bronze`, `_silver`, `_master`, `_gold`, `_features`, `_semantic`, `_ref` |
| S3 | `s3a://federal-buk-574bcea0/data/IB/rsingh_evx/{landing,raw,checkpoints,reports}/<source>/<date>/` |
| CDE resource | `rsingh-evx-pipeline` (git, public repo) |
| CDE batch jobs | `rsingh-evx-generate`, `-land`, `-bronze`, `-extract`, `-silver`, `-master`, `-gold`, `-features`, `-outcomes`, `-recon`, `-reprocess`, `-backfill`, `-maintenance`, `-wap-spike` |
| CDE streaming jobs | `rsingh-evx-stream-produce`, `-stream-telemetry`, `-stream-dtc`, `-stream-qc`, `-stream-line`, `-stream-agg`, `-stream-line-window` |
| CDE DAG | `rsingh-evx-orchestration` |
| CAI project | `rsingh-ev-factory-to-field` |
| CAI jobs | `evx-uc01-00-sync-code`, `-01-features`, `-02-train`, `-03-evaluate`, `-04-gate`, `-05-register-deploy`, `-06-score`, `-07-promote`, `-08-drift`, `-09-summarise`; `evx-uc02-02-baseline`; `stop-evx-project` |
| CAI model / app | model `evx-uc01-service`; app `EVX Quality & Reliability Workbench` |
| Registry models | `evx-uc01-pack-risk-eol`, `evx-uc01-pack-risk-field` |
| Kafka topics | `evx.vehicle.telemetry`, `evx.vehicle.dtc`, `evx.factory.qc`, `evx.factory.line`, `evx.alert.priority`, `evx.control` |
| NiFi | process group `EVX - Alert Router`, registry bucket `evx-flows` |
| Atlas / Ranger | classifications `EVX_SENSITIVE_PII`, `EVX_SENSITIVE_LOCATION`, `EVX_SENSITIVE_CONTACT`; glossary `EVX Data Products`; policies `rsingh-evx-*` |
| CDV | dashboards `EVX Factory Quality & Yield`, `EVX Traceability & Containment`, `EVX Fleet Reliability`, `EVX Streaming & Freshness`, `EVX Models, Drift & Guardrails`, `EVX Data Estate`; keys core 15000–15999, uc01 16000–16499, uc02 16500–16999 |
| Env vars | `EVX_*` (section 19) |

## 7. Sources, planted causes and planted faults

| # | Source | Generator output | Cadence | Domain |
|---|---|---|---|---|
| 1 | SAP S/4HANA ERP | OData-style JSON pages (`MATNR`, `CHARG`, `LIFNR`, `AUFNR`, `PRUEFLOS`, `BWART` …), full load then daily delta pages with delta tokens; field `ZZ_MOISTURE_PPM` appears on 2026-11-08 (schema evolution) | daily | supply |
| 2 | Cell MES | pipe-delimited CSV per process area per shift with trailer counts; formation summaries for every cell; full cycler curves for flagged cells + 1 % sample | per shift | cell |
| 3 | Inline QC | verdict, defect code and scores for every cell (rows + `evx.factory.qc`); X-ray PNGs and film PNGs for flagged + 1 % sample; JCAMP-DX spectra per powder lot | per shift / per lot | cell, supply |
| 4 | Pack line MES | JSON document per pack (cells → modules → pack, wire-bond pulls, pack EOL) | per shift | pack_vehicle |
| 5 | Vehicle EOL | JSON document per vehicle (VIN → pack, motor, MCU, EOL results) | per shift | pack_vehicle |
| 6 | Digital twin | scenario JSON; actual station cycle events on `evx.factory.line` | weekly / streaming | twin |
| 7 | Telematics | daily summaries (history, legacy store); `evx.vehicle.telemetry` and `evx.vehicle.dtc` (live) | daily / streaming | field |
| 8 | EDR | binary dumps (header + 10 Hz pre/post buffer), synced hours to days late | late | field |
| 9 | Service DMS / CRM | rider master (PII) CSV, job-card PDFs (scanned), warranty claims CSV, parts | daily | service |
| 10 | Supplier quality | CoA PDF per material batch; teardown and 8D PDFs per investigation | daily | supply |
| + | `evx.control` | per-minute producer counts per topic | streaming | ref |

**[CHANGE] Pack and vehicle documents are written as NDJSON, one file per line per shift** (each line is one pack or
vehicle JSON document). The prompt says JSON documents per pack. One S3 object per pack and per vehicle is ≈ 36k
small objects at initial load. The documents themselves are unchanged and parsed the same way.

**[CHANGE] Full formation curves are packed one CSV per formation line per shift**, instead of one per cycler
channel per shift. With ≈ 500 channels, per-channel-per-shift files would be ≈ 400k mostly tiny objects; per line
per shift is ≈ 3.2k at initial load and 12 per date. Each row carries `channel_id`, so nothing is lost.

**Planted causes (cause → effect), recurring across the history.** Truth lives in `generators/truth/` and
`rsingh_evx_ref.generator_truth` (never read by features: `tests/test_no_truth_in_features.py`).

| Cause | Episodes (history 2026-02-09 .. 2026-11-05) | Factory signal | Field effect |
|---|---|---|---|
| Cathode powder contamination | 5 supplier lots: weeks of 03-02, 04-13, 05-25, 06-29, 07-20 (last two feed the TEST window) | spectrometer anomaly; higher K-value at aging | self-discharge and thermal DTCs |
| Calender roller drift | 6 episodes on different lines and shifts, 3–5 days each, spread Mar–Jul | film thickness drift | wider capacity spread in packs → imbalance |
| X-ray overhang just under threshold | steady 0.3 % of cells | sub-threshold overhang score | higher thermal / imbalance rate |
| Weak wire-bond station `PB-WB-03` | 3 periods between maintenance events (Mar, May, Jul) | low minimum pull strength | cell-imbalance faults |
| Usage modifiers | whole period | — | fast-charge share, hot geohashes, aggressive ride mode raise rates |

`tests/test_cause_split_coverage.py` proves each cause has positives in TRAIN and TEST of the chosen split.

**Planted data faults** (each with a test that it is caught where planted; expected-recon rows in section 16):
SAP delta page re-sent; MES CSV trailer off by one; corrupt X-ray PNG; scanned job card with no text layer; pack
genealogy referencing a non-existent cell; vehicle re-flashed with firmware reporting °F; stuck SoC sensor; EDR
dumps 3 days late; VIN typo between DMS and EOL; pack swapped between two vehicles mid-week (SCD2); clock skew on one
station's line events.

## 8. Genealogy and gold model

```
supplier ─ material_batch (SCD2) ─ material lot ─ cell ─ module ─ pack (SCD2) ─ vehicle/VIN (SCD2)
                                                                      └─ telemetry · DTC · EDR · service jobs · claims
```

- **Master (GDL pattern):** golden `material_batch`, `cell`, `pack`, `vehicle`, `supplier`; candidates → match rules
  → connected components → survivorship; `xref` from every source key (SAP `CHARG`, MES lot, pack serial, VIN,
  registration). Unresolved identities go to `ref.identity_review`.
- **Gold dims:** `dim_date`, `dim_supplier`, `dim_material_batch` (SCD2), `dim_station`, `dim_cell`, `dim_pack`
  (SCD2), `dim_vehicle` (SCD2: pack link, firmware), `dim_rider` (masked).
- **Gold facts:** `fact_cell_process`, `fact_cell_grading`, `fact_qc_inspection`, `fact_pack_eol`,
  `fact_vehicle_eol`, `fact_telemetry_window` (15 min), `fact_vehicle_daily`, `fact_dtc`, `fact_edr_event`,
  `fact_service_job`, `fact_warranty_claim`, `fact_line_cycle`, `fact_twin_scenario`, `bridge_genealogy`.
- **Partitioning / sort:** facts by `days(event_ts)` (or `business_date`) with hidden partitioning; sort by the
  main join key (`pack_id`, `vehicle_id`, `cell_id`). Streams: `hours(event_ts)`, compacted nightly.
- **Shared tables with `use_case`:** `gold.model_score`, `gold.review_feedback`, `gold.daily_model_summary`,
  `ref.model_event`, `ref.guardrail_event`. Use-case outputs: `gold.uc01_*`.

## 9. Production behaviour

| Behaviour | Design | Milestone |
|---|---|---|
| Initial load + daily increments | idempotent `overwritePartitions` per `business_date`; `scripts/backfill.py --from --to` | M1 |
| `publish()` | one helper for every gold write; M1 = plain `overwritePartitions`; M2 = the WAP path chosen by the spike | M1 / M2 |
| WAP spike | first live step: `rsingh-evx-wap-spike` on `rsingh_evx_ref.wap_spike_scratch` (6 checks of the prompt), result in `docs/WAP_SPIKE.md`, scratch dropped | M1 (phase 0) |
| Late data | EDR or late telemetry re-opens affected dates; only touched gold aggregates are recomputed; `ref.reprocess_log` | M2 |
| Schema evolution | contract allows additive fields; SAP `ZZ_MOISTURE_PPM` accepted, recorded in `ref.schema_change`, surfaced in the Data Estate dashboard | M2 |
| Table maintenance | `rsingh-evx-maintenance` nightly 04:00 UTC: compaction of stream tables, rewrite manifests, expire snapshots > `retention.snapshots_days` (7), remove orphans > 3 days | M2 |
| Freshness SLAs | `ref.freshness` per product, DAG `sla_miss_callback`, green / amber / red tile | M2 |
| Failed-batch drill | `--mode fail-during` then `resume` on `silver` (GDL) | M2 |
| Recon | per layer (counts, sums, keys) + streams (`evx.control` vs bronze), compared with `config/expected_recon.json` | M1 (batch) / M2 (streams) |
| Privacy | rider name, phone, address, precise location classified; geohash-5 in silver; Ranger masks for `federal01` / `federal07`; retention per table | M1 (classify + mask) / M3 (verify both users) |

**Streaming (OGX pattern):** trigger 60 s, watermark 10 min (DTC 30 min, per topic in `config/streaming.json`),
exactly-once batch markers on Iceberg commits, MERGE through `foreachBatch`, long-running for demos and
available-now inside the DAG. M1 has `evx.vehicle.telemetry` only.

**DAG `rsingh-evx-orchestration`:** land → bronze → extract → silver → master → gold (WAP) → features →
CAI `evx-uc01-06-score` → outcomes → recon; maintenance on its own schedule. `max_active_runs=1`,
`dagrun_timeout` 4 h, task `execution_timeout` 90 min, `retries=0`. Manual runs take `params.business_date`.

## 10. Use case 1: pack field-risk from factory DNA

| Head | Scores when | Features | Stage |
|---|---|---|---|
| `pack_risk_eol` | once at vehicle EOL (VIN known), before dispatch | factory only: grading spread across the pack's 52 cells (capacity, IR, K-value tails), formation anomaly (Isolation Forest), X-ray / film scores, wire-bond minimum pull, supplier / material batch quality (factory + QC only), station, shift | **champion** (M1) |
| `pack_risk_field` | daily, from day 14 in the field | EOL features + daily-grain telemetry (temperature exposure, imbalance trend, fast-charge share, DTC counts) + EDR events | **silent trial** → `evx-uc01-07` (M2) |

- **Label:** a battery-related field event within 90 days of delivery (thermal or imbalance DTC, battery warranty
  claim, EDR thermal trigger). Broad by design, so far above a true failure rate (stated in `docs/DATA_MODEL.md`).
- **[CHANGE] Field-head label window is (day 14, day 90].** The prompt gives one 90-day label. The field head scores
  at day 14, so an event already seen by day 14 is known at score time (it would also trip the safety floor). Such
  vehicles are excluded from field-head training and the backtest, and reported separately.
- **Split:** cell production batch = cathode mixing lot (≈ 2 lines × 3 shifts × 270 days ≈ 1,600 batches). Batches
  ordered by the last delivery of their packs; TEST = latest ≈ 20 % of batches; TRAIN = rest, 5 out-of-fold folds
  grouped by batch. A mixed pack goes with its majority batch (test checks the mixed share < 15 %).
- **Thresholds:** from the 5-fold out-of-fold scores (top-k worklist capacity and the abstain band), never VAL or
  TEST. Isotonic calibration on OOF. Selection by grouped OOF PR-AUC; AUROC reported.
- **Point-in-time:** every feature row has `as_of`; tests: no feature postdates the score time; the EOL head never
  sees post-dispatch data; batch-feature leakage test (remove later-maturing outcomes of the same batch → no value
  changes); field-grain parity (same synthetic day through the legacy-summary and stream paths → identical features).
- **Field go-live (`models.pack_risk_field.go_live`):** (1) rolling-origin backtest at day 14 on matured history:
  ≥ 150 positives, sensitivity ≥ 0.60 at the worklist threshold, specificity ≥ 0.80, AUROC lift ≥ +0.03 over
  `pack_risk_eol` on the same vehicles (values frozen in Phase 0 calibration); (2) live stability over ≥ 3 trial
  dates: score PSI vs backtest < 0.25, abstain rate < 10 %, OOD rate < 5 %, Spearman rank agreement with the champion
  ≥ 0.5. `PROMOTION_REFUSED` with failing checks is a valid outcome. Approver name from `EVX_APPROVED_BY`.
- **Scoring population (field):** the live subset with ≥ 14 days in field and delivered ≤ 90 days before the score
  date. The live subset is drawn from deliveries 2026-08-08 .. 2026-10-23.
- **Guardrails (`ref.guardrail_event`):** input: contracts / quarantine, genealogy completeness (a pack with a
  missing cell grading record abstains), sensor sanity (stuck SoC, °F / °C, too few readings), OOD distance,
  unresolved identity → review queue. Output: abstain band; **safety floor** (any EDR thermal trigger or thermal DTC
  → P1 whatever the score); no automated action. Process: KPI gate + non-regression, feature-hash lock, PSI drift
  (`TOO_FEW` below 30), silent trial + named approver. LLM (M3): page citations, PII redaction, refusals logged.
  **M1 minimum:** contracts / quarantine, genealogy completeness, OOD, abstain band, safety floor (DTCs from batch
  history), no automated action, KPI gate, feature-hash lock.
- **KPIs (certified, Impala views + consistency check):** FPY (cell, pack; by line, station, shift, day); FIR90
  (per 1,000 vehicles within 90 days, by build week and supplier batch); TTC (days from first field signal to batch
  flagged; risk-ranked vs claim-driven on the same events; safety-floor P1 vehicles reported as their own group and
  not credited to the model).
- **App `EVX Quality & Reliability Workbench`:** Vehicle risk worklist, Batch containment, Pack 360 (M1); Line &
  twin, Data products & freshness, Models & guardrails, Engineer feedback (M2; feedback table exists from M1).
- **Dashboards:** Factory Quality & Yield, Traceability & Containment, Fleet Reliability (M1); Streaming &
  Freshness, Models Drift & Guardrails, Data Estate (M2).

## 11. Extensibility design (backbone)

- **Core** (`generators/`, `stream/`, `extract/`, `cde/`, `contracts/`, `sql/semantic/` core views, `features/`
  shared tables, governance, ops) knows no use case. **A use case** (`usecases/ucNN_<name>/`) reads only `gold`,
  `features`, `semantic`, writes `gold.ucNN_*` and the shared tables with its `use_case` value, never core tables.
  `tests/test_boundary.py` parses use-case SQL / Spark reads and fails on any other database.
- **Contracts:** `contracts/products/<domain>.<table>.json` with semver; additive = minor; removed / retyped = major
  with one release of deprecation. `config/consumers.json` maps use case → tables → columns;
  `tests/test_consumers.py` fails when a core change breaks a registered consumer.
- **Config-driven wiring:** the DAG builds feature and scoring tasks from every `usecases/*/config.yaml` with
  `enabled: true`; the app discovers `usecases/*/app_page.py`; `dataviz/` builds use-case dashboards from each spec,
  keys from the use case's 500-key block.
- **New source or domain:** a generator module under `generators/sources/`, a contract, a config entry
  (`docs/ADDING_A_DATA_PRODUCT.md`).
- **`mlcore/`** (copied from OGX / CXR, generalised): feature hash and feature-table builder, KPI gate, registry,
  deploy with rollback, drift, guardrails, silent trial and promotion, LLM guard. M1 is built inside this structure.
- **`usecases/_template/` + `scripts/new_usecase.py ucNN <name>`**; `docs/ADDING_A_USE_CASE.md`;
  `docs/USE_CASE_BACKLOG.md` with uc02–uc10, each with its data-readiness SQL check
  (`run_semantic.py --steps readiness`).
- **uc02 proof:** scaffolded from the template, readiness check, feature view, baseline trained on a default-scale
  extract as CAI job `evx-uc02-02-baseline`, not deployed; `git diff` shows no core file changed.

## 12. Phases (one commit each, pushed; PROJECT_LOG entry after every live step)

| Phase | Contents | Milestone |
|---|---|---|
| ☐ 0 | Calibration and discovery: shared-capacity check; **WAP spike**; pick and pin dataset, LightGBM / scikit-learn, LLM (measure tok/s at 2 vCPU / 8 GB); generator dry run at CI scale; distributions, rates, splits, object counts printed and frozen | M1 |
| ☐ 1 | Repo skeleton, config (`lakehouse.json`, `pipeline.yaml`, `streaming.json`, `kpi.json`, `guardrails.yaml`, `governance.json`, `platform.json`, `ci.yaml`, `expected_recon.json`, `consumers.json`), contracts, tests, CI workflow, `common.py`, `cdsw-build.sh` | M1 |
| ☐ 2 | Generators: `supply`, `cell`, `pack_vehicle`, field + service history (DTC, EDR, daily summaries), planted causes + faults; partitioned CDE generator; `scripts/run_local.py` at 0.05; `platform_ctl.py` + start/stop rule | M1 |
| ☐ 3 | Land, bronze (contracts, quarantine), extract (PDF, OCR, PNG, JCAMP-DX, EDR binary, OData, NDJSON) | M1 |
| ☐ 4 | Silver, master (MDM, xref), genealogy, SCD2 | M1 |
| ☐ 5 | Gold through `publish()`; semantic views; FPY, FIR90; SDX classifications + masks; first default-scale CDE run | M1 |
| ☐ 6 | Telemetry stream: producer (CDE), bronze stream, aggregation | M1 |
| ☐ 7 | `pack_risk_eol`: features (PIT), train, evaluate, gate, registry, deploy, M1 guardrails, scoring job `evx-uc01-06`; CAI project + jobs | M1 |
| ☐ 8 | DAG with the CAI scoring task; app (3 tabs); 3 dashboards; demo runbook draft; **M1 demo** at default scale; skill structural update #1 | M1 |
| ☐ 9 | DTC / QC / line streams, line-cycle window, NiFi router; late data; schema evolution; maintenance; WAP in `publish()`; freshness; failed-batch drill; `twin` domain | M2 |
| ☐ 10 | `pack_risk_field` silent trial, backtest, `evx-uc01-07`; outcomes job, TTC; remaining tabs and dashboards; guardrails complete; 5 live dates through the DAG | M2 |
| ☐ 11 | `mlcore/` refactor (if still needed); versioned contracts, consumer registry + CI test, `ADDING_A_DATA_PRODUCT.md`; LLM summaries (`evx-uc01-09-summarise`, `gold.doc_summary`, LLM guard) | M3 |
| ☐ 11b | **Use-case 2 scaffold:** `_template`, `new_usecase.py`, uc02 readiness + feature view + baseline (`evx-uc02-02-baseline`), backlog readiness results | M3 |
| ☐ 12 | SQL files, all docs, governance verified as masked and clear users, runbook rehearsal with timings, `PLAN_REVISED.md`, skill structural update #2, definition-of-done checklist | M3 |

## 13. Expected row and S3 object counts per source (default scale; Phase 0 confirms)

| Source | Rows, initial load | Objects, initial load | Objects per business date |
|---|---|---|---|
| SAP OData pages (6 entity sets) | ≈ 120k | ≈ 1,700 pages | ≈ 6 |
| Cell MES CSV (3 areas × 3 shifts) | ≈ 10M step rows | ≈ 2,430 | 9 |
| Formation curves (flagged + 1 %) | ≈ 25k curves × ≈ 600 points ≈ 15M points | ≈ 3,240 (per line per shift) | 12 |
| QC verdict rows | ≈ 2.0M (X-ray + film) | in MES / stream | — |
| X-ray PNGs (flagged ≈ 1.5 % + 1 % sample) | — | ≈ 25,000 | ≈ 120 |
| Film PNGs (flagged segments + sample) | — | ≈ 2,500 | ≈ 10 |
| Spectra JCAMP-DX (per powder lot) | — | ≈ 540 | 2 |
| Pack NDJSON (per line per shift) | ≈ 18,200 docs | ≈ 1,620 | 6 |
| Vehicle NDJSON | ≈ 17,600 docs | ≈ 810 | 3 |
| Twin scenario JSON | ≈ 400 | ≈ 400 | ≈ 2 (weekly batch) |
| Telemetry daily summaries (legacy) | ≈ 2.1M vehicle-days | ≈ 270 | — |
| DTC history | ≈ 85k | ≈ 270 | — (stream) |
| EDR dumps (binary, ≈ 70 KB) | ≈ 3,000 events | ≈ 3,000 | ≈ 20 |
| Rider master CSV | ≈ 17,600 | ≈ 270 | 1 |
| Job-card PDFs | ≈ 7,000 | ≈ 7,000 | ≈ 40 |
| Warranty claims CSV | ≈ 3,000 | ≈ 270 | 1 |
| CoA PDFs | ≈ 1,500 | ≈ 1,500 | ≈ 5 |
| Teardown / 8D PDFs | ≈ 300 | ≈ 300 | ≈ 1 |
| Manifests | — | ≈ 4,000 | ≈ 15 |
| **Total** | | **≈ 55,000** | **≈ 250** |
| Streams per live date | telemetry ≈ 290k msgs; DTC ≈ 2k; QC ≈ 13k (one per cell inspection); line ≈ 60k; control ≈ 8.6k | Iceberg data files, compacted nightly | |

## 14. Incident rate, packs, cells and expected positives per split

| Population | Packs | Expected positives at 9 % | Floor |
|---|---|---|---|
| Matured deliveries (all) | ≈ 10,800 | **≈ 970** | ≥ 800 |
| TRAIN (≈ 80 % of batches) | ≈ 8,600 | ≈ 780 (≈ 155 per OOF fold) | ≥ 100 per fold |
| TEST (latest ≈ 20 % of batches) | ≈ 2,200 | ≈ 195 | ≥ 100 |
| Field head (event in (14, 90]) | ≈ 10,600 | ≈ 820 (TEST ≈ 165) | ≥ 100 |
| Cells in all packs | ≈ 946k (≈ 1.0M produced) | — | — |

Phase 0 prints the realised rate, packs, cells and positives per split, adjusts the ramp, then freezes. A run with a
realised rate outside 8–10 % fails the calibration test.

## 15. Dates (history end, 5 live business dates, nightly window)

- **History:** 2026-02-09 .. **2026-11-05** (270 days). Matured-label cutoff: deliveries ≤ 2026-08-07.
- **Live business dates:** 2026-11-06, 11-07, 11-08, 11-09, 11-10.
- **[CHANGE] Business dates are decoupled from the wall clock.** The live dates are synthetic business dates held
  in `config/lakehouse.json` `calendar.live_dates`. The nightly DAG reads the next unprocessed live date from
  `ref.business_calendar` (not the Airflow logical date), so a skipped night never skips or repeats a date, and the
  live dates do not depend on when M2 goes live. The DAG window (`start_date` / `end_date`) is set to 5 nights
  when the live phase starts. A date marked `live_done` is never replayed (guard in the DAG and the producer).
- **Nightly window:** **23:30 UTC** (05:00 IST), `dagrun_timeout` 4 h; maintenance 04:00 UTC. Read-only on
  2026-11-01, two sibling DAGs are scheduled and active, Spend at 21:00 UTC and Churn at 22:00 UTC. OGX
  (20:30) and MCC (20:30, 03:30) have ended, and CXR (01:30), Mule, Coll and CASA are paused. **[CHANGE]**
  The prompt implies avoiding 20:30 and 01:30–02:30; the live state moves the free window to 23:30–03:30.

## 16. Expected-recon table (`config/expected_recon.json`)

| # | Planted fault | Business date | Layer | Entity | Check | Expected |
|---|---|---|---|---|---|---|
| 1 | SAP delta page re-sent | 2026-11-07 | bronze | `supply.goods_movement` | delta-token duplicate | EXPLAINED (page dropped, counted) |
| 2 | MES CSV trailer off by one | 2026-09-15 (history) and 2026-11-07 | bronze | `cell.mes_export` formation area, shift B | trailer count | MISMATCH (file quarantined) |
| 3 | Corrupt X-ray PNG | 2026-10-12 and 2026-11-08 | extract | `cell.qc_image` | decode | EXPLAINED (quarantined, verdict row kept) |
| 4 | Job card with no text layer | 2026-11-08 | extract | `service.job_card` | text layer | EXPLAINED (OCR fallback) |
| 5 | Genealogy to a non-existent cell | 2026-10-20 and 2026-11-07 | master | `pack_vehicle.bridge_genealogy` | orphan cell | MISMATCH (pack abstains) |
| 6 | Firmware reporting °F | 2026-11-08 .. 11-10 | silver | `field.telemetry` | unit sanity | EXPLAINED (converted, guardrail event) |
| 7 | Stuck SoC sensor | 2026-11-09 | silver | `field.telemetry` | stuck sensor | EXPLAINED (flagged, abstain) |
| 8 | EDR dumps 3 days late | events of 2026-11-06 arrive 2026-11-09 | gold | `field.fact_edr_event` | late reprocess | EXPLAINED (`ref.reprocess_log`) |
| 9 | VIN typo DMS vs EOL | 2026-10-28 and 2026-11-07 | master | `vehicle` xref | identity match | EXPLAINED (review queue) |
| 10 | Pack swapped between two vehicles | 2026-11-08 | gold | `dim_vehicle` SCD2 | link change | EXPLAINED (two SCD2 versions each) |
| 11 | Clock skew on station `VA-EOL-02` | 2026-11-09 | silver | `twin.line_event` | skew | EXPLAINED (corrected offset) |
| 12 | Schema evolution `ZZ_MOISTURE_PPM` | 2026-11-08 | bronze | `supply.qm_lot` | contract diff | EXPLAINED (`ref.schema_change`) |

An unlisted MISMATCH fails the run; a listed row that is not seen fails it too. History rows (2, 3, 5, 9) make
the M1 guardrails visible in the M1 demo; the others belong to the M2 live dates.

## 17. Model choice per task

| Task | Choice (pinned in Phase 0) | Licence | Notes |
|---|---|---|---|
| `pack_risk_eol`, `pack_risk_field` | **LightGBM 4.7.0**; baseline scikit-learn **1.9.1** `LogisticRegression`; fallback `HistGradientBoostingClassifier` | MIT; BSD-3 | grouped OOF PR-AUC selects; isotonic calibration; explanations from `pred_contrib` |
| Formation anomaly features | scikit-learn `IsolationForest` on curve summaries, fitted on TRAIN batches | BSD-3 | |
| 8D / teardown summaries (M3) | **`Qwen/Qwen3-4B-GGUF`, Q4_K_M (≈ 2.5 GB), non-thinking mode**; fallback `microsoft/Phi-4-mini-instruct` GGUF Q4_K_M (≈ 2.5 GB) | Apache-2.0 (verified on the model's LICENSE file); fallback MIT (verified) | run with `llama-cpp-python` 0.3.36 (MIT) on CPU; revision hash pinned in Phase 0; **tokens/s at 2 vCPU / 8 GB to be measured in Phase 0** (open item: the download needs approval) |
| Retrieval (only if a document exceeds the context) | `BAAI/bge-small-en-v1.5` | MIT | |
| Degradation calibration dataset | **Severson / Attia 2019** (124 LFP 18650 cells, fast-charge cycling), via `data.matr.io/1` or the Parquet mirror `bsebench-org/severson-2019` | **CC BY 4.0** (attribution in `docs/DATA_MODEL.md`) | shapes capacity-fade and IR-growth curves, rescaled to the synthetic 4680 nominal values; chemistry differs (LFP vs the synthetic NMC-type cell), stated. NASA PCoE (US Government work, "licence not specified") is the fallback |

Not chosen: LLMs for scoring; TabICL-style tabular foundation models (2–16 s per call on CPU); GPU models;
hosted or closed LLMs; Cloudera AI Inference (unless Phase 0 finds a CPU endpoint).

## 18. Resources: defaults, ceilings, triggers (rule 14)

Costs are AWS on-demand list prices for `ap-south-1`, compute only, **excluding Cloudera credits**, and only
while the extra capacity is actually in use (all groups autoscale).

| Item | Default | Ceiling | Trigger (evidence) | Extra cost/day at ceiling |
|---|---|---|---|---|
| `evx` CDE batch job (generator, backfill, gold) | 8 executor cores / 32 GB in total (driver 1 / 4 GB) | 16 cores / 64 GB in total | OOM, or stage > 2× Phase 0 estimate with headroom free | within VC quota |
| `evx` CDE streaming job | driver 1 / 2 GB + 1 executor 1 / 2 GB | 2 executors 2 / 4 GB | batch duration > trigger (60 s) for 10 min | within VC quota |
| CDE VC `federal-cde-vc` quota | 20 CPU / 80 GiB (shared) | 32 CPU / 128 GiB | `evx` run in starting > 15 min while quota full | ≈ USD 27 (one r5.4xlarge, 24 h) |
| `evx` CAI jobs | 1 vCPU / 4 GB | 2 / 8 | OOM or > 2× expected runtime | within group |
| `evx` LLM summary job | 2 / 8 | 4 / 16 | < 2 tok/s or OOM | within group |
| `evx` CAI model | 1 / 2 | 2 / 4 | p95 latency > 1 s or OOM | within group |
| `evx` CAI app | 2 / 8 | 4 / 16 | app OOM / restarts | within group |
| CAI CPU group `mlcpu-d697` (m5.2xlarge) max nodes | 10 (2 in use) | 12 | `evx` engine in scheduling > 15 min with the group at max | ≈ USD 19 (2 nodes, 24 h) |
| Impala `federal-impala-1` autoscale max | 5 executor groups | 6 | queries queued > 60 s during a demo or the DAG | ≈ USD 62 (2 × r5d.4xlarge, 24 h) |
| Kafka `evx.vehicle.telemetry` partitions | 6 | 12 | consumer lag > 5 min sustained | none (existing brokers) |

Rules: never resize a CAI instance group while any engine runs; resize shared services only with no sibling run in
flight; capacity before request size; log before / after in `docs/PROJECT_LOG.md` and `config/platform.json`
`resources`; scale back at the end of the need or of the live session unless told to keep it.

**Shared state read-only on 2026-11-01:** CDE no `evx` or sibling run active, no live sessions; VC actual requests
27 CPU / 103 GiB (service overhead above the quota line). CAI apps running: Churn (2), Spend, OGX workbench, MCC;
no model deployment running; CAI schedules active: `cxr-05-nightly-worklist`, `ogx-05-nightly-drift`,
`ogx-08-retrain-trigger`, MCC's four jobs. Re-checked before every live step.

## 19. Credentials and configuration

- `.env` (git-ignored, mode 600): OGX's values renamed `OGX_*` → `EVX_*` (Impala, workload user, CAI host and API
  key, Data Visualization key, CDE endpoint, data lake CRN, Kafka bootstrap / protocol / mechanism / user /
  password / CA path, Schema Registry, SMM, HF token, `HF_HOME`) plus `EVX_BRAND_NAME` (empty until set).
- `.env.example`: placeholders only. Project env vars on CAI: the same names. `EVX_APPROVED_BY` is set per
  promotion run.
- Hooks (`scripts/install_hooks.sh`): `pre-commit` = gitleaks on the staged diff + a check that the
  `EVX_BRAND_NAME` value occurs in no tracked file or file name; `commit-msg` = OGX's trailer strip + the same check
  on the message. Tested with a dummy value.
- CLIs: `cdp` 0.9.163 at `~/Library/Python/3.9/bin/cdp` (not on PATH; scripts call it by `EVX_CDP_CLI` or that
  path), `cde` at `/usr/local/bin/cde` (profile `~/.cde/config.yaml`), `gitleaks` at `/opt/homebrew/bin/gitleaks`.

## 20. Core / use-case boundary and consumer registry

| Use case | Reads | Writes |
|---|---|---|
| core | sources, landing, bronze → gold, `features` shared tables, `semantic` | all core tables |
| uc01 `pack_risk` | `gold.dim_*`, `gold.fact_*`, `gold.bridge_genealogy`, `features.pack_factory_v1`, `features.vehicle_daily_v1`, `semantic.kpi_*` | `gold.uc01_*`, shared tables with `use_case='uc01'` |
| uc02 `formation_shortening` (scaffold) | `gold.fact_cell_process`, `gold.fact_cell_grading`, `features.cell_formation_v1` | `gold.uc02_*` |

`config/consumers.json` lists each table and column per use case; `tests/test_consumers.py` diffs contract versions
against it.

## 21. How we work

Copy the OGX counterpart first (CXR / GDL for their parts), then adapt. Local first at CI scale (`pytest -q`,
`scripts/run_local.py all --scale 0.05`); default scale on CDE only. One commit per phase, pushed. A PROJECT_LOG
entry after every live step (run ids, counts, recon, plan changes, no secrets). New platform lessons go to the
skill's `reference.md` in the same session. Stop the project at the end of every live session. Never touch sibling
resources; ask before stopping anything that is not `evx`.

## 22. Decisions and open items

**Decided (by the owner, before this plan):** Data Viz keys 15000+ (core) and 500-key blocks from 16000; descriptive
customer references may stay public, the brand name only in `.env`; field go-live = backtest + live stability,
`PROMOTION_REFUSED` acceptable; ≥ 800 positives, ≥ 100 per split; default scale on CDE only; WAP spike first; one
approval list; skill enhancement; backbone design; resource increases under rule 14; LightGBM + small CPU LLM.

**Open (asked in the approval message):**
1. `EVX_BRAND_NAME` value (or leave the neutral default).
2. Cells per pack 52 (13s4p) and the larger ramp (≈ 18k packs).
3. Decoupled business calendar and the 23:30 UTC nightly window; any demo times to avoid.
4. LLM pick (Qwen3-4B, fallback Phi-4-mini); tokens/s measured only after the download is approved.
5. Kafka authorisation for `evx.*` topics for `rsingh` (OGX's `ogx.*` worked; to verify live by creating topics).
6. `llama-cpp-python` on the CAI runtime: prebuilt CPU wheel or a one-time source build (needs a compiler in the
   runtime; checked in Phase 0, otherwise `llm.enabled: false` until fixed).
7. GitHub Actions secrets (`CAI_URL`, `CAI_API_KEY`, `CAI_PROJECT_ID`) for the CAI MLOps workflow.
8. NiFi ListenHTTP endpoint reachability for EDR ingest (optional; checked in M2).
