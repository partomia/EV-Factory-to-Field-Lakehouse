# Cursor agent prompt (v3.3, final): EV Factory-to-Field Lakehouse, short code `evx`

Replaces v1 (the charging-network version), v2, v3, v3.1 and v3.2. Modelled on a large Indian electric two-wheeler OEM that makes its
own cells: a 4680 cell gigafactory using a dry electrode process, a pack and vehicle "Futurefactory", SAP Cloud ERP,
a factory digital twin, 100 % inline quality control, and connected scooters with event data recorders. It is built
as a **production prototype**: a lakehouse others can keep adding use cases to, with one ML use case built end to end.

## Changes in v3 (2026-10-30, after reading the skill, OGX, CXR and GDL)

| # | Topic | v2 said | v3 says | Why |
|---|---|---|---|---|
| 1 | Data Visualization keys | 14000+ | **15000+** (dashboards 15000, datasets 15100, visuals 15200 + 100 per dashboard) | `loan-approval` already uses 14000–14505; OGX 13000–13507, CXR 12000–12414 |
| 2 | Customer references in the repo | implied care | descriptive references to the customer's setup (this prompt included) may stay in the public repo; only the brand name itself stays in `.env` | decided by the owner |
| 3 | Tracked files | not specified | `.idea/` is git-ignored; the OGX build-plan `.docx` is reference material, kept untracked (`docs/private/`) | IDE state and another project's plan do not belong in this repo |
| 4 | `pack_risk_field` go-live evidence | CXR-style live sensitivity | backtest on matured history + live stability over the trial dates; `PROMOTION_REFUSED` is an acceptable recorded outcome | a 90-day label cannot mature within 5 live dates |
| 5 | Positives | ≥ 300 positive packs | **≥ 500** positive packs, ≥ 100 positives in every split (TEST included) | rule 5 (≥ 100 per class per split) fails at 300 |
| 6 | Where the default scale runs | locally at CI and default scale | locally at CI scale (`--scale 0.05`) only; **default scale runs directly on CDE** | cell-level history is ~15M+ rows from stdlib generators |
| 7 | Write-audit-publish | verify, else fall back | a **WAP feasibility spike on CDE** is the first live step after approval; the result decides the gold publish design and is logged | Iceberg 1.5.2 has branches and `fast_forward`; Impala branch reads are unlikely |
| 8 | Shared capacity | not specified | read-only check of the shared CAI / CDE state (OGX and other siblings) before the first live step and whenever work queues; never stop a sibling's resources without asking | OGX's README shows it running since 10-23; the quotas are shared |
| 9 | Missing pieces in OGX | "copy from OGX" | OGX has no `promote_champion.py`, `explain.py`, `lakehouse/score_*.py` or `build_outcomes.py`: copy those from CXR | found while reading |

## Changes in v3.1 (2026-10-30, review of v3)

| # | Topic | v3 said | v3.1 says | Why |
|---|---|---|---|---|
| 10 | Images and curves per cell | an X-ray PNG and a formation curve CSV for every cell | **verdicts and scores for 100 % of cells as rows; images and full curves only for every defect plus a ~1 % random sample; curves packed one file per cycler channel per shift** | about 1.7M cells at default scale would mean millions of small S3 objects (slow to write and list, costly). A real line keeps every verdict but only flagged images |
| 11 | Planted causes vs the split | each cause planted once; split grouped by build week | **each cause recurs across the history** (4–6 contaminated batches, several drift episodes, the weak station active in several periods); **split grouped by cell production batch**; a test checks every cause has positives in TRAIN and in TEST | a one-off cause lands wholly in TRAIN (TEST never sees it) or wholly in TEST (the model never learns it); grouping by week does not keep a batch on one side |
| 12 | Batch-quality features | "supplier and material batch quality" | **from factory and QC data only**, or from outcomes already matured before the score time; a dedicated leakage test | a batch failure rate built from other packs' 90-day outcomes is label leakage the plain point-in-time test can miss |
| 13 | Positives and history | ≥ 500 positives, 180 days | **≥ 800 positive packs**, history **270 days** (≈ 180 days of matured deliveries) | only deliveries ≥ 90 days old have a label; 180 days left ~13 build weeks for 5 folds plus TEST, too tight for ≥ 100 TEST positives |
| 14 | Shared CDE quota | not specified | **executor caps for the default-scale generator and backfill (≤ 8 cores / 32 GB total), run off-hours, sizes listed in the approval list** | the 20 CPU / 80 GB virtual cluster is shared with the sibling demos |
| 15 | Scope | everything at once | **three milestones, each finished before the next**: M1 demo-ready core, M2 production behaviour and the field head, M3 extensibility and the rest | OGX ended with many [NOT BUILT] items; v3 asks for more, so there must be a demo after M1 whatever happens later |
| 16 | CI gate | not specified | **`config/ci.yaml` relaxes the gate minimums and uses its own feature hash**, so a CI-trained model can never be deployed | CI scale cannot meet ≥ 100 positives per split (CXR's overlay pattern) |
| 17 | Customer anonymity | brand name blocked by a pre-commit check | stated plainly: **the descriptive references identify the customer**; the check keeps only the brand string out | so nobody mistakes the check for anonymity; keeping the references is a deliberate owner decision (#2) |

## Changes in v3.2 (2026-11-01, review of v3.1; final)

| # | Topic | v3.1 said | v3.2 says | Why |
|---|---|---|---|---|
| 18 | Incident rate | implied, never stated | **target 8–10 % of matured packs positive** (`label.target_rate` in config, calibrated in Phase 0); cells per pack set in config | ≥ 800 positives from ~7.5k matured packs (~1.7M cells) needs ~10 %. Defensible because the label is broad (any thermal / imbalance DTC, battery claim or EDR thermal trigger, not only failures); a 2–3 % rate would need 3–4× the volume |
| 19 | Split | grouped by cell batch, and TEST = latest deliveries | **time-ordered, batch-grouped**: cell batches ordered by the last delivery of their packs; TEST = the latest batches; the 5 OOF folds are batch-grouped too; **every cause has at least one episode inside the TEST window** | the two v3.1 rules conflicted; ungrouped folds leak into the threshold; a time-ordered TEST needs causes inside it |
| 20 | Recon in the definition of done | no MISMATCH | **no unexpected MISMATCH**: every MISMATCH / EXPLAINED matches a row of `config/expected_recon.json` (one per planted fault), and every row there is seen | planted faults produce MISMATCHes by design (OGX: trailer off by one) |
| 21 | Field-head grain | not specified | **field features on the daily grain** (`fact_vehicle_daily`), built by one function from the legacy daily summaries and from the live stream's aggregates; DTC and EDR history exist for the backtest; scoring population stated | history is daily summaries, live is 30 s readings: training / serving skew otherwise |
| 22 | Batch-feature leakage test | no value changes when other packs' outcomes are removed | removing **outcomes that mature after the score time** changes no value | outcomes matured before the score time are allowed, so removing them must change values |
| 23 | "Off-hours" | run off-hours | **submit when the read-only check shows virtual-cluster headroom, outside sibling schedule windows and announced demo times** | the agent works in interactive sessions; siblings' nightly DAGs run around 20:30 UTC and 01:30–02:30 UTC |
| 24 | Object counts | low thousands per business date | estimates **for the initial load and per business date separately**; initial load tens of thousands is acceptable | a 1 % sample of 1.7M cells alone is ~17k X-ray PNGs |
| 25 | M1 contents | WAP in M2, guardrails complete in M2 | **`publish()` used by gold from M1** (simple path; M2 swaps its inside); **M1 minimum guardrails** listed; M1 safety floor reads DTCs from batch history | so M2 does not rewrite M1 and the M1 champion is not unguarded |
| 26 | Live dates | not fixed | **PLAN.md fixes the history end date and the 5 live dates against the nightly window**; never replay a stream date that has live data | OGX: deterministic event ids duplicate on a replay of a live date |
| 27 | uc02 baseline | trained locally | trained on a **default-scale extract** (or as a CAI job); not deployed | a CI-scale baseline means nothing |
| 28 | EOL score time | "at pack and vehicle EOL" | **one score time: vehicle EOL** (VIN known); pack EOL is the earliest feature availability | one head needs one score time for point-in-time tests |
| 29 | TTC and the safety floor | not specified | vehicles raised by the safety floor are **reported separately and not credited to the model** in TTC / risk-vs-claim comparisons | otherwise the floor inflates the model's KPI |
| 30 | Prompt files | v3 and v3.1 side by side | v3.2 is `EV_CURSOR_PROMPT.md`; older versions are not kept in the repo | one source of truth |

## Changes in v3.3 (2026-11-01, owner's additions; final)

| # | Topic | v3.2 said | v3.3 says | Why |
|---|---|---|---|---|
| 31 | Approvals | one list, but destructive and cost items asked again each time | **one approval list for the whole project (M1–M3)**, including `evx`-scoped destructive operations, resource increases up to stated ceilings, model downloads and skill edits; asked again only for the short exception list in Step 8 | owner: take all approvals in one go |
| 32 | Skill | append lessons to `reference.md` | **enhance the skill**: SKILL.md, templates.md and reference.md updated, plus a new `lakehouse-backbone.md`, so the next build (or the next use case on this lakehouse) starts from the production-prototype pattern (Step 10) | owner: the skill needs to be enhanced |
| 33 | Lakehouse as backbone | "add use cases without touching the core" | **explicit core / use-case boundary**: versioned data-product contracts, a consumer registry checked in CI, config-driven DAG / app / dashboards, `use_case` column on shared tables, per-use-case key blocks, `ADDING_A_DATA_PRODUCT.md`; backlog widened to more EV use cases | owner: the lakehouse is the backbone for future EV use cases |
| 34 | Cloudera resources | fixed caps (8 cores / 32 GB; CAI 1 / 4) | **small defaults, and the agent may increase resources when there is a need**, up to ceilings listed in the approval list, with rules (never resize while jobs run, capacity before request size, log before / after, scale back when the need ends) | owner: resources can be increased if needed |
| 35 | ML and LLM | classical features, CPU only; LLM summaries optional and off | **model choice by task**: gradient-boosted trees (LightGBM, with a scikit-learn baseline) for the risk heads, Isolation Forest for formation anomalies; a **small open-weight LLM on CPU for grounded 8D / teardown summaries, in scope in M3**, precomputed nightly, never scoring or deciding | owner: open-source LLM or ML may be used; the wise split is ML for tabular risk, LLM for text |

## Before you paste (once)

1. Create the empty **public** repo `partomia/EV-Factory-to-Field-Lakehouse` and clone it to
   `/Users/rsingh/Downloads/py_aws/EV-Factory-to-Field-Lakehouse`. If you pick another name, change it wherever it
   appears below. *(Done: cloned, initial commit `df4bf55`.)*
2. Install the house skill, if it is not installed yet *(done: symlink present)*:
   ```bash
   mkdir -p ~/.cursor/skills
   ln -sfn ~/Downloads/py_aws/O-G-Asset-Integrity-Lakehouse/tools/cursor-skills/cloudera-usecase-builder \
           ~/.cursor/skills/cloudera-usecase-builder
   ```
3. In Cursor, open the new repo and add `O-G-Asset-Integrity-Lakehouse`, `Chest-X-ray-Triage` and
   `General-DataLakehouse` as extra workspace folders.
4. Customer name: the brand name itself goes only in the git-ignored `.env` as `EVX_BRAND_NAME` (the app and
   dashboards show it at demo time). Descriptive references to the customer's setup (as in this prompt) may stay in
   the public repo. The customer-specific talk track goes in the git-ignored `docs/private/`. Be aware that the
   descriptive references (4680 dry-electrode cells, the "Futurefactory", the 1M units/year nameplate) identify the
   customer on their own: the brand-name check keeps the name string out of Git, it does not make the repo anonymous.
5. Paste everything below the line.

---

Use the **cloudera-usecase-builder** skill. Short code **evx**.

New repo (write here): `/Users/rsingh/Downloads/py_aws/EV-Factory-to-Field-Lakehouse`,
remote `https://github.com/partomia/EV-Factory-to-Field-Lakehouse` (**PUBLIC**, stays public; OGX's CDE sync and CAI
pull broke while it was private).

Reference repos (read only, `/Users/rsingh/Downloads/py_aws/`):
- **OGX** (`O-G-Asset-Integrity-Lakehouse`): the primary template. Unstructured objects, Kafka producer, CDE
  streaming, NiFi, guardrails, MLOps loop, `platform_ctl.py`, Airflow DAG. It ran live on the same "federal"
  environment until 2026-10-23.
- **CXR** (`Chest-X-ray-Triage`): silent trial, named-approver promotion, lakehouse scoring job called from the DAG,
  outcomes job.
- **GDL** (`General-DataLakehouse`): MDM / golden record, SCD2, reconciliation, failed-batch drill, time travel.

OGX was a blueprint: one industry lakehouse and one use case. **This project goes further: a realistic production
prototype.** It is a lakehouse organised as data products, built and maintained by CDE Spark and Airflow, with
production behaviour (incremental loads, late data, schema evolution, table maintenance, freshness SLAs,
point-in-time-correct features). One ML use case is built end to end on it. A documented, tested path adds the next
use cases without touching the core.

**This lakehouse is the backbone for the EV use cases that follow.** Use case 1 is the first consumer, not the
reason the lakehouse exists. Design every core table, contract and job for consumers that do not exist yet
(Step 5).

## Step 1. Read, in this order, nothing else yet

1. The skill: `SKILL.md`, `reference.md` (search it before debugging anything), `templates.md`.
2. OGX:
   - `docs/PLAN_REVISED.md` (plan vs built, and why);
   - `docs/REUSE_INVENTORY.md`, `docs/PROJECT_LOG.md`, `docs/USE_CASE_PLAYBOOK.md`;
   - `docs/GUARDRAILS.md`, `docs/MLOPS.md`, `docs/NIFI.md`, `docs/DEMO_RUNBOOK.md`;
   - `.env.example`, `config/*`, `.cursor/rules/project-start-stop.mdc`, `README.md`.
3. CXR, only:
   - `serve/promote_champion.py`, `serve/registry.py`, `serve/explain.py`;
   - `lakehouse/score_studies.py`, `cde/jobs/build_outcomes.py`;
   - `cde/dags/cxr_dag.py` (the CAI scoring task).
   OGX has none of these files (they are its [NOT BUILT] items), so CXR is the source for them.
4. GDL, only: `cde/jobs/build_mdm.py`, `docs/FAILED_BATCH_DEMO.md`, `sql/time_travel.sql`, `sql/adhoc.sql`.
5. Read, never print: `O-G-Asset-Integrity-Lakehouse/.env`, `~/.cde/config.yaml`.
6. Every sibling's `dataviz/*.json` for the primary keys in use (as of 2026-10-30: Collections / Mule 7000+,
   Spend 8000+, GDL / Churn / CASA 9000+, CXR 12000+, OGX 13000+, loan-approval 14000+).

Already decided:
- Kafka is the existing Streams Messaging Data Hub `federal-kafka`.
- NiFi is `federal-nifi`.
- There is no GPU.
- The CDE virtual cluster, CDW warehouse `federal-impala-1`, CAI workspace and Data Visualization instance are the
  ones OGX used.
- Data Visualization primary keys: 15000+.
- Default-scale data is generated and processed on CDE only; the laptop runs CI scale.
- Default-scale CDE jobs start at 8 executor cores / 32 GB in total. They are submitted only when a read-only
  check shows headroom on the virtual cluster, outside sibling schedule windows and announced demo times (its
  20 CPU / 80 GB is shared). Resources may be increased when there is a need, within the rules and ceilings of
  rule 14.
- Models: gradient-boosted trees for the risk heads; a small open-weight LLM on CPU for grounded document
  summaries (Step 4, "Model choice").
- Target incident rate 8–10 % of matured packs (the broad battery-event label).
- The build runs in three milestones (Step 7), each finished before the next starts.
- The CAI and CDE quotas are shared with the sibling demos. Check their live state read-only before the first live
  step and whenever runs queue; ask before stopping anything that is not `evx`.

## Step 2. The business and its source systems (synthetic, seeded, labelled synthetic everywhere)

A fictional two-wheeler EV OEM; its display name comes from `EVX_BRAND_NAME`, with a neutral default. It runs:
- a **cell gigafactory**: 4680 cylindrical cells, dry electrode process, capacity scaling over phases toward multi-GWh;
- a **vehicle factory**: pack assembly with wire bonding, motor and frame lines, vehicle assembly and end-of-line
  (EOL) test, more than 100 robots, a 1M units/year nameplate;
- **connected scooters** in the field: telematics plus an on-board event data recorder (EDR) that buffers locally
  and syncs later;
- **service and warranty**.

Synthetic data must never be presented as the customer's real data.

| # | Source system (as the customer has it) | Synthetic generator output | Type, cadence |
|---|---|---|---|
| 1 | SAP S/4HANA Cloud ERP: materials, vendors, batches, BOMs, production orders, goods movements, QM inspection lots | OData-style JSON pages using SAP field names (`MATNR`, `CHARG`, `LIFNR`, `AUFNR`, `PRUEFLOS`, `BWART` ...): an initial full load, then daily delta pages with delta tokens. A new field appears mid-week (schema evolution) | structured, daily delta |
| 2 | Cell MES (gigafactory): one record per cell per process step (dry electrode mixing and film, calendering, winding, tab welding, can, electrolyte fill, formation, aging, grading) | shift exports as pipe-delimited CSV with trailer counts; formation curve **summaries for every cell** in the export; **full cycler curves for flagged cells plus a ~1 % sample, packed one CSV per cycler channel per shift** (not one file per cell) | structured + semi-structured, per shift |
| 3 | Inline QC (100 % sampling): X-ray / CT of each cell, vision camera on electrode film, spectrometer on incoming powder lots | **verdict, defect code and scores for every cell as rows** (MES export and the Kafka stream `evx.factory.qc`); **image files only for every flagged cell plus a ~1 % random sample** of passed cells: X-ray cross-section PNGs (jelly roll, overhang / misalignment defects), film PNGs with coating defects; spectra as JCAMP-DX-style text per powder lot | structured + unstructured + streaming |
| 4 | Pack line MES (Futurefactory): cells to modules to pack genealogy, wire-bond pull tests, pack EOL (capacity, insulation, BMS firmware) | JSON documents per pack | semi-structured |
| 5 | Vehicle assembly and EOL: VIN to pack, motor and MCU serials, EOL results | JSON per vehicle | semi-structured |
| 6 | Digital twin / virtual commissioning | twin scenario outputs (station cycle times, predicted bottlenecks per scenario) as JSON; actual station cycle events from the MES on Kafka `evx.factory.line` | semi-structured + streaming |
| 7 | Vehicle telematics | Kafka `evx.vehicle.telemetry`: SoC, pack V / I, min / max cell V, cell temperatures, motor / MCU temperature, speed, ride mode, firmware, coarse geohash. Fault codes go to `evx.vehicle.dtc` | streaming |
| 8 | Event data recorder | binary EDR dump files: header plus a 10 Hz pre/post-event buffer around a fault, thermal or crash trigger. Uploaded on sync, so they **arrive hours to days late** | unstructured, binary, late |
| 9 | Service DMS / CRM | rider master (PII), service job cards as scanned PDFs, warranty claims CSV, parts replaced | structured + unstructured |
| 10 | Supplier quality | certificates of analysis (PDF) per material batch; field-return teardown and 8D reports (PDF) | unstructured |

Plus `evx.control` (per-minute producer counts) for stream reconciliation, as OGX.

**Scale: realistic but configurable** (`--scale`). Pick default numbers in Phase 0 and print the distributions.
- **Initial load**: about **270 days** of factory and delivery history. Only deliveries at least 90 days before the
  end of history carry a matured label, which leaves about 180 days of labelled deliveries. Size the deliveries so
  the ML labels have **≥ 800 positive packs**, with ≥ 100 positives in every split (TEST included). Telematics
  history is pre-aggregated to daily summaries, as from a legacy store; DTC and EDR history exist as events over
  the whole period (the field-head backtest needs them).
- **Incident rate**: target **8–10 % of matured packs** with a battery-related field event within 90 days
  (`label.target_rate` in config). The label is broad (any thermal or imbalance DTC, battery warranty claim or EDR
  thermal trigger), so this is far above a true failure rate, and is stated as such in `docs/DATA_MODEL.md`. At
  about 150 cells per pack (from config) this gives about 11k packs and 1.7M cells at default scale. Phase 0 prints
  the realised rate, packs, cells and positives per split, and adjusts deliveries before freezing.
- The default scale represents a ramp-phase slice of the plant, not the 1M units/year nameplate.
- **File volume**: every cell gets rows (process steps, verdicts, scores, curve summaries); only flagged cells plus
  a ~1 % random sample get image and full-curve files. Phase 0 prints the expected object count per source, **for
  the initial load and per business date separately**. The initial load may be tens of thousands of objects (a 1 %
  sample of 1.7M cells is about 17k X-ray PNGs); keep the new S3 objects per business date in the low thousands,
  not millions.
- **Dates**: PLAN.md fixes the history end date and the 5 live business dates against the nightly DAG window
  before anything is generated. A stream date that has live data is never replayed (deterministic event ids would
  duplicate it).
- **Then 5 live business dates** of incremental batches and live streams for an active subset of about 2,000
  vehicles (one reading every 30 s while riding).
- **CI** at `--scale 0.05`, locally and in GitHub Actions.
- **Default scale runs directly on CDE.** Cell-level history (cells per pack × process steps × packs) is about
  15M+ rows, too slow for stdlib generators on the laptop. Generators run as a CDE Spark job that generates in
  partitions (seeded per partition, so the output is the same however it is split), and the default-scale
  end-to-end run is the CDE run, not a local one. The generator and backfill jobs start at 8 executor cores /
  32 GB in total (raised only under rule 14), and are submitted only when the virtual cluster has headroom, outside sibling schedule windows
  (their nightly DAGs run around 20:30 UTC and 01:30–02:30 UTC) and announced demo times.
- An optional `--scale 10` CDE run shows production volume; it needs approval because of cost.
- Cells per pack and cell nominal values are set in config from public specs and marked synthetic.

**Planted mechanisms (cause → effect), the signal the ML must find.** Generator truth is stored apart from features;
a test proves no feature reads it. **Every cause recurs across the history**, so that whatever the split, the model
can learn it and TEST can measure it:
- Cathode powder contamination: **4–6 contaminated supplier batches spread over the 270 days**. Each shows a
  spectrometer anomaly, slightly higher self-discharge (K-value) at aging, and more field self-discharge and thermal
  DTCs later.
- Calender roller drift: **several drift episodes on different shifts and weeks**. Film thickness drifts, then
  packs built from those cells show a wider capacity spread.
- X-ray overhang defects just under the QC threshold, at a low steady rate throughout. They pass inline QC but raise
  the field failure rate.
- A wire-bonding station with weak pull strength, **active in several periods** (for example between two
  maintenance events). It causes cell-imbalance faults in the field.
- Usage modifiers: fast-charging share, hot-climate geohashes, aggressive ride mode.

Because TEST is the latest window of matured deliveries (rule 5), **every cause has at least one episode whose
packs fall inside the TEST window**. A test proves that each cause has positives in both TRAIN and TEST of the
chosen split.

**Expected reconciliation.** `config/expected_recon.json` lists, for each planted fault, the date, layer, entity,
check and the expected status (MISMATCH or EXPLAINED). Recon results are compared with it: an unlisted MISMATCH
fails the run, and a listed fault that is not seen fails it too.

**Planted data faults (each with a test that it is caught where planted):**
- an SAP delta page re-sent;
- an MES CSV whose trailer is off by one;
- a corrupt X-ray PNG and a scanned job card with no text layer;
- a pack whose genealogy references a cell ID that never existed;
- a vehicle re-flashed with firmware that reports temperature in °F;
- a stuck SoC sensor;
- EDR dumps arriving 3 days late;
- a VIN typo between service DMS and EOL;
- a pack swapped between two vehicles mid-week (SCD2);
- clock skew on one station's line events.

## Step 3. Production-prototype lakehouse (beyond OGX)

**Data products, not just layers.** Each domain owns bronze → silver → gold tables, a contract, an owner, a
freshness SLA and a glossary term. Domains:
- `supply` (SAP materials, vendors, batches);
- `cell` (MES and QC);
- `pack_vehicle` (genealogy and EOL);
- `field` (telematics, DTC, EDR);
- `service` (job cards, claims, riders);
- `twin` (scenarios and line events).

`docs/DATA_PRODUCTS.md` lists each with consumers and SLA.

**Databases:**
- `rsingh_evx_{bronze,silver,master,gold,features,semantic,ref}`;
- `features` holds shared, versioned, point-in-time feature tables that any use case may reuse.

**The core asset is genealogy.** It is a conformed trace from supplier batch → material lot → cell → module → pack →
vehicle (VIN) → telematics, EDR events, service jobs and claims, kept as SCD2 where links change (pack-to-vehicle,
firmware).
- **Master data** (GDL pattern) gives golden `material_batch`, `cell`, `pack`, `vehicle` and `supplier` records,
  with xref from every source name (SAP batch, MES lot, pack serial, VIN, registration).
- **Gold** includes conformed dimensions (`dim_date`, `dim_supplier`, `dim_material_batch` SCD2, `dim_station`,
  `dim_cell`, `dim_pack` SCD2, `dim_vehicle` SCD2, `dim_rider` masked).
- **Gold facts:**
  - `fact_cell_process`, `fact_cell_grading`, `fact_qc_inspection`;
  - `fact_pack_eol`, `fact_vehicle_eol`;
  - `fact_telemetry_window` (15-minute), `fact_vehicle_daily`, `fact_dtc`, `fact_edr_event`;
  - `fact_service_job`, `fact_warranty_claim`;
  - `fact_line_cycle`, `fact_twin_scenario`;
  - `bridge_genealogy`.

**Production behaviour:**
- initial load, then idempotent daily increments (`overwritePartitions` per date), plus a backfill command;
- **late data**: an EDR dump or late telemetry re-opens the affected dates and recomputes only the gold aggregates
  it touches, logged in `ref.reprocess_log`;
- schema evolution from contracts (the SAP field added mid-week is accepted, recorded and surfaced);
- **Iceberg table maintenance**: CDE job `rsingh-evx-maintenance`, nightly. It compacts the streaming tables'
  small files, rewrites manifests, expires snapshots older than the retention in config, and removes orphan files.
  Sort orders and hidden partitioning are set per table;
- **write-audit-publish for gold**: Iceberg branch, then audit, then fast-forward. **The first live step after
  approval is a WAP feasibility spike** (CDE job `rsingh-evx-wap-spike` on a scratch table in `rsingh_evx_ref`).
  It tests on CDE Spark 3.5.4 / Iceberg 1.5.2:
  1. `ALTER TABLE ... CREATE BRANCH` and writes with `spark.wap.branch` (or `branch_<name>` writes);
  2. that readers of `main` do not see the branch;
  3. an audit query on the branch in Spark;
  4. `CALL system.fast_forward('<table>', 'main', '<branch>')` and `cherrypick_snapshot` as an alternative;
  5. Impala: a read of `main` after the fast-forward, and whether a branch can be read at all (`FOR SYSTEM_VERSION
     AS OF <snapshot id>`);
  6. Atlas lineage and the existing `overwritePartitions` per date on a branch.

  Write the results to `docs/WAP_SPIKE.md` and `docs/PROJECT_LOG.md`, choose one of these, record the decision in
  PLAN.md, and drop the scratch table:
  - (a) branch WAP with the audit in Spark;
  - (b) the WAP-ID staged-snapshot cherry-pick;
  - (c) staging tables plus an atomic swap.

  Gold jobs use one `publish()` helper from M1 (a plain `overwritePartitions` path until the spike's choice is
  built in M2), so the choice is made in one place and M2 changes only the helper;
- freshness and SLA per data product (`ref.freshness`, a DAG SLA-miss callback, a dashboard tile);
- failed-batch drill and re-run (GDL);
- reconciliation per layer and for streams;
- **privacy (India DPDP Act 2023 framing)**:
  - rider name, phone, address and precise location are classified;
  - location is coarsened to geohash-5 in silver;
  - Ranger masks for `federal01` / `federal07`;
  - retention set per table in config.

**Streaming (CDE Spark Structured Streaming, OGX's pattern):**
- Bronze jobs for `evx.vehicle.telemetry`, `evx.vehicle.dtc`, `evx.factory.qc` and `evx.factory.line`.
- An aggregation job for telemetry windows, and a line cycle-time window job.
- The producer runs as CDE job `rsingh-evx-stream-produce` (backfill and live modes).
- **NiFi**:
  - route thermal and critical DTCs to `evx.alert.priority`, kept out of the bronze subscriptions;
  - optionally, an HTTPS ingest endpoint (ListenHTTP) where synced EDR dumps land in S3 raw. Verify that it is
    reachable; otherwise the generator writes to landing.

**Orchestration:** one Airflow DAG per business date. Its stages run in this order:
1. land
2. bronze
3. extract
4. silver
5. master
6. gold (WAP)
7. features
8. **CAI scoring `evx-uc01-06`**
9. outcomes
10. recon
11. maintenance (its own schedule)

The DAG has a nightly window bounded by `start_date` / `end_date`. Manual runs take `params.business_date`.

## Step 4. Use case 1: pack field-risk from factory DNA

**Decision supported.** Two worklists:
- **(a) Vehicles** to call in for a proactive battery inspection, ranked by risk instead of waiting for claims.
- **(b) Material and supplier batches** to investigate, where packs built from them show elevated risk.

Each traces down to the cells, packs and vehicles affected. **Workflow prioritisation only.** No recall, no OTA
derate, no supplier rejection, no warranty decision: a quality engineer decides.

**Heads:**

| Head | Scores when | Features | Stage |
|---|---|---|---|
| `pack_risk_eol` | once, at vehicle EOL before dispatch (the VIN is known); pack EOL is the earliest its features are available | factory only: cell grading spread across the pack's cells (capacity, IR, K-value tails), formation anomalies, X-ray and film defect scores, wire-bond minimum pull, supplier and material batch quality, station and shift | **champion** |
| `pack_risk_field` | daily, after 14 days in the field | the factory features plus early telemetry (temperature exposure, imbalance trend, fast-charge share, DTC counts) and EDR events | **silent trial**, then promotion by `evx-uc01-07` with evidence and a named approver (go-live evidence below) |

- **Go-live evidence for `pack_risk_field`** (`models.pack_risk_field.go_live` in config). A 90-day label cannot
  mature within 5 live dates, so the evidence has two parts:
  - **(1) a backtest on matured history**: rolling-origin scoring at day 14 over deliveries with a matured 90-day
    label. It needs a minimum number of positives, a minimum sensitivity and specificity, and a minimum AUROC lift
    over `pack_risk_eol` on the same vehicles;
  - **(2) live stability over the trial dates (≥ 3)**: score PSI against the backtest within limits, abstain and
    OOD rates within limits, and rank agreement with the champion on the vehicles both score. Live field events
    that occur during the trial are reported as partial evidence, never as the main gate.

  `evx-uc01-07` writes the evidence to `gold.daily_model_summary` and `ref.model_event`. `PROMOTION_REFUSED`, with
  the failing checks listed, is a valid recorded outcome.

- **Field-head features are on the daily grain.** They come from `fact_vehicle_daily`, built by one function from
  both the legacy daily summaries (history, training and backtest) and the live stream's telemetry windows (live
  scoring). A test feeds the same synthetic day through both paths and gets identical features. EDR and DTC
  features come from `fact_edr_event` / `fact_dtc`, which exist over the whole history.
- **Field-head scoring population**: vehicles in the live subset (~2,000) with at least 14 days in the field and
  delivered no more than 90 days before the score date.

- **Label**: a battery-related field event within 90 days of delivery (thermal or imbalance DTC, battery warranty
  claim, EDR thermal trigger).
- Engineered tabular features, CPU only. Thresholds come from 5-fold out-of-fold scores. The KPI gate checks
  absolute values plus non-regression.
- **Point-in-time correctness**: every feature carries an `as_of` and a test proves no feature postdates the score
  time. The EOL head never sees post-dispatch data.
- **Batch-quality features come from factory and QC data only**: spectrometer results, QM inspection lots, the
  batch's cell grading and verdict statistics. A batch's field failure rate may be used only from outcomes that
  had matured before the score time. A dedicated leakage test builds the features for a pack and proves that no
  value changes when the field outcomes of other packs in the same batch **that mature after the pack's score
  time** are removed.
- A pinned public battery dataset (Phase 0 picks it and checks its licence; candidates: NASA PCoE, Severson /
  Attia) calibrates the generator's degradation model. It is not presented as customer data.

**Model choice (open source; ML where the data is tabular, an LLM where it is text).** There is no GPU, the CAI
quota is shared, and the decisions need calibrated, explainable scores. So:

| Task | Choice | Why | Not chosen |
|---|---|---|---|
| `pack_risk_eol`, `pack_risk_field` | **LightGBM** (MIT) as the main candidate; scikit-learn logistic regression as the baseline and `HistGradientBoosting` as the fallback. The winner is picked by grouped out-of-fold **PR-AUC** (positives are 8–10 %) with AUROC reported; probabilities are calibrated (isotonic) on the out-of-fold scores | best accuracy per CPU-second on tabular data; per-score explanations from LightGBM's built-in `pred_contrib` (TreeSHAP values, no extra dependency) | LLMs for tabular scoring (weaker, slower, hard to calibrate); tabular foundation models such as TabICL (2–16 s per call on CPU in the sibling repos) |
| Formation anomaly features | scikit-learn **Isolation Forest** on formation-curve summaries, fitted on TRAIN batches only | unsupervised, fast, point-in-time safe when fitted on TRAIN | deep sequence models (GPU) |
| Grounded summaries of 8D and teardown reports (Batch containment, Pack 360) | a **small open-weight instruct LLM (≤ 4B parameters, Apache-2.0 or MIT licence, for example the Qwen or Phi families)**, quantised (GGUF) and run on CPU with `llama-cpp-python`. Retrieval over the document's own extracted pages, with a small open embedding model (for example `bge-small-en-v1.5`, MIT) when needed | text is where an LLM adds value; a small quantised model fits a CAI job on CPU | hosted or closed models; models over 4B (too slow on CPU); Cloudera AI Inference, unless Phase 0 finds it running on CPU in this environment |

Rules for the LLM:
- **It never scores, ranks or decides.** Its output is a labelled summary beside the evidence.
- Summaries are **precomputed by a nightly CAI job** (`evx-uc01-09-summarise`, only for new or changed documents)
  into `gold.doc_summary`, with the model name, revision and prompt hash. The app reads the table and never calls
  the LLM interactively.
- The LLM guardrail (OGX's planned `llm_guard`): every sentence cites a page of the source PDF, or the summary is
  refused; rider PII and `EVX_SENSITIVE_*` columns are never sent to it; refusals go to `ref.guardrail_event`.
- Phase 0 picks the exact model, checks its licence, pins the revision and measures tokens per second and memory
  at 2 vCPU / 8 GB. The demo never depends on it: with `llm.enabled: false`, the app shows the extracted text.

**Guardrails** (OGX's set, adapted), all logged to `ref.guardrail_event`:
- Input:
  - contracts and quarantine;
  - genealogy completeness: a pack with a missing cell grading record abstains;
  - sensor sanity: stuck SoC, °F / °C, too few readings;
  - OOD distance;
  - unresolved identity goes to the review queue.
- Output:
  - abstain band;
  - **safety floor**: any EDR thermal trigger or thermal DTC makes the vehicle P1, whatever the score;
  - no automated action.
- Process:
  - KPI gate and non-regression;
  - feature hash lock;
  - drift (PSI, `TOO_FEW` below 30);
  - silent trial plus named approver.
- LLM (M3): grounding with page citations, PII redaction, refusal logged; the LLM never scores or decides.

**Certified KPIs** (Impala views plus a consistency check):
- **FPY**: first-pass yield, cell and pack, by line, station, shift and day.
- **FIR90**: battery-related field incidents per 1,000 vehicles within 90 days, by build week and supplier batch.
- **TTC**: time to containment, days from the first field signal to the batch being flagged. Risk-ranked vs the
  claim-driven baseline, on the same events. Vehicles raised to P1 by the safety floor are reported as their own
  group and are not credited to the model in TTC or in the risk-vs-claim worklist comparison.

**App: Quality & Reliability Workbench** (Streamlit on CAI, brand from `EVX_BRAND_NAME`). Tabs:
- **Vehicle risk worklist**: EOL and field heads, risk vs claim order.
- **Batch containment**: supplier batch → cells → packs → vehicles, with risk and an explanation.
- **Pack 360**:
  - genealogy tree;
  - cell grading spread;
  - X-ray images with the defect box;
  - EDR event plot;
  - telemetry trend;
  - claims.
- **Line & twin**: actual vs twin cycle times; read only.
- **Data products & freshness**.
- **Models & guardrails**.
- **Engineer feedback**: written to Iceberg `gold.review_feedback` from day one; not CSV files.

**Dashboards (CDV, as code):**
- EVX Factory Quality & Yield
- EVX Traceability & Containment
- EVX Fleet Reliability
- EVX Streaming & Freshness
- EVX Models, Drift & Guardrails
- EVX Data Estate

## Step 5. The backbone for future EV use cases

**Core and use cases are separate.**
- The **core** is generators and sources, the data-product domains (bronze → silver → master → gold), the shared
  `features` tables, the semantic layer, recon, maintenance, governance and operations. It knows no use case.
- A **use case** reads only `gold`, `features` and `semantic`.
  - Its outputs go to tables prefixed `ucNN_` in `gold`.
  - It also writes to the shared tables (`gold.model_score`, `gold.review_feedback`, `gold.daily_model_summary`,
    `ref.model_event`, `ref.guardrail_event`), each of which has a `use_case` column.
  - It never writes core tables.

**Contracts that later consumers can rely on.**
- Every data product has a versioned contract (`contracts/products/<domain>.<table>.json`, semver):
  - additive changes are minor;
  - a removed or retyped column is major and needs a deprecation period, so the old column stays for one release.
- `config/consumers.json` records which use case reads which tables and columns. A CI test fails when a core
  change breaks a registered consumer.
- `docs/DATA_PRODUCTS.md` is generated from the contracts and the consumer registry.

**Config-driven wiring, so a new use case or data product edits no core code.**
- The DAG builds its feature and scoring tasks from every `usecases/*/config.yaml` with `enabled: true`.
- The app discovers use-case pages, and `dataviz/` builds use-case dashboards from each use case's spec.
- Data Visualization keys: core 15000–15999; each use case gets a 500-key block from 16000 (uc01 16000, uc02 16500,
  and so on).
- A new source or domain is a generator module under `generators/sources/` plus a contract and a config entry.
  `docs/ADDING_A_DATA_PRODUCT.md` walks through it, and the DAG and recon pick the new domain up from config.

**Shared ML platform.**
- Shared ML platform code lives in `mlcore/`:
  - feature hash and feature-table builder;
  - KPI gate;
  - registry;
  - deploy with rollback;
  - drift;
  - guardrails;
  - silent trial and promotion.

  It is copied from OGX / CXR and generalised. Each use case lives in `usecases/ucNN_<name>/`:
  - `config.yaml`;
  - `features.py`, which reads only `gold` / `features`;
  - its own train / gate / serve entries;
  - its app page and dashboard spec;
  - a `README.md`.

  CAI jobs are named `evx-ucNN-00..09` (09 = optional LLM summaries); registry models `evx-ucNN-<head>`. All use
  cases share the one CAI project.
- `usecases/_template/` plus `scripts/new_usecase.py ucNN <name>` scaffold a new use case that passes tests on day one.
- `docs/ADDING_A_USE_CASE.md`: step by step, with what never changes in the core.
- `docs/USE_CASE_BACKLOG.md`: candidates, each with the gold / feature tables it needs and a **data-readiness SQL
  check** that runs in `run_semantic.py --steps readiness` and must pass now. Candidates:
  - uc02: shorten formation and aging, by predicting the final grade from early formation cycles;
  - uc03: supplier quality scorecard and incoming inspection from spectra;
  - uc04: line bottlenecks and OEE, digital twin vs actual;
  - uc05: warranty claims anomaly and cost forecast;
  - uc06: field SoH and range by firmware cohort (OTA impact);
  - uc07: GenAI service assistant over job cards and 8D reports, using RAG on CPU (it reuses the M3 LLM stack);
  - uc08: service-centre spare-parts demand forecast from claims, job cards and the field fleet;
  - uc09: predictive maintenance of line robots and stations from line cycle events;
  - uc10: charging behaviour and battery-ageing cohorts (fast-charge share, climate, ride mode) for warranty
    reserve planning.

  Each backlog entry says which data products it consumes and whether they exist now. If something is missing,
  it names the core change it would need (a new column, product or source).
- Prove it: scaffold **uc02** from the template, with its readiness check, feature view and a baseline model
  trained on a default-scale extract (or as a CAI job), not on CI-scale data. Do not deploy it.

## Step 6. Design rules learned from OGX: use from the first commit

1. **Standard library first.** Generators, writers, parsers and CDE job code use only the standard library plus
   PySpark, so CDE needs no python-env. This covers PDF, PNG, JCAMP-DX, the binary EDR format, CSV and OData JSON.
   Copy OGX's `generators/formats.py`, `extract/pdf_text.py`, `extract/ocr.py`, `extract/font5x7.py`.
2. **Kafka work runs from CDE or through the SMM REST API**; broker port 9093 is closed from the laptop. Keep
   derived topics out of bronze subscriptions.
3. **Streaming, OGX's way:**
   - trigger `"60 seconds"`;
   - 10-minute watermark (longer for DTCs, set per topic in config);
   - exactly-once batch markers on Iceberg commits;
   - MERGE through foreachBatch;
   - long-running for demos, available-now inside the nightly DAG;
   - the maintenance job compacts the small files the streams create.
4. **Calibrate every rule, label and threshold against the generated data before freezing it.** Print the
   distribution, choose the value, then test that the planted cause fires and clean data does not. OGX's
   stuck-sensor rule never fired, one limit made every window breach, and a severity label gave 4 positives in
   1,249.
5. **Thresholds from 5-fold out-of-fold scores**, never VAL alone or TEST. At least about 100 positives per class
   in each split (hence ≥ 800 positive packs at default scale). The split is **time-ordered and grouped by cell
   production batch**:
   - order the cell batches by the last delivery date of the packs built from them;
   - TEST is the latest batches, covering the latest matured deliveries;
   - TRAIN is the rest, and the 5 out-of-fold folds inside it are grouped by batch too (no batch in two folds).

   Packs built from one batch never straddle TRAIN and TEST, or two folds. A pack with cells from several batches
   goes with its majority batch; a test checks the share of mixed packs.
   **CI**: `config/ci.yaml` relaxes the gate minimums so the chain passes at CI scale, and gives CI its own feature
   hash, so a CI-trained model can never pass the real gate or be deployed (CXR's overlay pattern).
6. **CAI:**
   - the model function is wrapped in `cml.models_v1.cml_model`;
   - default sizes: jobs 1 vCPU / 4 GB, model 1 / 2, app 2 / 8, the LLM summary job 2 / 8 (rule 14 for increases);
   - register the AI Registry version before the deploy, idempotent per MLflow run;
   - replace a build that has not finished in 600 s (up to 3 tries);
   - the workbench home state is git-ignored, the serving folder is not (use `.git/info/exclude`);
   - `sys.exit` only on failure;
   - a `_repo_root()` fallback and `parse_known_args()`;
   - run parameters passed as environment variables.
7. **App:**
   - a supervising launcher;
   - one Impala connection per session thread, with reconnect;
   - cached scores;
   - score in process from the champion files; verify the endpoint separately.
8. **Impala:**
   - no reserved words as names (`format`, `method`, `change`, `matched`);
   - Impala-written ref tables use `timestamp_ntz` in UTC;
   - the lineage column is `_source_file`.
9. **Airflow:**
   - `max_active_runs=1`, `dagrun_timeout`, a per-task `execution_timeout`;
   - redeploy only with no run in flight;
   - the virtual cluster quota is shared: never leave runs hanging.
10. **Data Visualization primary keys.** Check every sibling's `dataviz/` for the highest key used and take a
    free block: **15000+** (`loan-approval` holds 14000–14505, OGX 13000–13507). Re-check before the first import.
11. **Waits and naming:**
    - bounded foreground polls only, no `nohup`;
    - re-read live state after any gap;
    - no module named after a stdlib one;
    - no `status` variable in zsh.
12. **Operations from Phase 2:**
    - `scripts/platform_ctl.py` + `config/platform.json`, project scope by default;
    - `.cursor/rules/project-start-stop.mdc`;
    - CAI self-stop job `stop-evx-project`;
    - CAI schedules stop by clearing the cron;
    - stop the project at the end of every live session unless I say otherwise;
    - before the first live step, before every default-scale CDE submission, and whenever a CAI job sits in
      scheduling or a CDE run sits in starting, check read-only what else holds the shared quota (OGX and the other
      siblings: apps, model deployments, schedules, runs). Report it and ask before stopping anything that is not
      `evx`.
13. **Secrets and the customer name:**
    - never print `.env`, environment variables or run `environment` fields;
    - gitleaks pre-commit, plus a custom pre-commit check that blocks the value of `EVX_BRAND_NAME` from any
      tracked file or commit message;
    - `docs/private/` is git-ignored.
14. **Increasing Cloudera resources when there is a need** (pre-approved up to the ceilings in the approval list):
    - Start small: the default sizes in rule 6 and Step 2, which schedule at once on this environment.
    - A need is evidence, not a guess. Examples: a run sits in scheduling or starting past the bounded wait while
      the shared quota is full; a job runs out of memory; or a stage misses its freshness SLA.
    - **Add capacity before you enlarge a request.** Bigger requests often wait longer (2 vCPU / 8 GB CAI jobs sat
      for 37 min; 4 vCPU never left scheduling). So raise the CDE virtual cluster's maximum, the CAI CPU instance
      group's maximum nodes or the Impala warehouse's executors first. Only then raise `evx` executor or engine
      sizes, one step at a time.
    - **Never resize a CAI instance group while any engine runs** (it can kill them, `reference.md`). Resize a
      shared service only when no sibling run is in flight, and say so in the log.
    - Record every change before and after in `docs/PROJECT_LOG.md` and `config/platform.json` (`resources`):
      what, from, to, why, when. Scale back when the need ends, or at the end of the live session, unless I say
      keep it.
    - If a change is outside your permissions (for example the CAI per-user quota), report it with the exact
      setting and who can change it, and continue with what fits.

## Step 7. Scope

**Milestones.** Finish each one, including its commit, push, live check and PROJECT_LOG entry, before starting the
next. At the end of M1 there is a demo, whatever happens after.

| Milestone | Contents | Done when |
|---|---|---|
| **M1: demo-ready core** | the WAP spike; the `supply`, `cell` and `pack_vehicle` domains plus field and service history (DTC and EDR history from batch); genealogy, master data and gold, written through `publish()`; telemetry streaming (one topic, `evx.vehicle.telemetry`); the `pack_risk_eol` champion (features, train, gate, registry, deploy) with the **M1 minimum guardrails** (contracts and quarantine, genealogy completeness, OOD, abstain band, the safety floor reading DTCs from batch history, no automated action, KPI gate, feature hash lock); the scoring job in the DAG; the app tabs Vehicle risk worklist, Batch containment and Pack 360; the dashboards Factory Quality & Yield, Traceability & Containment and Fleet Reliability; FPY and FIR90; `platform_ctl.py` | a 20-minute demo runs end to end on CDE data at default scale |
| **M2: production behaviour** | the other three streams and NiFi; late-data reprocessing; schema evolution; table maintenance; WAP (or the fallback) in every gold job; freshness SLAs; the failed-batch drill; the `pack_risk_field` silent trial and `evx-uc01-07`; the outcomes job and TTC; the `twin` domain; the remaining app tabs and dashboards; guardrails complete | the data and ML items of the definition of done pass |
| **M3: extensibility and docs** | `mlcore/` refactor if not done in M1, `usecases/_template/`, `new_usecase.py`, the uc02 scaffold and readiness checks; versioned product contracts, the consumer registry and its CI test, `ADDING_A_DATA_PRODUCT.md`; the LLM summaries (`evx-uc01-09-summarise`, `gold.doc_summary`, the LLM guardrail); the skill enhancement (Step 10); the SQL files; all docs; governance verified as masked and clear users; runbook rehearsal; `PLAN_REVISED.md` | the whole definition of done passes |

Build M1 inside the final structure (`usecases/uc01_pack_risk/`, shared code in `mlcore/`) so M3 does not have to
move it.

**In scope**, each with a phase line and tests:
- everything in Steps 3–5;
- the items OGX left unbuilt:
  - the lakehouse scoring job called from the DAG;
  - the outcomes job;
  - the silent trial and promotion;
  - per-score explanations;
  - `sql/adhoc.sql`, `sql/unstructured_search.sql`, `sql/time_travel.sql` (for example, a pack's risk before and
    after an EDR dump arrives late);
  - docs `UNSTRUCTURED.md`, `STREAMING.md`, `DATA_MODEL.md`, `DATA_PRODUCTS.md`, `GOVERNANCE.md`,
    `FAILED_BATCH_DEMO.md`, `PRESENTER_RUNBOOK.md`, `ADDING_A_USE_CASE.md`, `USE_CASE_BACKLOG.md`.

**Optional, off by default:**
- vector search beyond a single document's pages (it belongs to uc07);
- the `--scale 10` run;
- the NiFi EDR ingest endpoint, if it is not reachable.

Flink/SSB is documented as the sub-second alternative, not built. Direct PLC or OPC-UA integration is shown as a
future-state roadmap item: line events come from the MES. If time runs short, cut from the optional list first, and
never from the in-scope list without asking me.

## Step 8. First deliverable: plan, credentials, one approval list. Nothing live yet

1. **`PLAN.md`** from the skill's skeleton, with these sections:
   - story and decision;
   - scale;
   - requirements map (numbered, each to a file or service);
   - platform mapping;
   - data products;
   - names;
   - sources and planted causes and faults;
   - genealogy and gold model;
   - production behaviour;
   - use case 1 (heads, label, features, guardrails, KPIs, app, dashboards);
   - the extensibility design;
   - phases 0–12 plus a "use-case 2 scaffold" phase, each assigned to milestone M1, M2 or M3 (Step 7);
   - the expected row and S3 object counts per source at default scale, for the initial load and per business
     date;
   - the incident rate, packs, cells and expected positives per split;
   - the history end date and the 5 live business dates, aligned with the nightly DAG window;
   - the expected-recon table (one row per planted fault);
   - the model choice per task (Step 4), with the LLM picked, its licence and measured CPU speed;
   - resources: default sizes, ceilings and the triggers for each increase (rule 14);
   - the core / use-case boundary and the consumer registry (Step 5);
   - decisions and open items.

   Names:
   - databases `rsingh_evx_*`;
   - CDE jobs `rsingh-evx-*`;
   - CAI project `rsingh-ev-factory-to-field`, jobs `evx-uc01-00..09`, model `evx-uc01-service`;
   - registry models `evx-uc01-pack-risk-eol`, `evx-uc01-pack-risk-field`;
   - topics `evx.*`;
   - NiFi group `EVX - Alert Router` and bucket `evx-flows`;
   - Atlas `EVX_SENSITIVE_*`, Ranger `rsingh-evx-*`;
   - landing `s3a://federal-buk-574bcea0/data/IB/rsingh_evx/...`.

   Mark every change from this prompt with its reason.
2. **`docs/REUSE_INVENTORY.md`**: start from OGX's inventory. Mark each row "verified read-only <date>" or "to verify".
   Add a row per copied file. No secret values.
3. **`.env`**: values from OGX's `.env`, renamed `OGX_*` → `EVX_*`, plus `EVX_BRAND_NAME`. Before the first commit,
   set up:
   - `.gitignore`, ignoring `docs/private/`, `.idea/`, and `.cursor/*` except `.cursor/rules/`;
   - unstage `.idea/` and the OGX build-plan `.docx`; move the `.docx` to `docs/private/` (reference only, untracked);
   - keep only this prompt (`EV_CURSOR_PROMPT.md`, v3.3) in the repo; earlier versions go to `docs/private/`;
   - `.env.example` with placeholders;
   - the hooks from rule 13.
4. **One message titled `APPROVALS NEEDED`, covering the whole project (M1, M2 and M3) in one go**, in the skill's
   approval-list format, with exact names and sizes. Anything foreseeable in any milestone goes on it now, not
   later:
   - Git;
   - CAI: project, env var names, jobs (`evx-uc01-00..09`, the uc02 baseline job, `stop-evx-project`), model, app,
     registry models;
   - CDE: resource, batch jobs, streaming jobs, maintenance job, DAG with its window, the WAP spike job (scratch
     table created and dropped in `rsingh_evx_ref`), the default-scale generator and backfill runs with their
     starting executor sizes (8 cores / 32 GB in total; increases under the resource item below) and the
     submission rule (headroom, outside sibling schedule
     windows and demo times);
   - Kafka: topics with partitions and retention, schemas;
   - NiFi;
   - CDW: databases, first write, views;
   - CDV: dashboards, key range 15000+;
   - SDX: classifications, glossary per data product, Ranger;
   - the public dataset, with name, revision and licence;
   - models and packages to download: LightGBM and scikit-learn versions, the LLM (name, revision, licence,
     quantisation, size on disk), the embedding model if used;
   - **resource increases (rule 14)**, each with its default, its ceiling and the evidence that triggers it:
     - `evx` CDE executors and drivers;
     - `evx` CAI job, model and app sizes;
     - the CDE virtual cluster's maximum quota;
     - the CAI CPU instance group's maximum nodes;
     - the Impala warehouse's executor count or autoscale maximum;
     - Kafka topic partitions;
     - the expected cost per day at each ceiling;
   - **`evx`-scoped destructive operations during the build**:
     - drop and recreate `rsingh_evx_*` tables and the scratch table;
     - roll back an `evx` table to an earlier snapshot;
     - kill `evx` runs;
     - delete retired `evx` CDE jobs, CAI jobs, model builds and deployments;
     - clear `evx` Kafka topics and checkpoints for a re-run;
   - ops: project-scope stop / start; read-only status checks of sibling projects' CAI and CDE workloads;
   - the sibling edits allowed: the **skill enhancement** (Step 10) in OGX's `tools/cursor-skills/`, as its own
     commits pushed to OGX. No other OGX file is touched.

   Put every open question in the same message.

I answer once. After that, do not ask again for anything on the list, in any milestone. Ask only for anything that
is:
- not on the list and not foreseeable when it was written;
- above a ceiling on the list;
- destructive outside the `evx` prefixes, or a force push / history rewrite of any repo;
- touching shared services at platform scope (stop / start of Data Hubs, the warehouse, the virtual cluster or the
  workspace);
- touching any sibling demo's resources (other than the skill files above).

## Step 9. Build

- Follow the phases in the skill and PLAN.md. Copy the OGX counterpart first (CXR / GDL for their parts), then
  adapt.
- **Local first, at CI scale:**
  - `pytest -q`;
  - `scripts/run_local.py all` (Spark 4, Iceberg, SQLite catalog, local Kafka) at `--scale 0.05`.
- **Default scale on CDE directly.** No local default-scale run. Calibration (rule 4) at default scale reads the
  distributions from the CDE run's tables. Re-check at CDE scale any rule or threshold calibrated at CI scale.
- First live step after approval: the shared-capacity check (rule 12), then the WAP spike (Step 3).
- Then go live one service at a time, reading status back.
- One commit per phase, then push.
- A `docs/PROJECT_LOG.md` entry after every live step: run ids, counts, recon, plan changes, no secrets.
- Add new platform lessons to the skill's `reference.md` (symptom → cause → fix) in the same session.
- One status line per phase. Full detail only when blocked. If something does not fit, park it with options and
  continue the rest of the phase.

## Step 10. Enhance the skill

The skill (`O-G-Asset-Integrity-Lakehouse/tools/cursor-skills/cloudera-usecase-builder/`, symlinked into
`~/.cursor/skills/`) was written for one-lakehouse, one-use-case demos. Bring it up to the production-prototype and
backbone pattern this project proves, so the next build, or the next use case on this lakehouse, starts from it.
Keep it generic: `evx` is a reference implementation in its table, not its subject.

- **`SKILL.md`**:
  - add EVX to the reference-implementations table;
  - two entry modes: "new lakehouse" and "new use case on an existing lakehouse" (the second follows
    `ADDING_A_USE_CASE.md`);
  - the workflow gains the pre-work review of the prompt, milestones, the one-time approval list with ceilings, the
    WAP spike and the shared-capacity check;
  - platform essentials gain the resource-increase rules (rule 14) and the model-choice rule (ML for tabular, a
    small CPU LLM only for text, never for scores or decisions).
- **`templates.md`**: the PLAN.md skeleton with milestones; a data-product contract; `consumers.json`;
  `expected_recon.json`; the `usecases/_template/` layout; `config/platform.json` with `resources`; the approval
  list with ceilings and `evx`-style scoped destructive operations.
- **`reference.md`**: every new symptom → cause → fix from this build, added as it happens.
- **New `lakehouse-backbone.md`**:
  - data products and versioned contracts;
  - the core / use-case boundary;
  - config-driven DAG, app and dashboards;
  - point-in-time features;
  - batch-grouped time splits;
  - planted-cause recurrence;
  - incremental loads, late data and WAP;
  - table maintenance;
  - freshness SLAs.
- Update OGX's `docs/USE_CASE_PLAYBOOK.md` only where it describes the skill.

Commit the skill in OGX as its own commits: lessons as they come, the structural update at the end of M1 and at
the end of M3. Push them, and check that the symlinked skill loads.

## Definition of done

**Data:**
- Initial load plus 5 business dates run end to end through the DAG, including scoring, outcomes, recon and
  maintenance.
- No unexpected recon MISMATCH: every MISMATCH or EXPLAINED result matches a row of `config/expected_recon.json`,
  and every row there is seen. The KPI consistency check is all MATCHED.
- Late EDR dumps reprocess only the affected dates.
- Every planted fault and every planted cause is caught where planted, each with a test and a visible place in the
  app or a dashboard.
- Freshness SLAs show green / amber / red correctly.

**ML:**
- `pack_risk_eol` is champion: registered, gated and deployed.
- `pack_risk_field` has run a silent trial over at least 3 dates, with one promotion attempt recorded and its
  evidence (backtest plus live stability). `PROMOTED` and `PROMOTION_REFUSED` both count.
- The default-scale run on CDE has ≥ 800 positive packs and ≥ 100 positives in every split, and every planted
  cause has positives in both TRAIN and TEST.
- The realised incident rate is within the configured target (8–10 %).
- The point-in-time, no-leakage, batch-feature leakage and field-grain parity tests pass.
- The default-scale S3 object counts (initial load and per business date) match the Phase 0 estimates.

**Extensibility (backbone):**
- uc02 is scaffolded from the template, and its readiness check passes.
- Adding uc02 changed no core file (checked by `git diff` against the core paths).
- The consumer-registry CI test fails on a deliberately breaking contract change and passes on an additive one.
- Every backlog entry has its readiness result.

**LLM:** `gold.doc_summary` holds grounded summaries for the 8D and teardown reports, every sentence cited. The
guardrail refuses an uncited summary in a test. The app works with `llm.enabled: false`.

**Resources:** every increase is in `docs/PROJECT_LOG.md` and `config/platform.json` with its reason, and is
scaled back or kept on my say.

**Skill:** SKILL.md, templates.md, reference.md and lakehouse-backbone.md are updated, committed and pushed in OGX.

**Platform decisions:** `docs/WAP_SPIKE.md` records what CDE and Impala support, and the gold publish path in use.

**Delivery:**
- The app and 6 dashboards are live.
- Masking is checked as a masked user and as a clear user.
- `docs/DEMO_RUNBOOK.md` is rehearsed once, with timings.
- `platform_ctl.py stop` leaves nothing of `evx` running.
- `docs/PLAN_REVISED.md` is written in OGX's format: a change map with [ADDED] / [CHANGED] / [NOT BUILT] against
  this prompt.

Start with Step 1 now.
