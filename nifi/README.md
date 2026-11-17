# NiFi: EVX - Alert Router

Process group **EVX - Alert Router** on the `federal-nifi` Data Hub (NiFi 2.6, Flow Management Light
Duty), built as code by `nifi/deploy_flow.py` and versioned in **NiFi Registry** (registry client
"Default NiFi Registry Client", bucket `evx-flows`, flow `evx_alert_router`). The flow definition is
committed at `nifi/flows/evx_alert_router.json`, with every sensitive value removed.

```
ConsumeKafka evx.vehicle.dtc ──► QueryRecord ──thermal──┐
 group evx-alert-router           │ ──critical─┴─► UpdateRecord ──► PublishKafka evx.alert.priority
 JSON, DTC reader schema          │ (other DTCs:     routed_at       key = vin, alert shape
                                  │  dropped)        (NiFi time)
 parse / query / update / publish failure ──► "Hold failures (inspect, then replay)"  (stays stopped)
```

## What it does

Field DTCs arrive on `evx.vehicle.dtc` (synthetic, from `rsingh-evx-stream-produce`). The router
publishes a P1 alert to `evx.alert.priority` for every DTC that is:

| Relationship | Rule | `reason` |
|---|---|---|
| `thermal` | `dtc_code` has catalog `kind` `THERMAL` in `config/dtc_catalog.json` (today `BMS-T01`, `MCU-T10`; read at deploy time) | `THERMAL` |
| `critical` | `severity = 'CRITICAL'` and not thermal | `CRITICAL` |

THERMAL wins when both apply, so each DTC leaves on one relationship at most. Everything else is
dropped by QueryRecord (its `original` relationship is auto-terminated); a dropped DTC is not an
error. The alert carries `alert_id` (= DTC `event_id`), `vin`, `event_ts`, `dtc_code`, `severity`,
`reason`, `priority` (`P1`), `source_topic` (`evx.vehicle.dtc`) and `routed_at` (NiFi time, ISO UTC).
No bronze stream subscribes to `evx.alert.priority`: derived copies are never ingested twice.

Every failure queues in front of **Hold failures**, an UpdateAttribute that is never started, so a
failed record stays visible on the canvas until someone inspects it and replays it.

| Piece | Component |
|---|---|
| Kafka | `Kafka3ConnectionService`, SASL_SSL PLAIN with the workload user; SSL context = the existing root service **rsingh Truststore-only NiFi SSL Context Service**, looked up by name |
| Reader / writers | `JsonTreeReader` with fixed schemas (the five DTC fields the router reads; the alert shape), `JsonRecordSetWriter` one JSON object per line |
| Routing | `QueryRecord` (two SQL relationships), `UpdateRecord` for `routed_at` |
| Publish | `PublishKafka` with record reader / writer, message key `vin` |

## Commands

```bash
set -a; source .env; set +a                        # EVX_WORKLOAD_USER / EVX_WORKLOAD_PASSWORD / EVX_KAFKA_BOOTSTRAP
python nifi/deploy_flow.py status                  # counts, per-processor state, registry version
python nifi/deploy_flow.py validate                # read-only: every property against NiFi's definitions
python nifi/deploy_flow.py deploy --dry-run        # the plan: GETs only, every POST / PUT printed
python nifi/deploy_flow.py deploy                  # group + services (enabled) + processors (STOPPED)
python nifi/deploy_flow.py version -m "initial"    # creates bucket evx-flows if absent, commits
python nifi/deploy_flow.py export                  # registry snapshot -> nifi/flows/evx_alert_router.json
python -m pytest -q tests/test_nifi_export.py      # the exported file passes the security checks
python nifi/deploy_flow.py start                   # every processor except Hold*
python nifi/deploy_flow.py stop                    # services stay enabled, queues and offsets kept
python nifi/deploy_flow.py export --offline        # render the file from flow_spec() without NiFi
```

Every write command takes `--dry-run`. `deploy` refuses when the group exists (replacing it is a
delete and needs approval). `scripts/platform_ctl.py` stops and starts the group with the other
project layers (`--only nifi`), and keeps Hold* stopped as well.

Prerequisites: topics `evx.vehicle.dtc` and `evx.alert.priority` exist (created by the producer's
`--setup` on CDE), and the workload user may read the first, write the second and use consumer group
`evx-alert-router`.

## Security posture

- No credentials in the repo or in the flow file: the workload user and password are read from the
  environment at deploy time, sent once as NiFi properties (the password is a sensitive property),
  and never printed or logged; error texts are redacted. The export nulls every sensitive property
  and writes `${EVX_KAFKA_BOOTSTRAP}` / `${EVX_WORKLOAD_USER}` placeholders.
- The truststore is reused unmodified: the root SSL context service is referenced by id (an external
  controller service in the export), never copied, edited or re-created.
- No CDP access keys, no `CdpCredentialsProviderControllerService`, no object-store writer.
- No listener and no new port: no ListenHTTP / HandleHttpRequest. The optional EDR HTTPS ingest
  endpoint (PLAN open item 8) is not built; the generator keeps writing EDR dumps to landing.
- Only the EVX group, its own five controller services and the `evx-flows` bucket are created.
