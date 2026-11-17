"""publish(): direct mode as M1, branch WAP (docs/WAP_SPIKE.md decision (a)) on local Spark + Iceberg."""
import os
import shutil
import subprocess
import sys
from datetime import date
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import evx_common as C  # noqa: E402
import publish as P  # noqa: E402

D1, D2 = "2026-01-01", "2026-01-02"


class FakeConf:
    def __init__(self, values):
        self.values = values

    def get(self, key, default=None):
        return self.values.get(key, default)


class FakeSpark:
    def __init__(self, **conf):
        self.conf = FakeConf(conf)


def test_mode_resolution_order(monkeypatch):
    monkeypatch.delenv(P.ENV_KEY, raising=False)
    assert P.publish_mode(FakeSpark(), {}) == "direct"
    assert P.publish_mode(FakeSpark(), {"publish": {"mode": "wap"}}) == "wap"
    monkeypatch.setenv(P.ENV_KEY, "direct")
    assert P.publish_mode(FakeSpark(), {"publish": {"mode": "wap"}}) == "direct"
    assert P.publish_mode(FakeSpark(**{P.CONF_KEY: "WAP"}), {"publish": {"mode": "direct"}}) == "wap"
    monkeypatch.setenv(P.ENV_KEY, " ")
    assert P.publish_mode(FakeSpark(**{P.CONF_KEY: ""}), {}) == "direct"
    with pytest.raises(ValueError):
        P.publish_mode(FakeSpark(**{P.CONF_KEY: "branch"}), {})
    monkeypatch.setenv(P.ENV_KEY, "audit")
    with pytest.raises(ValueError):
        P.publish_mode(FakeSpark(), {})


def test_default_mode_is_direct_in_the_shipped_config(monkeypatch):
    monkeypatch.delenv(P.ENV_KEY, raising=False)
    assert P.publish_mode(FakeSpark()) == "direct"


def test_branch_name_is_sanitised():
    class A:
        run_id, pipeline_run = "build_gold-3F9a.b c", "x"
    assert P.branch_name(A) == "wap_build_gold_3f9a_b_c"
    A.run_id = "manual__2026-11-09T23:30:00+00:00"
    assert P.NAME.match(P.branch_name(A))
    A.run_id = "---"
    with pytest.raises(P.PublishError):
        P.branch_name(A)


def test_impala_branch_sql_validates():
    assert P.impala_branch_sql("rsingh_evx_gold.fact_dtc", 123) == \
        "SELECT * FROM rsingh_evx_gold.fact_dtc FOR SYSTEM_VERSION AS OF 123"
    for table, snap in (("rsingh_evx_gold.fact_dtc; DROP", 1), ("fact_dtc", 1), ("a.b", "1"), ("a.b", True)):
        with pytest.raises(ValueError):
            P.impala_branch_sql(table, snap)


def test_targets_are_project_gold_tables_only():
    assert P.gold_target(C.Names("rsingh_evx"), "fact_dtc") == "rsingh_evx_gold.fact_dtc"
    for prefix, table in (("other_evx", "fact_dtc"), ("rsingh_evx", "fact-dtc"), ("rsingh_evx", "x.y")):
        with pytest.raises(AssertionError):
            P.gold_target(C.Names(prefix), table)


# ---------------------------------------------------------------- local Spark + Iceberg


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    if not os.environ.get("JAVA_HOME") and Path("/usr/libexec/java_home").exists():
        r = subprocess.run(["/usr/libexec/java_home", "-v", "17"], capture_output=True, text=True)
        if r.returncode == 0:
            os.environ["JAVA_HOME"] = r.stdout.strip()
    if not (os.environ.get("JAVA_HOME") or shutil.which("java")):
        pytest.skip("no Java for local Spark")
    import run_local

    try:
        s = run_local.local_spark(tmp_path_factory.mktemp("wap-warehouse"), "3g")
        s.sql("SELECT 1").collect()
    except Exception as e:  # no JVM or the Iceberg / SQLite jars cannot be resolved
        pytest.skip(f"local Spark + Iceberg unavailable: {C.error_summary(e, 200)}")
    yield s
    s.stop()


@pytest.fixture(autouse=True)
def _no_mode_env(monkeypatch):
    monkeypatch.delenv(P.ENV_KEY, raising=False)


def setup(spark, prefix):
    names = C.Names(prefix)
    C.ensure_databases(spark, names, ("gold", "ref"))
    return names


def audit(spark, names, job="t"):
    return C.Audit(spark, names, job, date(2026, 1, 2), None)


def facts(spark, n, days, v=1, offset=0):
    from pyspark.sql import functions as F

    day = F.lit(days[0]) if len(days) == 1 else F.when(F.col("id") % 2 == 0, F.lit(days[0])).otherwise(F.lit(days[1]))
    return spark.range(offset, offset + n).select(F.col("id").alias("k"), F.lit(v).cast("int").alias("v"),
                                                  day.cast("date").alias("_business_date"))


FACT = P.Spec("fact_x", ["k"], "dates", {"v_positive": "v > 0"}, "consume")
DIM = P.Spec("dim_x", ["k"], "full", transform="consume")


def rows(spark, table, ref=None):
    q = f"SELECT * FROM {table}" + (f" VERSION AS OF {ref!r}" if isinstance(ref, str) else
                                    f" VERSION AS OF {ref}" if ref is not None else "")
    return sorted(tuple(r) for r in spark.sql(q).collect())


def log_rows(spark, names, run_id):
    return [r.asDict() for r in spark.sql(
        f"SELECT * FROM {names.t('ref', 'publish_log')} WHERE run_id = '{run_id}' ORDER BY ended_at").collect()]


def snapshot_ops(spark, table):
    return [r[0] for r in spark.sql(f"SELECT operation FROM {table}.snapshots ORDER BY committed_at").collect()]


def test_direct_mode_writes_as_m1(spark):
    """Same rows and the same commits as C.write_dates / C.replace_table, the M1 load_audit row, no branch."""
    names = setup(spark, "rsingh_evx_tdirect")
    twin_f, twin_d = names.t("gold", "twin_fact"), names.t("gold", "twin_dim")
    for i, (batch, dates) in enumerate(((facts(spark, 20, [D1, D2]), [D1, D2]), (facts(spark, 6, [D1], offset=50), [D1, D2]))):
        a = audit(spark, names)
        out = P.publish(spark, batch, FACT, names, a, dates, ["src"])
        C.write_dates(spark, batch, twin_f, dates)
        P.publish(spark, batch.drop("_business_date"), DIM, names, a, dates, ["src"])
        C.replace_table(batch.drop("_business_date"), twin_d)
        target = names.t("gold", "fact_x")
        assert out == {"rows": batch.count(), "snapshot": C.snapshot_id(spark, target)}
        la = spark.sql(f"SELECT status, rows_out, snapshot_after, message FROM {names.t('ref', 'load_audit')} "
                       f"WHERE run_id = '{a.run_id}' AND entity = 'fact_x'").collect()
        assert [tuple(r) for r in la] == [("COMMITTED", batch.count(), out["snapshot"], f"{batch.count()} rows (dates)")]
        assert not C.table_exists(spark, names.t("ref", "publish_log"))
    for mine, twin in ((names.t("gold", "fact_x"), twin_f), (names.t("gold", "dim_x"), twin_d)):
        assert rows(spark, mine) == rows(spark, twin)
        assert snapshot_ops(spark, mine) == snapshot_ops(spark, twin)
        assert set(P.refs(spark, mine)) == {"main"}
    assert rows(spark, names.t("gold", "fact_x")) == sorted((k, 1, date(2026, 1, 1)) for k in range(50, 56))


def test_direct_audit_failure_writes_nothing(spark):
    names = setup(spark, "rsingh_evx_tdirfail")
    with pytest.raises(P.PublishError):
        P.publish(spark, facts(spark, 4, [D1], v=-1), FACT, names, audit(spark, names), [D1], [])
    assert not C.table_exists(spark, names.t("gold", "fact_x"))


def test_wap_pass_fast_forwards_main(spark):
    names = setup(spark, "rsingh_evx_twap")
    target = names.t("gold", "fact_x")
    a = audit(spark, names)
    out = P._publish(spark, facts(spark, 20, [D1, D2]), FACT, names, a, [D1, D2], ["src"], "wap")
    assert len(rows(spark, target)) == 20
    r = P.refs(spark, target)
    assert set(r) == {"main"} and r["main"] == out["snapshot"]
    props = {x[0]: x[1] for x in spark.sql(f"SHOW TBLPROPERTIES {target}").collect()}
    assert props.get("write.wap.enabled") == "true" and props.get("format-version") == "2"
    assert [c.name for c in spark.table(target).schema] == ["k", "v", "_business_date"]
    parts = spark.sql(f"SELECT partition FROM {target}.partitions").collect()
    assert len(parts) == 2
    (lg,) = log_rows(spark, names, a.run_id)
    assert lg["status"] == "PUBLISHED" and lg["mode"] == "wap" and lg["branch"] == P.branch_name(a)
    assert lg["branch_snapshot"] == lg["main_snapshot_after"] == out["snapshot"] != lg["main_snapshot_before"]
    la = spark.sql(f"SELECT status, rows_out, snapshot_before, snapshot_after FROM {names.t('ref', 'load_audit')} "
                   f"WHERE run_id = '{a.run_id}'").collect()
    assert [tuple(x) for x in la] == [("COMMITTED", 20, lg["main_snapshot_before"], out["snapshot"])]

    b = audit(spark, names)
    from pyspark.sql import functions as F

    wider = facts(spark, 10, [D2], offset=100).withColumn("extra", F.lit("e"))
    P._publish(spark, wider, FACT, names, b, [D2], ["src"], "wap")
    got = spark.sql(f"SELECT _business_date, COUNT(*), COUNT(extra) FROM {target} GROUP BY 1 ORDER BY 1").collect()
    assert [tuple(x) for x in got] == [(date(2026, 1, 1), 10, 0), (date(2026, 1, 2), 10, 10)]
    assert set(P.refs(spark, target)) == {"main"}


def test_wap_audit_failure_keeps_main_and_the_branch(spark):
    names = setup(spark, "rsingh_evx_twapfail")
    target = names.t("gold", "fact_x")
    P._publish(spark, facts(spark, 20, [D1, D2]), FACT, names, audit(spark, names), [D1, D2], [], "wap")
    main_before, content = P.main_snapshot(spark, target), rows(spark, target)
    a = audit(spark, names)
    dup = facts(spark, 8, [D1]).unionByName(facts(spark, 2, [D1]))
    with pytest.raises(P.PublishError, match="duplicate keys"):
        P._publish(spark, dup, FACT, names, a, [D1], [], "wap")
    assert P.main_snapshot(spark, target) == main_before and rows(spark, target) == content
    b = P.branch_name(a)
    r = P.refs(spark, target)
    assert b in r
    (lg,) = log_rows(spark, names, a.run_id)
    assert lg["status"] == "AUDIT_FAILED" and lg["branch_snapshot"] == r[b]
    assert lg["main_snapshot_before"] == lg["main_snapshot_after"] == main_before
    assert len(rows(spark, target, lg["branch_snapshot"])) == 10 + 10
    assert P.impala_branch_sql(target, lg["branch_snapshot"]).endswith(f"AS OF {r[b]}")
    assert not spark.sql(f"SELECT * FROM {names.t('ref', 'load_audit')} WHERE run_id = '{a.run_id}' "
                         "AND stage = 'gold'").collect()


def test_wap_check_failure_and_count_mismatch(spark):
    names = setup(spark, "rsingh_evx_twapchk")
    target = names.t("gold", "fact_x")
    a = audit(spark, names)
    with pytest.raises(P.PublishError, match="v_positive"):
        P._publish(spark, facts(spark, 6, [D1], v=0), FACT, names, a, [D1], [], "wap")
    assert rows(spark, target) == [] and P.branch_name(a) in P.refs(spark, target)
    with pytest.raises(P.PublishError, match="rows for the dates processed"):
        P.audit_branch(spark, P.Spec("fact_x", ["k"]), target, P.branch_name(a), [D1], 7)


def test_wap_main_moved_fails_and_keeps_the_branch(spark, monkeypatch):
    names = setup(spark, "rsingh_evx_tmoved")
    target = names.t("gold", "fact_x")
    P._publish(spark, facts(spark, 4, [D1]), FACT, names, audit(spark, names), [D1], [], "wap")
    real = P.audit_branch

    def concurrent_writer(spark_, *args):
        facts(spark_, 2, [D2], offset=900).writeTo(target).append()
        return real(spark_, *args)

    monkeypatch.setattr(P, "audit_branch", concurrent_writer)
    a = audit(spark, names)
    with pytest.raises(P.PublishError, match="main moved"):
        P._publish(spark, facts(spark, 4, [D1], offset=10), FACT, names, a, [D1], [], "wap")
    (lg,) = log_rows(spark, names, a.run_id)
    r = P.refs(spark, target)
    assert lg["status"] == "MAIN_MOVED" and P.branch_name(a) in r
    assert lg["main_snapshot_after"] == r["main"] != lg["main_snapshot_before"]
    assert sorted(x[0] for x in rows(spark, target)) == [0, 1, 2, 3, 900, 901]


def test_wap_full_mode_replaces_the_table(spark):
    names = setup(spark, "rsingh_evx_tfull")
    target = names.t("gold", "dim_x")
    for n in (12, 5):
        P._publish(spark, facts(spark, n, [D1]).drop("_business_date"), DIM, names, audit(spark, names), [D1], [], "wap")
        assert len(rows(spark, target)) == n and set(P.refs(spark, target)) == {"main"}
    assert spark.sql(f"SELECT * FROM {target}.partitions").count() == 1


def test_wap_empties_processed_dates_absent_from_the_batch(spark):
    names = setup(spark, "rsingh_evx_tempty")
    target = names.t("gold", "fact_x")
    P._publish(spark, facts(spark, 20, [D1, D2]), FACT, names, audit(spark, names), [D1, D2], [], "wap")
    main_before = P.main_snapshot(spark, target)
    a = audit(spark, names)
    P._publish(spark, facts(spark, 3, [D1], offset=40), FACT, names, a, [D1, D2], [], "wap")
    assert rows(spark, target) == sorted((k, 1, date(2026, 1, 1)) for k in range(40, 43))
    (lg,) = log_rows(spark, names, a.run_id)
    assert lg["main_snapshot_before"] == main_before and lg["row_count"] == 3
    a = audit(spark, names)
    P._publish(spark, facts(spark, 0, [D1]), FACT, names, a, [D1], [], "wap")
    assert rows(spark, target) == [] and log_rows(spark, names, a.run_id)[0]["status"] == "PUBLISHED"


def test_overwrite_fallback_empties_a_date_on_the_branch_only(spark):
    names = setup(spark, "rsingh_evx_tfallback")
    target = names.t("gold", "fact_x")
    P._publish(spark, facts(spark, 20, [D1, D2]), FACT, names, audit(spark, names), [D1, D2], [], "wap")
    spark.sql(f"ALTER TABLE {target} CREATE BRANCH fb")
    df = facts(spark, 1, [D1])
    P._empty_by_overwrite(spark, df, f"{target}.branch_fb", [D2])
    assert {x[2] for x in rows(spark, target, "fb")} == {date(2026, 1, 1)}
    assert len(rows(spark, target)) == 20


def test_publish_follows_the_spark_conf(spark):
    names = setup(spark, "rsingh_evx_tconf")
    a = audit(spark, names)
    spark.conf.set(P.CONF_KEY, "wap")
    try:
        P.publish(spark, facts(spark, 4, [D1]), FACT, names, a, [D1], [])
    finally:
        spark.conf.unset(P.CONF_KEY)
    assert [r["status"] for r in log_rows(spark, names, a.run_id)] == ["PUBLISHED"]


def test_self_test_is_green_and_cleans_up(spark, capsys):
    names = C.Names("rsingh_evx_tself")
    assert P.self_test(spark, names.prefix) == 0
    out = capsys.readouterr().out
    marks = [ln for ln in out.splitlines() if ln.startswith("WAP_SELFTEST")]
    assert marks and not [m for m in marks if " FAIL " in m], marks
    assert "WAP_SELFTEST result PASS" in out
    assert not C.table_exists(spark, names.t("gold", P.SCRATCH))
    assert spark.sql(f"SELECT COUNT(*) FROM {names.t('ref', 'publish_log')}").collect()[0][0] == 0
    assert spark.sql(f"SELECT COUNT(*) FROM {names.t('ref', 'load_audit')}").collect()[0][0] == 0


def test_self_test_keep_leaves_the_failed_branch(spark):
    names = C.Names("rsingh_evx_tkeep")
    assert P.self_test(spark, names.prefix, keep=True) == 0
    target = names.t("gold", P.SCRATCH)
    kept = [b for b in P.refs(spark, target) if b != "main"]
    assert len(kept) == 1
    st = spark.sql(f"SELECT status, branch FROM {names.t('ref', 'publish_log')} WHERE table_name = '{target}' "
                   "ORDER BY ended_at").collect()
    assert [tuple(x) for x in st][-1] == ("AUDIT_FAILED", kept[0])
