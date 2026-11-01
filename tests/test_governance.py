import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import governance as G  # noqa: E402


def test_policies_and_tags_stay_in_the_evx_prefix():
    gov, kpi, _ = G.load_config()
    for tag in gov["classifications"]:
        assert G.policy_name(gov, tag).startswith("rsingh-evx-sensitive-")
        assert G.masking_policy(gov, tag)["dataMaskPolicyItems"][0]["users"] == ["federal01", "federal07"]
    assert set(gov["columns"].values()) <= set(gov["classifications"])


def test_glossary_assignments_exist_in_code():
    gov, kpi, _ = G.load_config()
    gold = set(re.findall(r'Spec\("(\w+)"', (ROOT / "cde" / "jobs" / "build_gold.py").read_text()))
    views = {f.stem.split("_", 1)[1] for f in (ROOT / "sql" / "semantic").glob("[0-9][0-9]_*.sql")}
    terms = G.glossary_terms(gov, kpi)
    assert {t["abbreviation"] for t in terms} >= {k["kpi_code"] for k in kpi["kpis"]}
    for t in terms:
        for table in t["assigned_to"]:
            db, name = table.split(".")
            assert name in (gold if db == "rsingh_evx_gold" else views), table
    for k in kpi["kpis"]:
        assert k["certified_view"] in views


def test_classification_changes():
    gov = {"columns": {"phone": "EVX_SENSITIVE_CONTACT"}}
    cols = [{"guid": "1", "qualifiedName": "db.t.phone@cm", "name": "phone", "tags": []},
            {"guid": "2", "qualifiedName": "db.t.vin@cm", "name": "vin", "tags": ["EVX_SENSITIVE_PII", "OTHER_TAG"]}]
    adds, removes = G.classification_changes(gov, cols)
    assert adds == [("1", "db.t.phone@cm", "EVX_SENSITIVE_CONTACT")]
    assert removes == [("2", "db.t.vin@cm", "EVX_SENSITIVE_PII")]
