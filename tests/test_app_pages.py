"""The workbench discovers uc01 and its SQL keeps the safety floor ahead of a high score."""
from pathlib import Path

from app.discover import discover
from usecases.uc01_pack_risk.app_page import (
    explain_batch, lit, vehicle_sql, worklist_sql,
)


def test_the_three_m1_pages_are_discovered():
    titles = [title for title, _ in discover(Path(__file__).resolve().parents[1], "g", "s")]
    assert titles == ["Vehicle risk worklist", "Batch containment", "Pack 360"]


def test_risk_order_puts_the_safety_floor_before_the_score():
    sql = worklist_sql("g", "s", "risk", "WORKLIST")
    assert "WHEN 'SAFETY_FLOOR' THEN 0" in sql
    assert "WHEN 'WORKLIST' THEN 1" in sql
    assert "`decision` = 'WORKLIST'" in sql


def test_claim_order_uses_the_ninety_day_claim():
    sql = worklist_sql("g", "s", "claim", None)
    assert "battery_claim_90d" in sql
    assert "SAFETY_FLOOR" not in sql.split("ORDER BY", 1)[1]


def test_a_quote_in_a_vin_cannot_close_the_literal():
    assert lit("X' OR 1=1") == "'X OR 1=1'"
    sql = vehicle_sql("g", "X' OR 1=1")
    assert "v.vin = 'X OR 1=1'" in sql
    assert "v.vin = 'X'" not in sql


def test_a_batch_explanation_states_no_action():
    text = explain_batch({"supplier_batch": "CH1", "batch_status": "RELEASED", "cells": 52, "packs": 1,
                          "vehicles": 1, "worklist_vehicles": 1, "safety_floor_vehicles": 0})
    assert "CH1" in text and "automated action" in text
