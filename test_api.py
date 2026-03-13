import pytest
from fastapi.testclient import TestClient
import os
import psycopg2
from datetime import date, timedelta

# Set test database env vars BEFORE importing the app so it connects to the right DB
os.environ["DB_NAME"] = "test_welltower"
os.environ["DB_HOST"] = os.getenv("DB_HOST", "localhost")
os.environ["DB_PORT"] = os.getenv("DB_PORT", "5432")
os.environ["DB_USER"] = os.getenv("DB_USER", "postgres")
os.environ["DB_PASSWORD"] = os.getenv("DB_PASSWORD", "postgres")

from api import app, init_database, get_connection_params
from seed_data import seed_database

client = TestClient(app)
TODAY = date.today().isoformat()
YESTERDAY = (date.today() - timedelta(days=1)).isoformat()


# ── DB lifecycle ─────────────────────────────────────────────────────────────

def _admin_conn():
    return psycopg2.connect(**get_connection_params("postgres"))

def _test_conn():
    return psycopg2.connect(**get_connection_params("test_welltower"))

def _create_test_db():
    conn = _admin_conn()
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute("DROP DATABASE IF EXISTS test_welltower;")
        cur.execute("CREATE DATABASE test_welltower;")
    finally:
        cur.close()
        conn.close()

def _drop_test_db():
    conn = _admin_conn()
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE datname = 'test_welltower' AND pid != pg_backend_pid()
        """)
        cur.execute("DROP DATABASE IF EXISTS test_welltower;")
    finally:
        cur.close()
        conn.close()

@pytest.fixture(scope="session", autouse=True)
def setup_test_db():
    _create_test_db()
    init_database("test_welltower")
    yield
    _drop_test_db()


@pytest.fixture(scope="function", autouse=True)
def clean_tables(setup_test_db):
    yield
    conn = _test_conn()
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute(
            "TRUNCATE residents_scd2, units_scd2, properties_scd2, "
            "rentroll, residents, units, properties CASCADE"
        )
    finally:
        cur.close()
        conn.close()


# ── helpers ───────────────────────────────────────────────────────────────────

def make_property(prop_id=1, name="Sunset Gardens", owner="ACME"):
    return client.post("/properties", json={"property_id": prop_id, "property_name": name, "owner": owner})

def make_unit(unit_id=1, unit_number="101", property_id=1, status="active", occupied=False):
    return client.post("/units", json={
        "unit_id": unit_id, "unit_number": unit_number,
        "property_id": property_id, "unit_status": status, "occupied": occupied,
    })

def make_resident(unit_id=1, first_name="John", last_name="Doe", rent=1500.0, move_in_date=None):
    """Move in a new resident via POST /move_in."""
    return client.post(f"/move_in?unit_id={unit_id}", json={
        "first_name": first_name, "last_name": last_name,
        "rent": rent, "move_in_date": move_in_date or TODAY,
    })

def setup(prop_id=1, unit_id=1):
    """Create a property + active unit."""
    make_property(prop_id)
    make_unit(unit_id, property_id=prop_id)

def query(sql, params=()):
    conn = _test_conn()
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        return cur.fetchall()
    finally:
        cur.close()
        conn.close()


# ── root ──────────────────────────────────────────────────────────────────────

def test_root():
    assert client.get("/").json() == {"message": "Welltower PostgreSQL API"}


# ── properties ────────────────────────────────────────────────────────────────

def test_create_and_get_property():
    r = make_property()
    assert r.status_code == 200
    r = client.get("/properties/1")
    assert r.status_code == 200
    assert r.json()["property_name"] == "Sunset Gardens"
    assert r.json()["owner"] == "ACME"

def test_property_not_found():
    assert client.get("/properties/999").status_code == 404

def test_patch_property():
    make_property()
    r = client.patch("/properties/1", json={"property_name": "New Name"})
    assert r.status_code == 200
    assert client.get("/properties/1").json()["property_name"] == "New Name"

def test_patch_property_leaves_other_fields_unchanged():
    make_property(name="Original", owner="Owner A")
    client.patch("/properties/1", json={"property_name": "Updated"})
    r = client.get("/properties/1").json()
    assert r["property_name"] == "Updated"
    assert r["owner"] == "Owner A"

def test_patch_property_not_found():
    assert client.patch("/properties/999", json={"property_name": "X"}).status_code == 404

def test_patch_property_no_fields_returns_400():
    make_property()
    assert client.patch("/properties/1", json={}).status_code == 400


# ── units ─────────────────────────────────────────────────────────────────────

def test_create_and_get_unit():
    make_property()
    r = make_unit()
    assert r.status_code == 200
    r = client.get("/units/1")
    assert r.status_code == 200
    assert r.json()["unit_number"] == "101"
    assert r.json()["unit_status"] == "active"
    assert r.json()["occupied"] == False

def test_unit_not_found():
    assert client.get("/units/999").status_code == 404

def test_list_units():
    make_property()
    make_unit(1, "101")
    make_unit(2, "102")
    r = client.get("/units")
    assert r.status_code == 200
    assert len(r.json()) == 2

def test_inactive_unit_cannot_be_occupied():
    make_property()
    r = make_unit(status="inactive", occupied=True)
    assert r.status_code == 400
    assert "cannot be occupied" in r.json()["detail"].lower()

def test_inactive_unoccupied_unit_allowed():
    make_property()
    assert make_unit(status="inactive", occupied=False).status_code == 200

def test_patch_unit_number():
    make_property()
    make_unit()
    client.patch("/units/1", json={"unit_number": "202"})
    assert client.get("/units/1").json()["unit_number"] == "202"

def test_patch_unit_not_found():
    assert client.patch("/units/999", json={"unit_number": "X"}).status_code == 404


# ── residents ─────────────────────────────────────────────────────────────────

def test_get_resident_not_found():
    assert client.get("/residents/999").status_code == 404

def test_list_residents():
    setup()
    make_resident()
    r = client.get("/residents")
    assert r.status_code == 200
    assert len(r.json()) == 1

def test_patch_resident():
    setup()
    res = make_resident().json()
    rid = res["resident_id"]
    client.patch(f"/residents/{rid}", json={"first_name": "Jane"})
    assert client.get(f"/residents/{rid}").json()["first_name"] == "Jane"

def test_patch_resident_leaves_other_fields_unchanged():
    setup()
    rid = make_resident(first_name="John", last_name="Doe", rent=1500.0).json()["resident_id"]
    client.patch(f"/residents/{rid}", json={"first_name": "Jane"})
    r = client.get(f"/residents/{rid}").json()
    assert r["last_name"] == "Doe"
    assert r["rent"] == 1500.0

def test_patch_resident_not_found():
    assert client.patch("/residents/999", json={"first_name": "X"}).status_code == 404


# ── move_in: new resident ─────────────────────────────────────────────────────

def test_move_in_new_resident_succeeds():
    setup()
    r = make_resident(rent=1500.0)
    assert r.status_code == 200
    assert r.json()["unit_id"] == 1
    assert client.get("/units/1").json()["occupied"] == True

def test_move_in_missing_required_fields():
    setup()
    r = client.post("/move_in?unit_id=1", json={"rent": 1500.0})
    assert r.status_code == 422
    assert "first_name" in r.json()["detail"]

def test_move_in_rent_zero():
    setup()
    r = client.post("/move_in?unit_id=1", json={
        "first_name": "John", "last_name": "Doe", "rent": 0, "move_in_date": TODAY,
    })
    assert r.status_code == 422
    assert "greater than 0" in r.json()["detail"]

def test_move_in_rent_negative():
    setup()
    r = client.post("/move_in?unit_id=1", json={
        "first_name": "John", "last_name": "Doe", "rent": -100, "move_in_date": TODAY,
    })
    assert r.status_code == 422

def test_move_in_inactive_unit():
    make_property()
    make_unit(status="inactive")
    assert make_resident().status_code == 400

def test_move_in_already_occupied():
    setup()
    make_resident()
    r = make_resident(first_name="Alice")
    assert r.status_code == 400
    assert "already occupied" in r.json()["detail"].lower()

def test_move_in_unit_not_found():
    r = client.post("/move_in?unit_id=999", json={
        "first_name": "John", "last_name": "Doe", "rent": 1500.0, "move_in_date": TODAY,
    })
    assert r.status_code == 404


# ── move_in: existing resident ────────────────────────────────────────────────

def test_move_in_existing_resident_after_move_out():
    make_property()
    make_unit(1, "101")
    make_unit(2, "102")
    rid = make_resident(unit_id=1).json()["resident_id"]
    client.post(f"/move_out?resident_id={rid}&move_out_date={TODAY}")

    r = client.post(f"/move_in?unit_id=2&resident_id={rid}", json={"rent": 2000.0})
    assert r.status_code == 200
    assert r.json()["unit_id"] == 2
    assert client.get("/units/2").json()["occupied"] == True

def test_move_in_existing_resident_clears_move_out_date():
    make_property()
    make_unit(1, "101")
    make_unit(2, "102")
    rid = make_resident(unit_id=1).json()["resident_id"]
    client.post(f"/move_out?resident_id={rid}&move_out_date={TODAY}")
    client.post(f"/move_in?unit_id=2&resident_id={rid}", json={"rent": 2000.0})

    r = client.get(f"/residents/{rid}").json()
    assert r["move_out_date"] is None
    assert r["unit_id"] == 2

def test_move_in_existing_resident_still_active_rejected():
    make_property()
    make_unit(1, "101")
    make_unit(2, "102")
    rid = make_resident(unit_id=1).json()["resident_id"]

    r = client.post(f"/move_in?unit_id=2&resident_id={rid}", json={"rent": 2000.0})
    assert r.status_code == 400
    assert "already assigned" in r.json()["detail"].lower()

def test_move_in_existing_resident_rent_required():
    make_property()
    make_unit(1, "101")
    make_unit(2, "102")
    rid = make_resident(unit_id=1).json()["resident_id"]
    client.post(f"/move_out?resident_id={rid}&move_out_date={TODAY}")

    r = client.post(f"/move_in?unit_id=2&resident_id={rid}", json={})
    assert r.status_code == 422
    assert "rent is required" in r.json()["detail"]

def test_move_in_existing_resident_not_found():
    setup()
    r = client.post("/move_in?unit_id=1&resident_id=999", json={"rent": 1500.0})
    assert r.status_code == 404


# ── move_out ──────────────────────────────────────────────────────────────────

def test_move_out_sets_move_out_date_and_zeroes_rent():
    setup()
    rid = make_resident(rent=1500.0).json()["resident_id"]
    r = client.post(f"/move_out?resident_id={rid}&move_out_date={TODAY}")
    assert r.status_code == 200
    resident = client.get(f"/residents/{rid}").json()
    assert resident["move_out_date"] == TODAY
    assert resident["rent"] == 0

def test_move_out_marks_unit_unoccupied():
    setup()
    rid = make_resident().json()["resident_id"]
    client.post(f"/move_out?resident_id={rid}&move_out_date={TODAY}")
    assert client.get("/units/1").json()["occupied"] == False

def test_move_out_not_found():
    assert client.post("/move_out?resident_id=999").status_code == 404


# ── update_rent ───────────────────────────────────────────────────────────────

def test_update_rent():
    setup()
    rid = make_resident(rent=1500.0).json()["resident_id"]
    r = client.post(f"/update_rent?resident_id={rid}&new_rent=1800.0")
    assert r.status_code == 200
    assert r.json()["rent"] == 1800.0

def test_update_rent_zero_rejected():
    setup()
    rid = make_resident(rent=1500.0).json()["resident_id"]
    assert client.post(f"/update_rent?resident_id={rid}&new_rent=0").status_code == 422

def test_update_rent_negative_rejected():
    setup()
    rid = make_resident(rent=1500.0).json()["resident_id"]
    assert client.post(f"/update_rent?resident_id={rid}&new_rent=-500").status_code == 422

def test_update_rent_not_found():
    assert client.post("/update_rent?resident_id=999&new_rent=1500").status_code == 404


# ── update_unit_status ────────────────────────────────────────────────────────

def test_update_unit_status_to_inactive():
    make_property()
    make_unit()
    r = client.post("/update_unit_status?unit_id=1&new_status=inactive")
    assert r.status_code == 200
    assert client.get("/units/1").json()["unit_status"] == "inactive"

def test_update_unit_status_back_to_active():
    make_property()
    make_unit(status="inactive")
    r = client.post("/update_unit_status?unit_id=1&new_status=active")
    assert r.status_code == 200
    assert client.get("/units/1").json()["unit_status"] == "active"

def test_update_unit_status_invalid_value():
    make_property()
    make_unit()
    assert client.post("/update_unit_status?unit_id=1&new_status=renovating").status_code == 400

def test_update_unit_status_occupied_cannot_deactivate():
    setup()
    make_resident()
    r = client.post("/update_unit_status?unit_id=1&new_status=inactive")
    assert r.status_code == 400
    assert "occupied" in r.json()["detail"].lower()

def test_update_unit_status_not_found():
    assert client.post("/update_unit_status?unit_id=999&new_status=inactive").status_code == 404


# ── rentroll ──────────────────────────────────────────────────────────────────

def test_rentroll_by_property_empty():
    r = client.get("/rentroll/1")
    assert r.status_code == 200
    assert r.json() == []

def test_rentroll_all_empty():
    r = client.get("/rentroll")
    assert r.status_code == 200
    assert r.json() == []

def test_rentroll_date_filter_returns_empty_for_future():
    r = client.get("/rentroll?start_date=2099-01-01&end_date=2099-12-31")
    assert r.status_code == 200
    assert r.json() == []

def test_rentroll_date_filter_returns_empty_for_past():
    r = client.get("/rentroll?start_date=2000-01-01&end_date=2000-12-31")
    assert r.status_code == 200
    assert r.json() == []


# ── overview ──────────────────────────────────────────────────────────────────

def test_overview_vacant_unit():
    setup()
    r = client.get("/overview")
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 1
    assert data[0]["unit_number"] == "101"
    assert data[0]["resident_name"] is None
    assert data[0]["resident_id"] is None

def test_overview_occupied_unit_shows_resident_name():
    setup()
    make_resident(first_name="Alice", last_name="Smith")
    data = client.get("/overview").json()
    assert data[0]["resident_name"] == "Alice Smith"
    assert data[0]["resident_id"] is not None

def test_overview_after_move_out_shows_vacant():
    setup()
    rid = make_resident(first_name="Alice", last_name="Smith").json()["resident_id"]
    client.post(f"/move_out?resident_id={rid}&move_out_date={TODAY}")
    data = client.get("/overview").json()
    assert data[0]["resident_name"] is None


# ── SCD2 trigger behavior ─────────────────────────────────────────────────────

def test_move_in_creates_scd2_record():
    setup()
    rid = make_resident(rent=1500.0).json()["resident_id"]
    rows = query("SELECT is_current, rent, expiration_date FROM residents_scd2 WHERE resident_id = %s", (rid,))
    assert len(rows) == 1
    assert rows[0][0] == True    # is_current
    assert rows[0][1] == 1500.0  # rent
    assert rows[0][2] is None    # no expiration yet

def test_move_out_creates_new_scd2_row_and_expires_old():
    setup()
    rid = make_resident(rent=1500.0).json()["resident_id"]
    client.post(f"/move_out?resident_id={rid}&move_out_date={TODAY}")
    rows = query(
        "SELECT is_current, expiration_date FROM residents_scd2 WHERE resident_id = %s ORDER BY effective_date",
        (rid,),
    )
    assert len(rows) == 2
    assert rows[0][0] == False        # original row expired
    assert rows[0][1] is not None     # has expiration_date
    assert rows[1][0] == True         # new current row

def test_update_rent_creates_new_scd2_row():
    setup()
    rid = make_resident(rent=1500.0).json()["resident_id"]
    client.post(f"/update_rent?resident_id={rid}&new_rent=1800.0")
    rows = query(
        "SELECT is_current, rent FROM residents_scd2 WHERE resident_id = %s ORDER BY effective_date",
        (rid,),
    )
    assert len(rows) == 2
    assert rows[0][0] == False   # old row no longer current
    assert rows[1][0] == True    # new row is current
    assert rows[1][1] == 1800.0  # new rent recorded

def test_only_one_current_scd2_row_per_resident():
    setup()
    rid = make_resident(rent=1500.0).json()["resident_id"]
    client.post(f"/update_rent?resident_id={rid}&new_rent=1600.0")
    client.post(f"/update_rent?resident_id={rid}&new_rent=1700.0")
    rows = query(
        "SELECT COUNT(*) FROM residents_scd2 WHERE resident_id = %s AND is_current = TRUE",
        (rid,),
    )
    assert rows[0][0] == 1


# ── retroactive rentroll recomputation ────────────────────────────────────────
# SCD2 effective_date is always CURRENT_DATE, so recompute affects today's row.

def _todays_rentroll(unit_id=1):
    """Return today's rentroll row for a unit, or None."""
    rows = query(
        "SELECT resident_id, resident_name, rent, unit_status FROM rentroll WHERE unit_id = %s AND date = %s",
        (unit_id, TODAY),
    )
    return rows[0] if rows else None

def test_move_in_creates_todays_rentroll_row():
    setup()
    make_resident(first_name="Jane", last_name="Smith", rent=2000.0)
    row = _todays_rentroll()
    assert row is not None
    assert row[1] == "Jane Smith"
    assert row[2] == 2000.0

def test_move_out_vacates_todays_rentroll_row():
    setup()
    rid = make_resident(rent=2000.0).json()["resident_id"]
    client.post(f"/move_out?resident_id={rid}&move_out_date={TODAY}")
    row = _todays_rentroll()
    assert row is not None
    assert row[0] is None    # resident_id null (vacant)
    assert row[2] == 0       # rent 0

def test_update_rent_reflects_in_todays_rentroll():
    setup()
    rid = make_resident(rent=1500.0).json()["resident_id"]
    client.post(f"/update_rent?resident_id={rid}&new_rent=1800.0")
    row = _todays_rentroll()
    assert row is not None
    assert row[2] == 1800.0

def test_patch_rent_reflects_in_todays_rentroll():
    setup()
    rid = make_resident(rent=1500.0).json()["resident_id"]
    client.patch(f"/residents/{rid}", json={"rent": 2200.0})
    row = _todays_rentroll()
    assert row is not None
    assert row[2] == 2200.0

def test_patch_name_reflects_in_todays_rentroll():
    setup()
    rid = make_resident(first_name="John", last_name="Doe", rent=1500.0).json()["resident_id"]
    client.patch(f"/residents/{rid}", json={"first_name": "Jane"})
    row = _todays_rentroll()
    assert row is not None
    assert row[1] == "Jane Doe"

def test_update_unit_status_reflects_in_todays_rentroll():
    make_property()
    make_unit()
    make_resident()  # occupy first so we have a row, then move out
    rid = query("SELECT resident_id FROM residents WHERE unit_id = 1")[0][0]
    client.post(f"/move_out?resident_id={rid}&move_out_date={TODAY}")
    client.post("/update_unit_status?unit_id=1&new_status=inactive")
    row = _todays_rentroll()
    assert row is not None
    assert row[3] == "inactive"

def test_move_in_after_move_out_same_unit_updates_rentroll():
    """Move out then move in new resident same day — rentroll shows new resident."""
    setup()
    rid1 = make_resident(first_name="Alice", last_name="A", rent=1500.0).json()["resident_id"]
    client.post(f"/move_out?resident_id={rid1}&move_out_date={TODAY}")
    make_resident(first_name="Bob", last_name="B", rent=1800.0)
    row = _todays_rentroll()
    assert row[1] == "Bob B"
    assert row[2] == 1800.0


# ── seed data ─────────────────────────────────────────────────────────────────

def test_seed_data_business_rules():
    seed_database("test_welltower", num_properties=2, min_units_per_property=2, max_units_per_property=3, num_residents_ratio=0.7)

    # no inactive unit has an active resident
    rows = query("""
        SELECT COUNT(*) FROM residents r
        JOIN units u ON r.unit_id = u.unit_id
        WHERE u.unit_status = 'inactive' AND r.move_out_date IS NULL
    """)
    assert rows[0][0] == 0

    # all moved-out residents have rent = 0
    rows = query("SELECT COUNT(*) FROM residents WHERE move_out_date IS NOT NULL AND rent > 0")
    assert rows[0][0] == 0

    # every occupied unit has exactly one active resident
    rows = query("""
        SELECT COUNT(*) FROM units u
        WHERE u.occupied = TRUE
        AND NOT EXISTS (
            SELECT 1 FROM residents r
            WHERE r.unit_id = u.unit_id AND r.move_out_date IS NULL
        )
    """)
    assert rows[0][0] == 0

    # every active resident has a positive rent
    rows = query("SELECT COUNT(*) FROM residents WHERE move_out_date IS NULL AND rent <= 0")
    assert rows[0][0] == 0


# Run with: docker-compose --profile test run --rm test
