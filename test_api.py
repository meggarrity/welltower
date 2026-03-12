import pytest
from fastapi.testclient import TestClient
import os
import psycopg2
from datetime import date

# Set test database environment variables BEFORE importing app
os.environ["DB_NAME"] = "test_welltower"
os.environ["DB_HOST"] = os.getenv("DB_HOST", "localhost")
os.environ["DB_PORT"] = os.getenv("DB_PORT", "5432")
os.environ["DB_USER"] = os.getenv("DB_USER", "postgres")
os.environ["DB_PASSWORD"] = os.getenv("DB_PASSWORD", "postgres")

from api_new import app, init_database, get_connection_params

client = TestClient(app)

def get_postgres_default_conn():
    """Connect to the default 'postgres' database"""
    params = get_connection_params("postgres")
    return psycopg2.connect(**params)

def create_test_db():
    """Create test database"""
    conn = get_postgres_default_conn()
    conn.autocommit = True
    cursor = conn.cursor()
    try:
        cursor.execute("DROP DATABASE IF EXISTS test_welltower;")
        cursor.execute("CREATE DATABASE test_welltower;")
        print("Test database created")
    except Exception as e:
        print(f"Error creating test database: {e}")
    finally:
        cursor.close()
        conn.close()

def drop_test_db():
    """Drop test database"""
    conn = get_postgres_default_conn()
    conn.autocommit = True
    cursor = conn.cursor()
    try:
        cursor.execute("DROP DATABASE IF EXISTS test_welltower;")
        print("Test database dropped")
    except Exception as e:
        print(f"Error dropping test database: {e}")
    finally:
        cursor.close()
        conn.close()

@pytest.fixture(scope="function", autouse=True)
def setup_test_db():
    # Create and initialize test database for each test
    create_test_db()
    init_database("test_welltower")
    yield
    # Cleanup after each test
    drop_test_db()

def test_root():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Welltower PostgreSQL API"}

def test_create_and_get_property():
    # Create a property
    prop_data = {"property_id": 1, "property_name": "Test Property", "owner": "Test Owner"}
    response = client.post("/properties", json=prop_data)
    assert response.status_code == 200
    assert response.json() == prop_data

    # Get the property
    response = client.get("/properties/1")
    assert response.status_code == 200
    assert response.json() == prop_data

    # List properties
    response = client.get("/properties")
    assert response.status_code == 200
    assert len(response.json()) >= 1
    assert any(p["property_id"] == 1 for p in response.json())

def test_delete_property():
    # Create a property first
    prop_data = {"property_id": 1, "property_name": "Test Property", "owner": "Test Owner"}
    client.post("/properties", json=prop_data)
    
    # Delete the property
    response = client.delete("/properties/1")
    assert response.status_code == 200
    assert response.json() == {"deleted": 1}

    # Try to get it again - should 404
    response = client.get("/properties/1")
    assert response.status_code == 404

def test_create_and_get_unit():
    # First, create a property for the unit
    prop_data = {"property_id": 2, "property_name": "Unit Test Property", "owner": "Test"}
    client.post("/properties", json=prop_data)

    # Create a unit
    unit_data = {"unit_id": 1, "unit_number": "101", "property_id": 2, "unit_status": "active", "occupied": True, "rent": 1000.0}
    response = client.post("/units", json=unit_data)
    assert response.status_code == 200
    assert response.json() == unit_data

    # List units
    response = client.get("/units")
    assert response.status_code == 200
    assert len(response.json()) >= 1

def test_create_and_get_resident():
    # Create property and unit first
    prop_data = {"property_id": 2, "property_name": "Test Property", "owner": "Test"}
    client.post("/properties", json=prop_data)
    
    unit_data = {"unit_id": 1, "unit_number": "101", "property_id": 2, "unit_status": "active", "occupied": True, "rent": 1000.0}
    client.post("/units", json=unit_data)
    
    # Create a resident
    resident_data = {"resident_id": 1, "unit_id": 1, "property_id": 2, "first_name": "John", "last_name": "Doe", "move_in_date": "2023-01-01", "move_out_date": None}
    response = client.post("/residents", json=resident_data)
    assert response.status_code == 200
    assert response.json() == resident_data

    # List residents
    response = client.get("/residents")
    assert response.status_code == 200
    assert len(response.json()) >= 1

def test_rentrole_endpoints():
    # Assuming rentroll table is empty, these might return empty lists
    response = client.get("/rentrole/2")
    assert response.status_code == 200
    # Should be empty or have data if loaded

    response = client.get("/rentrole")
    assert response.status_code == 200

def test_inactive_unit_cannot_be_occupied():
    """Test business rule: Inactive units cannot be occupied"""
    # Create a property
    prop_data = {"property_id": 3, "property_name": "Test Property", "owner": "Test Owner"}
    client.post("/properties", json=prop_data)
    
    # Try to create an inactive occupied unit - should fail
    invalid_unit = {"unit_id": 5, "unit_number": "105", "property_id": 3, "unit_status": "inactive", "occupied": True, "rent": 1000.0}
    response = client.post("/units", json=invalid_unit)
    assert response.status_code == 400
    assert "Inactive units cannot be occupied" in response.json()["detail"]
    
    # Create a valid inactive unoccupied unit - should succeed
    valid_unit = {"unit_id": 6, "unit_number": "106", "property_id": 3, "unit_status": "inactive", "occupied": False, "rent": 1000.0}
    response = client.post("/units", json=valid_unit)
    assert response.status_code == 200
    assert response.json() == valid_unit

def test_seed_data_generation():
    """Test seed data generation"""
    from seed_data_new import seed_database
    seed_database("test_welltower", num_properties=3, min_units_per_property=2, max_units_per_property=3, num_residents_ratio=0.5)
    
    conn = psycopg2.connect(**get_connection_params("test_welltower"))
    cur = conn.cursor()
    
    try:
        # basic counts
        cur.execute("SELECT COUNT(*) FROM properties")
        props = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM units")
        units = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM residents")
        res = cur.fetchone()[0]
        assert props == 3
        assert units >= 6   # at least 2 per property
        assert res >= 0
        
        # ensure historical rentroll entries exist (dates before today)
        cur.execute("SELECT date FROM rentroll ORDER BY date LIMIT 1")
        first_date = cur.fetchone()[0]
        assert first_date is not None
        assert first_date < date.today()
        
        # ensure scd historical entries exist
        cur.execute("SELECT effective_from FROM scd ORDER BY effective_from LIMIT 1")
        sf = cur.fetchone()[0]
        assert sf is not None
        assert sf < date.today()

        # business rule: inactive units cannot have current occupants and
        # any resident tied to an inactive unit must have moved out on or
        # before today (i.e. before the inactivation)
        cur.execute("""
            SELECT r.move_out_date
            FROM residents r
            JOIN units u ON r.unit_id = u.unit_id
            WHERE u.unit_status = 'inactive'
        """
        )
        for (m_out,) in cur.fetchall():
            assert m_out is not None
            assert m_out <= date.today()
        # double-check there are no null move_out_date values on inactive units
        cur.execute("""
            SELECT COUNT(*)
            FROM residents r
            JOIN units u ON r.unit_id = u.unit_id
            WHERE u.unit_status = 'inactive' AND r.move_out_date IS NULL
        """
        )
        assert cur.fetchone()[0] == 0
    finally:
        cur.close()
        conn.close()

# Run with: pytest test_api.py
