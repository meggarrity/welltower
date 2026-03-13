import calendar
from datetime import date, datetime

from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from typing import Optional, List
from seed_data import seed_database
from fastapi.middleware.cors import CORSMiddleware


app = FastAPI(title="Welltower API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],   # add your prod domain here too
    allow_methods=["*"],
    allow_headers=["*"],
)
# -- PostgreSQL connection configuration ------------------------------------------
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "welltower")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

# For testing
TEST_DB_NAME = os.getenv("TEST_DB_NAME", "test_welltower")

def get_connection_params(db_name: str = DB_NAME) -> dict:
    """Build PostgreSQL connection parameters"""
    return {
        "host": DB_HOST,
        "port": DB_PORT,
        "database": db_name,
        "user": DB_USER,
        "password": DB_PASSWORD,
    }

def create_connection(db_name: str = DB_NAME):
    """Create a PostgreSQL connection"""
    try:
        conn = psycopg2.connect(**get_connection_params(db_name))
        conn.autocommit = False
        return conn
    except Exception as e:
        raise RuntimeError(f"Unable to connect to PostgreSQL: {e}")

# -- database dependency ---------------------------------------------------
def get_db():
    conn = create_connection(DB_NAME)
    try:
        yield conn
    finally:
        conn.close()

def init_database(db_name: str = DB_NAME):
    """Initialize database with tables, views, and triggers"""
    conn = create_connection(db_name)
    cursor = conn.cursor()
    
    try:
        # Create properties table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS properties (
          property_id INT PRIMARY KEY NOT NULL,
          property_name TEXT NOT NULL,
          owner TEXT NOT NULL,
          address TEXT
        );
        """)
        
        # create units table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS units (
          unit_id INT PRIMARY KEY NOT NULL,
          unit_number TEXT NOT NULL,
          property_id INT NOT NULL,
          unit_status TEXT NOT NULL CHECK(unit_status IN ('active', 'inactive')),
          occupied BOOLEAN NOT NULL,
          FOREIGN KEY (property_id) REFERENCES properties(property_id),
          CHECK (unit_status = 'active' OR occupied = false)
        );
        """)
        
        # create residents table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS residents (
          resident_id INT PRIMARY KEY NOT NULL,
          unit_id INT NOT NULL,
          property_id INT NOT NULL,
          first_name TEXT NOT NULL,
          last_name TEXT NOT NULL,
          rent FLOAT NOT NULL,
          move_in_date DATE NOT NULL,
          move_out_date DATE,
          FOREIGN KEY (unit_id) REFERENCES units(unit_id),
          FOREIGN KEY (property_id) REFERENCES properties(property_id)
        );
        """)
        
        # properties scd2 - changes when property name or owner changes
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS properties_scd2 (
            property_scd2_id         SERIAL PRIMARY KEY,
            property_id     INTEGER NOT NULL,
            property_name   TEXT,
            address         TEXT,
            effective_date  DATE NOT NULL,
            expiration_date  DATE,
            is_current      BOOLEAN NOT NULL DEFAULT TRUE
        );
        """)

        # unit scd2 - changes when unit status changes or occupied changes
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS units_scd2 (
            unit_scd2_id         SERIAL PRIMARY KEY,
            unit_id         INTEGER NOT NULL,
            property_id     INTEGER NOT NULL,
            unit_number     TEXT NOT NULL,
            unit_status     TEXT DEFAULT 'active' NOT NULL CHECK (unit_status IN ('active', 'inactive')),
            occupied        BOOLEAN NOT NULL,
            effective_date  DATE NOT NULL,
            expiration_date  DATE,
            is_current      BOOLEAN NOT NULL DEFAULT TRUE
        );
        """)

        # resident scd2 - changes when resident moves in/out or rent changes
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS residents_scd2 (
            resident_scd2_id         SERIAL PRIMARY KEY,
            resident_id     INTEGER NOT NULL,
            unit_id         INTEGER,
            first_name     TEXT NOT NULL,
            last_name      TEXT NOT NULL,
            move_in_date    DATE,
            move_out_date   DATE,
            rent    REAL,
            effective_date  DATE NOT NULL,
            expiration_date  DATE,
            is_current      BOOLEAN NOT NULL DEFAULT TRUE
        );
        """)

        # properties scd2 trigger
        cursor.execute("""
            CREATE OR REPLACE FUNCTION trg_properties_scd2()
            RETURNS TRIGGER LANGUAGE plpgsql AS $$
            BEGIN
                IF TG_OP IN ('UPDATE', 'DELETE') THEN
                    UPDATE properties_scd2
                    SET
                        expiration_date = CURRENT_DATE - 1,
                        is_current      = FALSE
                    WHERE property_id = OLD.property_id
                    AND is_current  = TRUE;
                END IF;

                IF TG_OP IN ('INSERT', 'UPDATE') THEN
                    IF TG_OP = 'UPDATE' AND NOT (
                        OLD.property_name IS DISTINCT FROM NEW.property_name OR
                        OLD.address       IS DISTINCT FROM NEW.address
                    ) THEN
                        RETURN NEW;
                    END IF;

                    INSERT INTO properties_scd2 (
                        property_id,    property_name,  address,
                        effective_date, expiration_date,    is_current
                    ) VALUES (
                        NEW.property_id,
                        NEW.property_name,
                        NEW.address,
                        CURRENT_DATE,
                        NULL,
                        TRUE
                    );
                END IF;

                RETURN COALESCE(NEW, OLD);
            END;
            $$;
                """)

        # Units trigger - captures changes to unit status and occupied flag
        cursor.execute("""
            CREATE OR REPLACE FUNCTION trg_units_scd2()
            RETURNS TRIGGER LANGUAGE plpgsql AS $$
            BEGIN
                IF TG_OP IN ('UPDATE', 'DELETE') THEN
                    UPDATE units_scd2
                    SET
                        expiration_date = CURRENT_DATE - 1,
                        is_current      = FALSE
                    WHERE unit_id    = OLD.unit_id
                    AND is_current = TRUE;
                END IF;

                IF TG_OP IN ('INSERT', 'UPDATE') THEN
                    IF TG_OP = 'UPDATE' AND NOT (
                        OLD.unit_number IS DISTINCT FROM NEW.unit_number OR
                        OLD.property_id IS DISTINCT FROM NEW.property_id OR
                        OLD.unit_status IS DISTINCT FROM NEW.unit_status  OR
                        OLD.occupied    IS DISTINCT FROM NEW.occupied
                    ) THEN
                        RETURN NEW;
                    END IF;

                    INSERT INTO units_scd2 (
                        unit_id,    property_id,    unit_number,    unit_status,    occupied,
                        effective_date, expiration_date, is_current
                    ) VALUES (
                        NEW.unit_id, NEW.property_id, NEW.unit_number, NEW.unit_status, NEW.occupied,
                        CURRENT_DATE, NULL, TRUE
                    );
                END IF;

                RETURN COALESCE(NEW, OLD);
            END;
            $$;
                """)

        # Residents trigger - captures move in/out and rent changes
        cursor.execute("""
            CREATE OR REPLACE FUNCTION trg_residents_scd2()
            RETURNS TRIGGER LANGUAGE plpgsql AS $$
            BEGIN
                IF TG_OP IN ('UPDATE', 'DELETE') THEN
                    UPDATE residents_scd2
                    SET
                        expiration_date = CURRENT_DATE - 1,
                        is_current      = FALSE
                    WHERE resident_id = OLD.resident_id
                    AND is_current = TRUE;
                END IF;

                IF TG_OP IN ('INSERT', 'UPDATE') THEN
                    IF TG_OP = 'UPDATE' AND NOT (
                        OLD.rent          IS DISTINCT FROM NEW.rent          OR
                        OLD.move_out_date IS DISTINCT FROM NEW.move_out_date OR
                        OLD.move_in_date  IS DISTINCT FROM NEW.move_in_date  OR
                        OLD.first_name    IS DISTINCT FROM NEW.first_name    OR
                        OLD.last_name     IS DISTINCT FROM NEW.last_name     OR
                        OLD.unit_id       IS DISTINCT FROM NEW.unit_id
                    ) THEN
                        RETURN NEW;
                    END IF;

                    INSERT INTO residents_scd2 (
                        resident_id, unit_id,  first_name,    last_name,
                        move_in_date, move_out_date, rent,
                        effective_date, expiration_date, is_current
                    ) VALUES (
                        NEW.resident_id, NEW.unit_id, NEW.first_name, NEW.last_name,
                        NEW.move_in_date, NEW.move_out_date, NEW.rent,
                        CURRENT_DATE, NULL, TRUE
                    );
                END IF;

                RETURN COALESCE(NEW, OLD);
            END;
            $$;
                       """)

        # Attach triggers to base tables
        cursor.execute("""
            DROP TRIGGER IF EXISTS trg_properties_scd2_trg ON properties;
            CREATE TRIGGER trg_properties_scd2_trg
            AFTER INSERT OR UPDATE OR DELETE ON properties
            FOR EACH ROW EXECUTE FUNCTION trg_properties_scd2();
        """)
        cursor.execute("""
            DROP TRIGGER IF EXISTS trg_units_scd2_trg ON units;
            CREATE TRIGGER trg_units_scd2_trg
            AFTER INSERT OR UPDATE OR DELETE ON units
            FOR EACH ROW EXECUTE FUNCTION trg_units_scd2();
        """)
        cursor.execute("""
            DROP TRIGGER IF EXISTS trg_residents_scd2_trg ON residents;
            CREATE TRIGGER trg_residents_scd2_trg
            AFTER INSERT OR UPDATE OR DELETE ON residents
            FOR EACH ROW EXECUTE FUNCTION trg_residents_scd2();
        """)

        # create rent roll table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS rentroll (
            rentroll_id     SERIAL PRIMARY KEY,
            date            DATE NOT NULL,
            property_id     INT NOT NULL,
            property_name   TEXT,
            unit_id         INT,
            unit_number     TEXT,
            resident_id     INT,
            resident_name   TEXT,
            rent            REAL,
            unit_status     TEXT,
            CONSTRAINT rentroll_date_unit UNIQUE (date, unit_id)
        );
        """)

        conn.commit()
        print(f"Database '{db_name}' initialized successfully")

        # Schedule nightly rentroll snapshot — only works when pg_cron is available
        try:
            cursor.execute("""
            SELECT cron.schedule(
                'nightly-rentroll-snapshot',
                '0 0 * * *',
                $$
                INSERT INTO rentroll (
                    date, unit_id, property_id, property_name,
                    unit_number, resident_id, resident_name, rent, unit_status
                )
                SELECT
                    CURRENT_DATE,
                    u.unit_id,
                    u.property_id,
                    p.property_name,
                    u.unit_number,
                    r.resident_id,
                    CONCAT(r.first_name, ' ', r.last_name),
                    COALESCE(r.rent, 0),
                    u.unit_status
                FROM units_scd2 u
                JOIN properties_scd2 p
                    ON  p.property_id = u.property_id
                    AND CURRENT_DATE >= p.effective_date
                    AND (p.expiration_date IS NULL OR CURRENT_DATE <= p.expiration_date)
                LEFT JOIN residents_scd2 r
                    ON  r.unit_id = u.unit_id
                    AND CURRENT_DATE >= r.effective_date
                    AND (r.expiration_date IS NULL OR CURRENT_DATE <= r.expiration_date)
                    AND (r.move_out_date IS NULL OR CURRENT_DATE < r.move_out_date)
                    AND u.unit_status = 'active'
                WHERE CURRENT_DATE >= u.effective_date
                    AND (u.expiration_date IS NULL OR CURRENT_DATE <= u.expiration_date)
                ON CONFLICT (date, unit_id) DO NOTHING;
                $$
            );
            """)
            conn.commit()
        except Exception:
            conn.rollback()
    except Exception as e:
        conn.rollback()
        print(f"Error initializing database: {e}")
        raise
    finally:
        cursor.close()

def take_rentroll_snapshot(db_name: str = DB_NAME):
    """Insert a rentroll row for every unit for every day from the earliest
    scd2 effective_date through today.  Safe to run repeatedly (ON CONFLICT DO NOTHING)."""
    conn = create_connection(db_name)
    cursor = conn.cursor()
    try:
        cursor.execute("""
        INSERT INTO rentroll (
            date, unit_id, property_id, property_name,
            unit_number, resident_id, resident_name, rent, unit_status
        )
        SELECT
            d.date,
            u.unit_id,
            u.property_id,
            p.property_name,
            u.unit_number,
            r.resident_id,
            CONCAT(r.first_name, ' ', r.last_name),
            COALESCE(r.rent, 0),
            u.unit_status
        FROM generate_series(
            COALESCE((SELECT MIN(effective_date) FROM units_scd2), CURRENT_DATE),
            CURRENT_DATE,
            INTERVAL '1 day'
        ) AS d(date)
        JOIN units_scd2 u
            ON  d.date >= u.effective_date
            AND (u.expiration_date IS NULL OR d.date <= u.expiration_date)
        JOIN properties_scd2 p
            ON  p.property_id = u.property_id
            AND d.date >= p.effective_date
            AND (p.expiration_date IS NULL OR d.date <= p.expiration_date)
        LEFT JOIN residents_scd2 r
            ON  r.unit_id = u.unit_id
            AND d.date >= r.effective_date
            AND (r.expiration_date IS NULL OR d.date <= r.expiration_date)
            AND (r.move_out_date IS NULL OR d.date < r.move_out_date)
            AND u.unit_status = 'active'
        ON CONFLICT (date, unit_id) DO NOTHING;
        """)
        conn.commit()
        print("Rentroll snapshot complete.")
    except Exception as e:
        conn.rollback()
        print(f"Error taking rentroll snapshot: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

def recompute_rentroll_for_unit(db, unit_id: int, start_date, end_date):
    """Delete and reinsert rentroll rows for one unit over [start_date, end_date].
    Call this after any write that changes what a unit's rent roll should show."""
    cursor = db.cursor()
    try:
        cursor.execute(
            "DELETE FROM rentroll WHERE unit_id = %s AND date >= %s AND date <= %s",
            (unit_id, start_date, end_date),
        )
        cursor.execute("""
        INSERT INTO rentroll (
            date, unit_id, property_id, property_name,
            unit_number, resident_id, resident_name, rent, unit_status
        )
        SELECT
            d.date,
            u.unit_id,
            u.property_id,
            p.property_name,
            u.unit_number,
            r.resident_id,
            CONCAT(r.first_name, ' ', r.last_name),
            COALESCE(r.rent, 0),
            u.unit_status
        FROM generate_series(%s::date, %s::date, INTERVAL '1 day') AS d(date)
        JOIN units_scd2 u
            ON  u.unit_id = %s
            AND d.date >= u.effective_date
            AND (u.expiration_date IS NULL OR d.date <= u.expiration_date)
        JOIN properties_scd2 p
            ON  p.property_id = u.property_id
            AND d.date >= p.effective_date
            AND (p.expiration_date IS NULL OR d.date <= p.expiration_date)
        LEFT JOIN residents_scd2 r
            ON  r.unit_id = u.unit_id
            AND d.date >= r.effective_date
            AND (r.expiration_date IS NULL OR d.date <= r.expiration_date)
            AND (r.move_out_date IS NULL OR d.date < r.move_out_date)
            AND u.unit_status = 'active'
        ON CONFLICT (date, unit_id) DO NOTHING
        """, (start_date, end_date, unit_id))
        db.commit()
    finally:
        cursor.close()


# -- startup event ---------------------------------------------------
@app.on_event("startup")
def startup_event():
    try:
        init_database(DB_NAME)
        
        # Check if LOAD_FAKE_DATA is set
        if os.getenv("LOAD_FAKE_DATA") != "false":
            seed_database(
                db_name=DB_NAME,
                num_properties=int(os.getenv("NUM_PROPERTIES", "3")),
                min_units_per_property=int(os.getenv("MIN_UNITS_PER_PROPERTY", "2")),
                max_units_per_property=int(os.getenv("MAX_UNITS_PER_PROPERTY", "5")),
                num_residents_ratio=float(os.getenv("RESIDENTS_RATIO", "0.7"))
            )
        else:
            print("Database tables created (no fake data loaded).")
        take_rentroll_snapshot(DB_NAME)
    except Exception as e:
        print(f"Startup error: {e}")

# -- models ---------------------------------------------------------------

class Property(BaseModel):
    property_id: int
    property_name: str
    owner: str
    address: Optional[str] = None

class Unit(BaseModel):
    unit_id: int
    unit_number: str
    property_id: int
    unit_status: str
    occupied: bool

class Resident(BaseModel):
    resident_id: int
    unit_id: int
    property_id: int
    first_name: str
    last_name: str
    rent: float
    move_in_date: str
    move_out_date: Optional[str] = None

class PropertySCD2(BaseModel):
    property_scd2_id: int
    property_id: int
    property_name: Optional[str] = None
    address: Optional[str] = None
    effective_date: str
    expiration_date: Optional[str] = None
    is_current: bool

class UnitSCD2(BaseModel):
    unit_scd2_id: int
    unit_id: int
    property_id: int
    unit_number: str
    unit_status: str
    occupied: bool
    effective_date: str
    expiration_date: Optional[str] = None
    is_current: bool

class ResidentSCD2(BaseModel):
    resident_scd2_id: int
    resident_id: int
    unit_id: Optional[int] = None
    first_name: str
    last_name: str
    move_in_date: Optional[str] = None
    move_out_date: Optional[str] = None
    rent: Optional[float] = None
    effective_date: str
    expiration_date: Optional[str] = None
    is_current: bool

class RentRoll(BaseModel):
    date: str = date.today().isoformat()
    property_id: int
    unit_id: int
    unit_number: str
    resident_id: Optional[int] = None
    resident_name: Optional[str] = None
    rent: Optional[float] = 0.00
    unit_status: str = 'active'

# Patch models — all fields optional so only supplied ones are updated
class PropertyPatch(BaseModel):
    model_config = {"json_schema_extra": {"example": {}}}
    property_name: Optional[str] = None
    owner: Optional[str] = None
    address: Optional[str] = None

class UnitPatch(BaseModel):
    model_config = {"json_schema_extra": {"example": {}}}
    unit_number: Optional[str] = None
    unit_status: Optional[str] = None
    occupied: Optional[bool] = None

class ResidentPatch(BaseModel):
    model_config = {"json_schema_extra": {"example": {}}}
    unit_id: Optional[int] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    rent: Optional[float] = None
    move_in_date: Optional[date] = None
    move_out_date: Optional[date] = None


# -- helper functions ------------------------------------------------------

def run_query(conn, sql: str, params: Optional[tuple] = None):
    cursor = conn.cursor()
    try:
        cursor.execute(sql, params or ())
        return cursor.fetchall()
    finally:
        cursor.close()

def apply_patch(db, table: str, id_col: str, id_val, patch: dict):
    """Build and run a dynamic UPDATE from a dict of {column: value} pairs.
    Expects patch to already be filtered (e.g. via model_dump(exclude_unset=True)).
    Returns the number of rows updated."""
    updates = patch
    if not updates:
        raise HTTPException(status_code=400, detail="No fields provided to update")
    set_clause = ", ".join(f"{col} = %s" for col in updates)
    values = list(updates.values()) + [id_val]
    cursor = db.cursor()
    try:
        cursor.execute(f"UPDATE {table} SET {set_clause} WHERE {id_col} = %s", values)
        db.commit()
        return cursor.rowcount
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()

# -- CRUD endpoints --------------------------------------------------------

@app.get("/properties", response_model=List[Property])
def list_properties(db = Depends(get_db)):
    records = run_query(db, "SELECT property_id, property_name, owner FROM properties")
    return [Property(property_id=r[0], property_name=r[1], owner=r[2]) for r in records]

@app.get("/properties/{property_id}", response_model=Property)
def get_property(property_id: int, db = Depends(get_db)):
    records = run_query(
        db, "SELECT property_id, property_name, owner FROM properties WHERE property_id = %s", (property_id,)
    )
    if not records:
        raise HTTPException(status_code=404, detail="Property not found")
    r = records[0]
    return Property(property_id=r[0], property_name=r[1], owner=r[2])

@app.get("/units/{unit_id}", response_model=Unit)
def get_unit(unit_id: int, db = Depends(get_db)):
    records = run_query(
        db, "SELECT unit_id, unit_number, property_id, unit_status, occupied FROM units WHERE unit_id = %s", (unit_id,)
    )
    if not records:
        raise HTTPException(status_code=404, detail="Unit not found")
    r = records[0]
    return Unit(unit_id=r[0], unit_number=r[1], property_id=r[2], unit_status=r[3], occupied=r[4])

@app.get("/residents/{resident_id}", response_model=Resident)
def get_resident(resident_id: int, db = Depends(get_db)):
    records = run_query(
        db, "SELECT * from residents where resident_id = %s", (resident_id,)
    )
    if not records:
        raise HTTPException(status_code=404, detail="Resident not found")
    r = records[0]
    return Resident(resident_id=r[0], unit_id=r[1], property_id=r[2], first_name=r[3], last_name=r[4], rent=r[5], move_in_date=str(r[6]), move_out_date=str(r[7]) if r[7] else None)

@app.post("/properties", response_model=Property)
def create_property(prop: Property, db = Depends(get_db)):
    cursor = db.cursor()
    try:
        cursor.execute(
            "INSERT INTO properties (property_id, property_name, owner, address) VALUES (%s, %s, %s, %s)",
            (prop.property_id, prop.property_name, prop.owner, prop.address),
        )
        db.commit()
        return prop
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()

@app.get("/units", response_model=List[Unit])
def list_units(db = Depends(get_db)):
    try:
        records = run_query(
            db, "SELECT unit_id, unit_number, property_id, unit_status, occupied FROM units"
        )
        return [Unit(unit_id=r[0], unit_number=r[1], property_id=r[2], unit_status=r[3], occupied=r[4]) for r in records]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.post("/units", response_model=Unit)
def create_unit(unit: Unit, db = Depends(get_db)):
    # Business rule: Inactive units cannot be occupied
    if unit.unit_status == 'inactive' and unit.occupied:
        raise HTTPException(status_code=400, detail="Inactive units cannot be occupied")
    
    cursor = db.cursor()
    try:
        cursor.execute(
            "INSERT INTO units (unit_id, unit_number, property_id, unit_status, occupied) VALUES (%s, %s, %s, %s, %s)",
            (unit.unit_id, unit.unit_number, unit.property_id, unit.unit_status, unit.occupied),
        )
        db.commit()
        return unit
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()

@app.get("/residents", response_model=List[Resident])
def list_residents(db = Depends(get_db)):
    records = run_query(
        db, "SELECT resident_id, unit_id, property_id, first_name, last_name, rent, move_in_date, move_out_date FROM residents"
    )
    return [Resident(resident_id=r[0], unit_id=r[1], property_id=r[2], first_name=r[3], last_name=r[4], rent=r[5], move_in_date=str(r[6]), move_out_date=str(r[7]) if r[7] else None) for r in records]


@app.get("/rentroll/{property_id}", response_model=List[RentRoll])
def list_rentroll(property_id: int, start_date: Optional[date] = None, end_date: Optional[date] = None, db = Depends(get_db)):
    sql = "SELECT rentroll_id, date, property_id, unit_id, unit_number, resident_id, resident_name, rent, unit_status FROM rentroll WHERE property_id = %s"
    params: list = [property_id]
    if start_date:
        sql += " AND date >= %s"
        params.append(start_date)
    if end_date:
        sql += " AND date <= %s"
        params.append(end_date)
    sql += " ORDER BY date"
    records = run_query(db, sql, tuple(params))
    return [RentRoll(date=str(r[1]), property_id=r[2], unit_id=r[3], unit_number=r[4], resident_id=r[5], resident_name=r[6], rent=r[7], unit_status=r[8]) for r in records]

@app.get("/rentroll", response_model=List[RentRoll])
def list_rentrolls(start_date: Optional[date] = None, end_date: Optional[date] = None, db = Depends(get_db)):
    sql = "SELECT rentroll_id, date, property_id, unit_id, unit_number, resident_id, resident_name, rent, unit_status FROM rentroll WHERE 1=1"
    params: list = []
    if start_date:
        sql += " AND date >= %s"
        params.append(start_date)
    if end_date:
        sql += " AND date <= %s"
        params.append(end_date)
    sql += " ORDER BY date"
    records = run_query(db, sql, tuple(params) if params else None)
    return [RentRoll(date=str(r[1]), property_id=r[2], unit_id=r[3], unit_number=r[4], resident_id=r[5], resident_name=r[6], rent=r[7], unit_status=r[8]) for r in records]

@app.get("/resident_scd", response_model=List[ResidentSCD2])
def list_resident_scd(db = Depends(get_db)):
    """Get all Resident SCD (Slowly Changing Dimension) records"""
    records = run_query(
        db, "SELECT resident_scd2_id, resident_id, unit_id, first_name, last_name, move_in_date, move_out_date, rent, effective_date, expiration_date, is_current FROM residents_scd2 ORDER BY effective_date DESC"
    )
    return [ResidentSCD2(resident_scd2_id=r[0], resident_id=r[1], unit_id=r[2], first_name=r[3], last_name=r[4], move_in_date=str(r[5]) if r[5] else None, move_out_date=str(r[6]) if r[6] else None, rent=r[7], effective_date=str(r[8]) if r[8] else None, expiration_date=str(r[9]) if r[9] else None, is_current=r[10]) for r in records]

@app.get("/unit_scd", response_model=List[UnitSCD2])
def list_unit_scd(db = Depends(get_db)):
    """Get all Unit SCD (Slowly Changing Dimension) records"""
    records = run_query(
        db, "SELECT unit_scd2_id, unit_id, property_id, unit_number, unit_status, occupied, effective_date, expiration_date, is_current FROM units_scd2 ORDER BY effective_date DESC"
    )
    return [UnitSCD2(unit_scd2_id=r[0], unit_id=r[1], property_id=r[2], unit_number=r[3], unit_status=r[4], occupied=r[5], effective_date=str(r[6]) if r[6] else None, expiration_date=str(r[7]) if r[7] else None, is_current=r[8]) for r in records]  

@app.get("/property_scd", response_model=List[PropertySCD2])
def list_property_scd(db = Depends(get_db)):
    """Get all Property SCD (Slowly Changing Dimension) records"""
    records = run_query(
        db, "SELECT property_scd2_id, property_id, property_name, address, effective_date, expiration_date, is_current FROM properties_scd2 ORDER BY effective_date DESC"
    )
    return [PropertySCD2(property_scd2_id=r[0], property_id=r[1], property_name=r[2], address=r[3], effective_date=str(r[4]) if r[4] else None, expiration_date=str(r[5]) if r[5] else None, is_current=r[6]) for r in records]

@app.patch("/properties/{property_id}")
def patch_property(property_id: int, patch: PropertyPatch, db = Depends(get_db)):
    updated = apply_patch(db, "properties", "property_id", property_id, patch.model_dump(exclude_unset=True))
    if not updated:
        raise HTTPException(status_code=404, detail="Property not found")
    return {"updated": property_id}

@app.patch("/units/{unit_id}")
def patch_unit(unit_id: int, patch: UnitPatch, db = Depends(get_db)):
    updated = apply_patch(db, "units", "unit_id", unit_id, patch.model_dump(exclude_unset=True))
    if not updated:
        raise HTTPException(status_code=404, detail="Unit not found")
    return {"updated": unit_id}

@app.patch("/residents/{resident_id}")
def patch_resident(resident_id: int, patch: ResidentPatch, db = Depends(get_db)):
    rows = run_query(db, "SELECT unit_id, move_in_date FROM residents WHERE resident_id = %s", (resident_id,))
    if not rows:
        raise HTTPException(status_code=404, detail="Resident not found")
    old_unit_id, old_move_in_date = rows[0]

    fields = patch.model_dump(exclude_unset=True)

    if "move_out_date" in fields and fields["move_out_date"] is not None:
        effective_move_in = fields.get("move_in_date", old_move_in_date)
        if effective_move_in and fields["move_out_date"] < effective_move_in:
            raise HTTPException(status_code=422, detail="move_out_date cannot be before move_in_date")

    updated = apply_patch(db, "residents", "resident_id", resident_id, fields)
    if not updated:
        raise HTTPException(status_code=404, detail="Resident not found")

    rentroll_fields = {"rent", "move_in_date", "move_out_date", "unit_id", "first_name", "last_name"}
    if fields.keys() & rentroll_fields and old_unit_id:
        new_unit_id = fields.get("unit_id", old_unit_id)
        start_date = fields.get("move_in_date", old_move_in_date) or old_move_in_date
        recompute_rentroll_for_unit(db, old_unit_id, start_date, date.today())
        if new_unit_id != old_unit_id:
            recompute_rentroll_for_unit(db, new_unit_id, start_date, date.today())

    return {"updated": resident_id}

@app.post("/move_in")
def move_in_resident(unit_id: int, resident: ResidentPatch, resident_id: Optional[int] = None, db = Depends(get_db)):
    cursor = db.cursor()
    try:
        cursor.execute("SELECT unit_status, occupied, property_id FROM units WHERE unit_id = %s", (unit_id,))
        unit_row = cursor.fetchone()
        if not unit_row:
            raise HTTPException(status_code=404, detail="Unit not found")
        if unit_row[0] == 'inactive':
            raise HTTPException(status_code=400, detail="Inactive units cannot be occupied")
        if unit_row[1]:
            raise HTTPException(status_code=400, detail="Unit is already occupied")
        property_id = unit_row[2]

        fields = resident.model_dump(exclude_unset=True)

        if resident_id is not None:
            if "rent" not in fields:
                raise HTTPException(status_code=422, detail="rent is required")
            if fields["rent"] <= 0:
                raise HTTPException(status_code=422, detail="rent must be greater than 0")
            cursor.execute("SELECT unit_id, move_out_date FROM residents WHERE resident_id = %s", (resident_id,))
            row = cursor.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Resident not found")
            if row[0] is not None and row[1] is None:
                raise HTTPException(status_code=400, detail="Resident is already assigned to a unit")
            patch = {**fields, "unit_id": unit_id, "property_id": property_id, "move_out_date": None}
            apply_patch(db, "residents", "resident_id", resident_id, patch)
        else:
            missing = [f for f in ("first_name", "last_name", "rent", "move_in_date") if f not in fields]
            if missing:
                raise HTTPException(status_code=422, detail=f"Required fields for new resident: {', '.join(missing)}")
            if fields["rent"] <= 0:
                raise HTTPException(status_code=422, detail="rent must be greater than 0")
            cursor.execute("SELECT COALESCE(MAX(resident_id), 0) + 1 FROM residents")
            resident_id = cursor.fetchone()[0]
            cursor.execute(
                "INSERT INTO residents (resident_id, unit_id, property_id, first_name, last_name, rent, move_in_date) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (resident_id, unit_id, property_id, fields["first_name"], fields["last_name"], fields["rent"], fields["move_in_date"]),
            )
        apply_patch(db, "units", "unit_id", unit_id, {"occupied": True})

        rows = run_query(db, "SELECT move_in_date FROM residents WHERE resident_id = %s", (resident_id,))
        move_in_date = rows[0][0] if rows else date.today()
        recompute_rentroll_for_unit(db, unit_id, move_in_date, date.today())

        return {"resident_id": resident_id, "unit_id": unit_id, "property_id": property_id}
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()

@app.post("/move_out")
def move_out_resident(resident_id: int, move_out_date: date = date.today(), db = Depends(get_db)):
    rows = run_query(db, "SELECT unit_id, move_in_date FROM residents WHERE resident_id = %s", (resident_id,))
    if not rows:
        raise HTTPException(status_code=404, detail="Resident not found")
    unit_id, move_in_date = rows[0]
    if move_in_date and move_out_date < move_in_date:
        raise HTTPException(status_code=422, detail="move_out_date cannot be before move_in_date")
    apply_patch(db, "residents", "resident_id", resident_id, {"move_out_date": move_out_date, "rent": 0})
    apply_patch(db, "units", "unit_id", unit_id, {"occupied": False})
    recompute_rentroll_for_unit(db, unit_id, move_out_date, date.today())

@app.post("/update_rent", response_model=Resident)
def update_rent(resident_id: int, new_rent: float, db = Depends(get_db)):
    if new_rent <= 0:
        raise HTTPException(status_code=422, detail="rent must be greater than 0")
    updated = apply_patch(db, "residents", "resident_id", resident_id, {"rent": new_rent})
    if not updated:
        raise HTTPException(status_code=404, detail="Resident not found")
    rows = run_query(db, "SELECT resident_id, unit_id, property_id, first_name, last_name, rent, move_in_date, move_out_date FROM residents WHERE resident_id = %s", (resident_id,))
    recompute_rentroll_for_unit(db, rows[0][1], date.today(), date.today())
    r = rows[0]
    return Resident(
        resident_id=r[0], 
        unit_id=r[1], 
        property_id=r[2], 
        first_name=r[3], 
        last_name=r[4], 
        rent=r[5], 
        move_in_date=str(r[6]), 
        move_out_date=str(r[7]) if r[7] else None
        )

@app.post("/update_unit_status", response_model=Unit)
def update_unit_status(unit_id: int, new_status: str, db = Depends(get_db)):
    if new_status not in ('active', 'inactive'):
        raise HTTPException(status_code=400, detail="Invalid unit status")
    if new_status == 'inactive':
        rows = run_query(db, "SELECT occupied FROM units WHERE unit_id = %s", (unit_id,))
        if not rows:
            raise HTTPException(status_code=404, detail="Unit not found")
        if rows[0][0]:
            raise HTTPException(status_code=400, detail="Cannot deactivate an occupied unit")
    updated = apply_patch(db, "units", "unit_id", unit_id, {"unit_status": new_status})
    if not updated:
        raise HTTPException(status_code=404, detail="Unit not found")
    recompute_rentroll_for_unit(db, unit_id, date.today(), date.today())
    rows = run_query(db, "SELECT unit_id, unit_number, property_id, unit_status, occupied FROM units WHERE unit_id = %s", (unit_id,))
    r = rows[0]
    return Unit(unit_id=r[0], unit_number=r[1], property_id=r[2], unit_status=r[3], occupied=r[4])

@app.get("/overview")
def get_joined_entities(db = Depends(get_db)):
    """Get the current joined view of all properties, units, and residents"""
    records = run_query(
        db, """
        SELECT p.property_id, p.property_name, p.owner,
               u.unit_id, u.unit_number, u.unit_status, u.occupied,
               r.rent, r.resident_id,
               r.first_name || ' ' || r.last_name AS resident_name
        FROM properties p
        JOIN units u ON u.property_id = p.property_id
        LEFT JOIN residents r ON r.unit_id = u.unit_id AND r.move_out_date IS NULL
        ORDER BY p.property_id, u.unit_id
        """
    )
    return [RentRoll(property_id=r[0], unit_id=r[3], unit_number=r[4], unit_status=r[5], rent=r[7], resident_id=r[8], resident_name=r[9]) for r in records]

@app.get("/kpis/{start_date}/{end_date}")
def get_kpis(start_date: str, end_date: str, db = Depends(get_db)):
    """Aggregate KPIs over a date range, broken down per property."""
    rows = run_query(
        db,
        """
        WITH date_range AS (
            SELECT
                %s::DATE                                        AS start_date,
                %s::DATE                                        AS end_date,
                (%s::DATE - %s::DATE + 1)                      AS num_days
        ),
        property_units AS (
            -- Total units per property including those with no rentroll activity
            SELECT
                p.property_id,
                p.property_name,
                COUNT(u.unit_id)                               AS total_units
            FROM properties p
            LEFT JOIN units u ON u.property_id = p.property_id
            GROUP BY p.property_id, p.property_name
        ),
        rentroll_agg AS (
            SELECT
                rr.property_id,
                COUNT(*) FILTER (
                    WHERE rr.unit_status = 'active'
                      AND rr.resident_name IS NOT NULL
                )                                               AS occupied_unit_days,

                -- Daily rent × occupied days:
                -- rent is monthly, so divide by days in that month first
                SUM(
                    rr.rent
                    / DATE_PART('days', DATE_TRUNC('month', rr.date)
                        + INTERVAL '1 month' - INTERVAL '1 day')
                ) FILTER (
                    WHERE rr.unit_status = 'active'
                      AND rr.resident_name IS NOT NULL
                )                                               AS total_rent,

                -- Average monthly rent: just average the rent column directly
                -- across distinct occupied unit-days (rent is already monthly)
                AVG(rr.rent) FILTER (
                    WHERE rr.unit_status = 'active'
                      AND rr.resident_name IS NOT NULL
                )                                               AS avg_monthly_rent

            FROM rentroll rr
            WHERE rr.date BETWEEN %s AND %s
            GROUP BY rr.property_id
        ),
        move_ins AS (
            SELECT property_id, COUNT(*)                       AS move_ins
            FROM residents
            WHERE move_in_date BETWEEN %s AND %s
            GROUP BY property_id
        ),
        move_outs AS (
            SELECT property_id, COUNT(*)                       AS move_outs
            FROM residents
            WHERE move_out_date BETWEEN %s AND %s
            GROUP BY property_id
        )
        SELECT
            pu.property_id,
            pu.property_name,
            pu.total_units,
            (pu.total_units * dr.num_days)                     AS possible_unit_days,
            COALESCE(ra.occupied_unit_days, 0)                 AS occupied_unit_days,
            COALESCE(mi.move_ins, 0)                           AS move_ins,
            COALESCE(mo.move_outs, 0)                          AS move_outs,
            ROUND(
                COALESCE(ra.occupied_unit_days, 0)::NUMERIC
                / NULLIF(pu.total_units * dr.num_days, 0),
                4
            )                                                  AS occupancy_rate,
            ROUND(
                COALESCE(ra.avg_monthly_rent, 0)::NUMERIC,
                2
            )                                                  AS average_rent_per_occupied_unit,
            ROUND(
                COALESCE(ra.total_rent, 0)::NUMERIC,
                2
            )                                                  AS total_rent_accrued
        FROM property_units pu
        CROSS JOIN date_range dr
        LEFT JOIN rentroll_agg ra ON ra.property_id = pu.property_id
        LEFT JOIN move_ins     mi ON mi.property_id = pu.property_id
        LEFT JOIN move_outs    mo ON mo.property_id = pu.property_id
        ORDER BY pu.property_id
        """,
        (
            start_date, end_date,   # date_range: start, end
            end_date, start_date,   # num_days: end - start + 1
            start_date, end_date,   # rentroll_agg
            start_date, end_date,   # move_ins
            start_date, end_date,   # move_outs
        ),
    )

    return [
        {
            "property_id":                    row[0],
            "property_name":                  row[1],
            "total_units":                    row[2],
            "num_days":                       row[3],
            "possible_unit_days":             row[4],
            "occupied_unit_days":             row[5],
            "move_ins":                       row[6],
            "move_outs":                      row[7],
            "occupancy_rate":                 float(row[8]) if row[8] is not None else 0,
            "average_rent_across_occupied_units": float(row[9]) if row[9] is not None else 0,
            "total_rent_accrued_in_time_range":             float(row[10]) if row[10] is not None else 0,
        }
        for row in rows
    ]

# root endpoint
@app.get("/")
def root():
    return {"message": "Welltower PostgreSQL API"}

# To run:
# pip install -r requirements.txt
# Set environment variables:
#   DB_HOST=localhost
#   DB_PORT=5432
#   DB_NAME=welltower
#   DB_USER=postgres
#   DB_PASSWORD=postgres
# For empty database: LOAD_FAKE_DATA=false uvicorn api:app --reload --host 0.0.0.0 --port 8000
# For database with fake data: uvicorn api:app --reload --host 0.0.0.0 --port 8000

# @app.delete("/properties/{property_id}")
# def delete_property(property_id: int, db = Depends(get_db)):
#     cursor = db.cursor()
#     try:
#         cursor.execute("DELETE FROM properties WHERE property_id = %s", (property_id,))
#         db.commit()
#         return {"deleted": property_id}
#     finally:
#         cursor.close()

# @app.delete("/units/{unit_id}")
# def delete_unit(unit_id: int, db = Depends(get_db)):
#     cursor = db.cursor()
#     try:
#         cursor.execute("DELETE FROM units WHERE unit_id = %s", (unit_id,))
#         db.commit()
#         return {"deleted": unit_id}
#     finally:
#         cursor.close()

# @app.delete("/residents/{resident_id}")
# def delete_resident(resident_id: int, db = Depends(get_db)):
#     cursor = db.cursor()
#     try:
#         cursor.execute("DELETE FROM residents WHERE resident_id = %s", (resident_id,))
#         db.commit()
#         return {"deleted": resident_id}
#     finally:
#         cursor.close()