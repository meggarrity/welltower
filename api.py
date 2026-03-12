from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from typing import Optional, List
from seed_data import seed_database

app = FastAPI(title="Welltower API")

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
        
        # set up triggers
        cursor.execute("""
        -- Residents: fire on any insert, update, or delete
        CREATE TRIGGER trg_residents_after
            AFTER INSERT OR UPDATE OR DELETE ON residents
            FOR EACH ROW EXECUTE FUNCTION trg_residents_scd2();

        -- Units: fire on insert, update, or delete
        CREATE TRIGGER trg_units_after
            AFTER INSERT OR UPDATE OR DELETE ON units
            FOR EACH ROW EXECUTE FUNCTION trg_units_scd2();

        -- Properties: fire only on updates (name/address changes)
        CREATE TRIGGER trg_properties_after
            AFTER UPDATE ON properties
            FOR EACH ROW
            WHEN (
                OLD.property_name IS DISTINCT FROM NEW.property_name OR
                OLD.address       IS DISTINCT FROM NEW.address
            )
            EXECUTE FUNCTION trg_properties_scd2();
        """)


        # create rentroll table
        cursor.execute("""
            CREATE OR REPLACE VIEW rent_roll_daily AS
            WITH date_spine AS (
                -- Generate every date from your earliest record to today
                SELECT generate_series(
                    (SELECT MIN(effective_date) FROM rent_roll_scd2),
                    CURRENT_DATE,
                    INTERVAL '1 day'
                )::DATE AS roll_date
            ),
            units_dates AS (
                -- Cross join every unit with every date
                SELECT
                    u.unit_id,
                    d.roll_date
                FROM units u
                CROSS JOIN date_spine d
            )
            SELECT
                ud.roll_date,
                scd.property_id,
                scd.property_name,
                scd.address,
                scd.unit_id,
                scd.unit_number,
                scd.bedrooms,
                scd.bathrooms,
                scd.sqft,
                scd.resident_id,
                scd.tenant_name,
                scd.move_in_date,
                scd.move_out_date,
                scd.monthly_rent,
                scd.occupancy_status,
                scd.change_reason,
                -- Flags useful for reporting
                CASE WHEN ud.roll_date = scd.effective_date THEN TRUE ELSE FALSE END AS is_change_day,
                scd.effective_date,
                scd.expiry_date
            FROM units_dates ud
            JOIN rent_roll_scd2 scd
                ON  ud.unit_id   = scd.unit_id
                AND ud.roll_date BETWEEN scd.effective_date AND scd.expiry_date
            ORDER BY ud.roll_date, scd.property_id, scd.unit_number;

        """)
        
        # rentroll cron
        cursor.execute("""
            -- Runs at midnight via pg_cron or your scheduler
            -- Only needed to "touch" the date spine for the daily view
            -- The SCD2 data itself is already correct from triggers

            SELECT cron.schedule(
                'daily-rent-roll-heartbeat',
                '0 0 * * *',  -- midnight every day
                $$
                    -- Re-materialize the daily view if you store it as a table
                    REFRESH MATERIALIZED VIEW rent_roll_daily;
                $$
            );
                          """)

        # resident trigger
        cursor.execute("""
                       CREATE OR REPLACE FUNCTION trg_residents_scd2()
                        RETURNS TRIGGER LANGUAGE plpgsql AS $$
                        DECLARE
                            v_unit        units%ROWTYPE;
                            v_property    properties%ROWTYPE;
                            v_new_status  VARCHAR(20);
                            v_resident_id VARCHAR(20);
                            v_tenant_name VARCHAR(100);
                            v_move_in_date DATE;
                            v_move_out_date   DATE;
                            v_monthly_rent NUMERIC(10,2);
                        BEGIN
                            -- Work out which unit_id we're dealing with
                            -- (on DELETE, use OLD; otherwise use NEW)
                            DECLARE
                                v_unit_id VARCHAR(20) := COALESCE(NEW.unit_id, OLD.unit_id);
                            BEGIN
                                -- Fetch current unit and property
                                SELECT * INTO v_unit     FROM units      WHERE unit_id   = v_unit_id;
                                SELECT * INTO v_property FROM properties WHERE property_id = v_unit.property_id;

                                -- Determine the active resident for this unit after the change
                                SELECT
                                    r.resident_id,
                                    r.tenant_name,
                                    r.move_in_date,
                                    r.move_out_date,
                                    r.monthly_rent,
                                    r.status
                                INTO
                                    v_resident_id,
                                    v_tenant_name,
                                    v_move_in_date,
                                    v_move_out_date,
                                    v_monthly_rent,
                                    v_new_status
                                FROM residents r
                                WHERE r.unit_id     = v_unit_id
                                AND r.status     != 'inactive'
                                AND r.move_in_date <= CURRENT_DATE
                                AND (r.move_out_date IS NULL OR r.move_out_date >= CURRENT_DATE)
                                ORDER BY r.move_in_date DESC
                                LIMIT 1;

                                -- If no active resident found, unit is vacant
                                IF v_resident_id IS NULL THEN
                                    v_new_status := 'vacant';
                                END IF;

                                -- Step 1: Expire the current SCD2 row for this unit
                                UPDATE rent_roll_scd2
                                SET
                                    expiry_date = CURRENT_DATE - 1,
                                    is_current  = FALSE
                                WHERE unit_id    = v_unit_id
                                AND is_current = TRUE
                                -- Only expire if something actually changed
                                AND (
                                    COALESCE(resident_id, '')        <> COALESCE(v_resident_id, '')    OR
                                    COALESCE(monthly_rent, 0)        <> COALESCE(v_monthly_rent, 0)    OR
                                    COALESCE(occupancy_status, '')   <> COALESCE(v_new_status, '')     OR
                                    COALESCE(move_out_date::TEXT, '')    <> COALESCE(v_move_out_date::TEXT, '') OR
                                    COALESCE(tenant_name, '')        <> COALESCE(v_tenant_name, '')
                                );

                                -- Step 2: Insert new current row only if something changed
                                -- (i.e. the UPDATE above actually expired a row)
                                IF FOUND THEN
                                    INSERT INTO rent_roll_scd2 (
                                        unit_id,        property_id,    property_name,  address,
                                        unit_number,    bedrooms,       bathrooms,      sqft,
                                        resident_id,    tenant_name,    move_in_date,    move_out_date,
                                        monthly_rent,   occupancy_status,
                                        effective_date, expiry_date,    is_current,     change_reason
                                    ) VALUES (
                                        v_unit_id,
                                        v_property.property_id,
                                        v_property.property_name,
                                        v_property.address,
                                        v_unit.unit_number,
                                        v_unit.bedrooms,
                                        v_unit.bathrooms,
                                        v_unit.sqft,
                                        v_resident_id,
                                        v_tenant_name,
                                        v_move_in_date,
                                        v_move_out_date,
                                        v_monthly_rent,
                                        v_new_status,
                                        CURRENT_DATE,
                                        '9999-12-31',
                                        TRUE,
                                        CASE
                                            WHEN TG_OP = 'DELETE'              THEN 'move_out'
                                            WHEN OLD.unit_id IS NULL           THEN 'move_in'
                                            WHEN OLD.status  != NEW.status     THEN 'status_change'
                                            WHEN OLD.monthly_rent != NEW.monthly_rent THEN 'rent_change'
                                            ELSE 'data_change'
                                        END
                                    );
                                END IF;
                            END;

                            RETURN COALESCE(NEW, OLD);
                        END;
                        $$;
                          """)

        # unit trigger
        cursor.execute("""
                       CREATE OR REPLACE FUNCTION trg_units_scd2()
                        RETURNS TRIGGER LANGUAGE plpgsql AS $$
                        DECLARE
                            v_property    properties%ROWTYPE;
                            v_resident    residents%ROWTYPE;
                        BEGIN
                            SELECT * INTO v_property FROM properties
                            WHERE property_id = COALESCE(NEW.property_id, OLD.property_id);

                            -- Get active resident if any
                            SELECT * INTO v_resident FROM residents
                            WHERE unit_id    = COALESCE(NEW.unit_id, OLD.unit_id)
                            AND status    != 'inactive'
                            AND move_in_date <= CURRENT_DATE
                            AND (move_out_date IS NULL OR move_out_date >= CURRENT_DATE)
                            ORDER BY move_in_date DESC LIMIT 1;

                            -- Expire old row
                            UPDATE rent_roll_scd2
                            SET expiry_date = CURRENT_DATE - 1, is_current = FALSE
                            WHERE unit_id    = COALESCE(NEW.unit_id, OLD.unit_id)
                            AND is_current = TRUE;

                            -- Insert new row (always insert on unit change)
                            IF TG_OP != 'DELETE' THEN
                                INSERT INTO rent_roll_scd2 (
                                    unit_id,        property_id,        property_name,  address,
                                    unit_number,    bedrooms,            bathrooms,      sqft,
                                    resident_id,    tenant_name,         move_in_date,    move_out_date,
                                    monthly_rent,   occupancy_status,
                                    effective_date, expiry_date,         is_current,     change_reason
                                ) VALUES (
                                    NEW.unit_id,
                                    v_property.property_id,
                                    v_property.property_name,
                                    v_property.address,
                                    NEW.unit_number,
                                    NEW.bedrooms,
                                    NEW.bathrooms,
                                    NEW.sqft,
                                    v_resident.resident_id,
                                    v_resident.tenant_name,
                                    v_resident.move_in_date,
                                    v_resident.move_out_date,
                                    v_resident.monthly_rent,
                                    COALESCE(v_resident.status, 'vacant'),
                                    CURRENT_DATE,
                                    '9999-12-31',
                                    TRUE,
                                    CASE WHEN TG_OP = 'INSERT' THEN 'new_unit' ELSE 'unit_change' END
                                );
                            END IF;

                            RETURN COALESCE(NEW, OLD);
                        END;
                        $$;
                          """)
        
        # create SCD table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS scd (
          scd_id SERIAL PRIMARY KEY,
          property_id INTEGER,
          unit_id INTEGER,
          resident_id INTEGER,
          property_name TEXT,
          owner TEXT,
          unit_number TEXT,
          unit_status TEXT,
          occupied BOOLEAN,
          rent FLOAT,
          first_name TEXT,
          last_name TEXT,
          effective_from DATE,
          effective_to DATE,
          current_flag BOOLEAN DEFAULT true,
          FOREIGN KEY (property_id) REFERENCES properties(property_id),
          FOREIGN KEY (unit_id) REFERENCES units(unit_id),
          FOREIGN KEY (resident_id) REFERENCES residents(resident_id)
        );
        """)
        
        # Create view for joined entities
        cursor.execute("""
        CREATE OR REPLACE VIEW vw_joined_entities AS
        SELECT
          p.property_id,
          p.property_name,
          p.owner,
          u.unit_id,
          u.unit_number,
          u.unit_status,
          u.occupied,
          u.rent,
          r.resident_id,
          r.first_name,
          r.last_name
        FROM properties p
        LEFT JOIN units u ON p.property_id = u.property_id
        LEFT JOIN residents r ON u.unit_id = r.unit_id;
        """)
        
        # Create or replace function for inactive unit occupancy check
        cursor.execute("""
        CREATE OR REPLACE FUNCTION check_unit_occupancy()
        RETURNS TRIGGER AS $$
        BEGIN
          IF NEW.unit_status = 'inactive' AND NEW.occupied = true THEN
            RAISE EXCEPTION 'Inactive units cannot be occupied';
          END IF;
          RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """)
        
        # Drop triggers if they exist
        cursor.execute("DROP TRIGGER IF EXISTS units_occupancy_check ON units;")
        cursor.execute("DROP TRIGGER IF EXISTS units_occupancy_update_check ON units;")
        cursor.execute("DROP TRIGGER IF EXISTS trg_scd2_backfill ON rent_roll_scd2;")
        cursor.execute("DROP FUNCTION IF EXISTS trg_scd2_backfill_daily();")

        # Create trigger for daily rent roll snapshot
        cursor.execute("""
                       SELECT cron.schedule(
                        'rent-roll-daily-snapshot',
                        '59 23 * * *',
                        $$
                            INSERT INTO rent_roll_daily (
                                roll_date,        unit_id,          property_id,      property_name,
                                address,          unit_number,       bedrooms,         bathrooms,
                                sqft,             resident_id,       tenant_name,      move_in_date,
                                move_out_date,        monthly_rent,      occupancy_status,
                                change_reason,    is_change_day,     scd2_id
                            )
                            SELECT
                                CURRENT_DATE,
                                scd.unit_id,
                                scd.property_id,
                                scd.property_name,
                                scd.address,
                                scd.unit_number,
                                scd.bedrooms,
                                scd.bathrooms,
                                scd.sqft,
                                scd.resident_id,
                                scd.tenant_name,
                                scd.move_in_date,
                                scd.move_out_date,
                                scd.monthly_rent,
                                scd.occupancy_status,
                                scd.change_reason,
                                -- mark units where a new SCD2 row became effective today
                                CASE WHEN scd.effective_date = CURRENT_DATE THEN TRUE ELSE FALSE END,
                                scd.scd2_id
                            FROM rent_roll_scd2 scd
                            WHERE scd.is_current = TRUE
                            ON CONFLICT (roll_date, unit_id) DO NOTHING;
                                $$
                            );        
                       """
                       )

        # Create stored procedure for backfilling rent roll daily snapshots
        cursor.execute(
            """CREATE OR REPLACE PROCEDURE backfill_rent_roll_daily(
            p_start_date DATE,
            p_end_date   DATE DEFAULT NULL
        )
        LANGUAGE plpgsql AS $$
        DECLARE
            v_date      DATE := p_start_date;
            v_end_date  DATE := COALESCE(p_end_date, p_start_date);
        BEGIN
            WHILE v_date <= v_end_date LOOP

        INSERT INTO rent_roll_daily (
            roll_date,        unit_id,          property_id,      property_name,
            address,          unit_number,       bedrooms,         bathrooms,
            sqft,             resident_id,       tenant_name,      move_in_date,
            move_out_date,        monthly_rent,      occupancy_status,
            change_reason,    is_change_day,     scd2_id
        )
        SELECT
            v_date,
            scd.unit_id,
            scd.property_id,
            scd.property_name,
            scd.address,
            scd.unit_number,
            scd.bedrooms,
            scd.bathrooms,
            scd.sqft,
            scd.resident_id,
            scd.tenant_name,
            scd.move_in_date,
            scd.move_out_date,
            scd.monthly_rent,
            scd.occupancy_status,
            scd.change_reason,
            CASE WHEN scd.effective_date = v_date THEN TRUE ELSE FALSE END,
            scd.scd2_id
        FROM rent_roll_scd2 scd
        WHERE scd.effective_date <= v_date
          AND scd.expiry_date    >= v_date
        ON CONFLICT (roll_date, unit_id) DO NOTHING;

        RAISE NOTICE 'Backfilled %', v_date;
        v_date := v_date + 1;
    END LOOP;
END;
$$;
        """)
        
        # Create triggers for unit insert occupancy check
        cursor.execute("""
        CREATE TRIGGER units_occupancy_check
        BEFORE INSERT ON units
        FOR EACH ROW
        EXECUTE FUNCTION check_unit_occupancy();
        """)
        
        # create trigger for unit update occupancy check
        cursor.execute("""
        CREATE TRIGGER units_occupancy_update_check
        BEFORE UPDATE ON units
        FOR EACH ROW
        EXECUTE FUNCTION check_unit_occupancy();
        """)
        
        # SCD tracking function
        cursor.execute("""
        CREATE OR REPLACE FUNCTION track_scd_change()
        RETURNS TRIGGER AS $$
        BEGIN
          INSERT INTO scd (property_id, property_name, owner, unit_id, unit_number, unit_status, occupied, rent, resident_id, first_name, last_name, effective_from, effective_to, current_flag)
          SELECT p.property_id, p.property_name, p.owner, u.unit_id, u.unit_number, u.unit_status, u.occupied, u.rent, r.resident_id, r.first_name, r.last_name, CURRENT_DATE, NULL, true
          FROM vw_joined_entities v
          LEFT JOIN properties p ON v.property_id = p.property_id
          LEFT JOIN units u ON v.unit_id = u.unit_id
          LEFT JOIN residents r ON v.resident_id = r.resident_id
          WHERE p.property_id = COALESCE(NEW.property_id, OLD.property_id);
          RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """)
        
        conn.commit()
        print(f"Database '{db_name}' initialized successfully")
    except Exception as e:
        conn.rollback()
        print(f"Error initializing database: {e}")
        raise
    finally:
        cursor.close()

# -- startup event ---------------------------------------------------
@app.on_event("startup")
def startup_event():
    try:
        init_database(DB_NAME)
        
        # Check if LOAD_FAKE_DATA is set
        if os.getenv("LOAD_FAKE_DATA") != "false":
            # Use the seed_data module to generate and insert realistic fake data
            seed_database(
                db_name=DB_NAME,
                num_properties=int(os.getenv("NUM_PROPERTIES", "3")),
                min_units_per_property=int(os.getenv("MIN_UNITS_PER_PROPERTY", "2")),
                max_units_per_property=int(os.getenv("MAX_UNITS_PER_PROPERTY", "5")),
                num_residents_ratio=float(os.getenv("RESIDENTS_RATIO", "0.7"))
            )
        else:
            print("Database tables created (no fake data loaded).")
    except Exception as e:
        print(f"Startup error: {e}")

# -- models ---------------------------------------------------------------

class Property(BaseModel):
    property_id: int
    property_name: str
    owner: str

class Unit(BaseModel):
    unit_id: int
    unit_number: str
    property_id: int
    unit_status: str
    occupied: bool
    rent: float

class Resident(BaseModel):
    resident_id: int
    unit_id: int
    property_id: int
    first_name: str
    last_name: str
    move_in_date: str
    move_out_date: Optional[str] = None

class RentRole(BaseModel):
    scd_id: int
    date: Optional[str] = None
    property_id: Optional[int] = None
    unit_id: Optional[int] = None
    resident_id: Optional[int] = None
    property_name: Optional[str] = None
    owner: Optional[str] = None
    unit_number: Optional[str] = None
    unit_status: Optional[str] = None
    occupied: Optional[bool] = None
    rent: Optional[float] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    effective_from: Optional[str] = None
    effective_to: Optional[str] = None
    current_flag: Optional[bool] = None

# -- helper functions ------------------------------------------------------

def run_query(conn, sql: str, params: Optional[tuple] = None):
    cursor = conn.cursor()
    try:
        cursor.execute(sql, params or ())
        return cursor.fetchall()
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

@app.post("/properties", response_model=Property)
def create_property(prop: Property, db = Depends(get_db)):
    cursor = db.cursor()
    try:
        cursor.execute(
            "INSERT INTO properties (property_id, property_name, owner) VALUES (%s, %s, %s)",
            (prop.property_id, prop.property_name, prop.owner),
        )
        db.commit()
        return prop
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()

@app.delete("/properties/{property_id}")
def delete_property(property_id: int, db = Depends(get_db)):
    cursor = db.cursor()
    try:
        cursor.execute("DELETE FROM properties WHERE property_id = %s", (property_id,))
        db.commit()
        return {"deleted": property_id}
    finally:
        cursor.close()

@app.get("/units", response_model=List[Unit])
def list_units(db = Depends(get_db)):
    try:
        records = run_query(
            db, "SELECT unit_id, unit_number, property_id, unit_status, occupied, rent FROM units"
        )
        return [Unit(unit_id=r[0], unit_number=r[1], property_id=r[2], unit_status=r[3], occupied=r[4], rent=r[5]) for r in records]
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
            "INSERT INTO units (unit_id, unit_number, property_id, unit_status, occupied, rent) VALUES (%s, %s, %s, %s, %s, %s)",
            (unit.unit_id, unit.unit_number, unit.property_id, unit.unit_status, unit.occupied, unit.rent),
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
        db, "SELECT resident_id, unit_id, property_id, first_name, last_name, move_in_date, move_out_date FROM residents"
    )
    return [Resident(resident_id=r[0], unit_id=r[1], property_id=r[2], first_name=r[3], last_name=r[4], move_in_date=str(r[5]), move_out_date=str(r[6]) if r[6] else None) for r in records]

@app.post("/residents", response_model=Resident)
def create_resident(res: Resident, db = Depends(get_db)):
    cursor = db.cursor()
    try:
        cursor.execute(
            "INSERT INTO residents (resident_id, unit_id, property_id, first_name, last_name, move_in_date, move_out_date) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (
                res.resident_id,
                res.unit_id,
                res.property_id,
                res.first_name,
                res.last_name,
                res.move_in_date,
                res.move_out_date,
            ),
        )
        db.commit()
        return res
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        cursor.close()

@app.get("/rentrole/{property_id}", response_model=List[RentRole])
def list_rentrole(property_id: int, db = Depends(get_db)):
    # return full history ordered by date
    records = run_query(
        db, "SELECT rentroll_id, date, property_id, unit_id, unit_number, resident_id, resident_name, rent_amount, unit_status FROM rentroll WHERE property_id = %s ORDER BY date",
        (property_id,)
    )
    return [RentRole(scd_id=r[0], date=str(r[1]), property_id=r[2], unit_id=r[3], unit_number=r[4], resident_id=r[5], resident_name=r[6], rent=r[7], unit_status=r[8]) for r in records]

@app.get("/rentrole", response_model=List[RentRole])
def list_rentroles(db = Depends(get_db)):
    # return full history ordered by date
    records = run_query(
        db, "SELECT rentroll_id, date, property_id, unit_id, unit_number, resident_id, resident_name, rent_amount, unit_status FROM rentroll ORDER BY date"
    )
    return [RentRole(scd_id=r[0], date=str(r[1]), property_id=r[2], unit_id=r[3], unit_number=r[4], resident_id=r[5], resident_name=r[6], rent=r[7], unit_status=r[8]) for r in records]

@app.get("/scd", response_model=List[RentRole])
def list_scd(db = Depends(get_db)):
    """Get all SCD (Slowly Changing Dimension) records"""
    records = run_query(
        db, "SELECT scd_id, property_id, unit_id, resident_id, property_name, owner, unit_number, unit_status, occupied, rent, first_name, last_name, effective_from, effective_to, current_flag FROM scd ORDER BY effective_from DESC"
    )
    return [RentRole(scd_id=r[0], property_id=r[1], unit_id=r[2], resident_id=r[3], property_name=r[4], owner=r[5], unit_number=r[6], unit_status=r[7], occupied=r[8], rent=r[9], first_name=r[10], last_name=r[11], effective_from=str(r[12]) if r[12] else None, effective_to=str(r[13]) if r[13] else None, current_flag=r[14]) for r in records]

@app.get("/scd/history/{entity_type}/{entity_id}")
def get_scd_history(entity_type: str, entity_id: int, db = Depends(get_db)):
    """Get change history for an entity (property, unit, or resident)"""
    if entity_type == "property":
        records = run_query(
            db, "SELECT scd_id, property_id, unit_id, resident_id, property_name, owner, unit_number, unit_status, occupied, rent, first_name, last_name, effective_from, effective_to, current_flag FROM scd WHERE property_id = %s ORDER BY effective_from DESC",
            (entity_id,)
        )
    elif entity_type == "unit":
        records = run_query(
            db, "SELECT scd_id, property_id, unit_id, resident_id, property_name, owner, unit_number, unit_status, occupied, rent, first_name, last_name, effective_from, effective_to, current_flag FROM scd WHERE unit_id = %s ORDER BY effective_from DESC",
            (entity_id,)
        )
    elif entity_type == "resident":
        records = run_query(
            db, "SELECT scd_id, property_id, unit_id, resident_id, property_name, owner, unit_number, unit_status, occupied, rent, first_name, last_name, effective_from, effective_to, current_flag FROM scd WHERE resident_id = %s ORDER BY effective_from DESC",
            (entity_id,)
        )
    else:
        raise HTTPException(status_code=400, detail="entity_type must be 'property', 'unit', or 'resident'")
    
    if not records:
        raise HTTPException(status_code=404, detail=f"No history found for {entity_type} {entity_id}")
    
    return [RentRole(scd_id=r[0], property_id=r[1], unit_id=r[2], resident_id=r[3], property_name=r[4], owner=r[5], unit_number=r[6], unit_status=r[7], occupied=r[8], rent=r[9], first_name=r[10], last_name=r[11], effective_from=str(r[12]) if r[12] else None, effective_to=str(r[13]) if r[13] else None, current_flag=r[14]) for r in records]

@app.get("/scd/current")
def get_scd_current(db = Depends(get_db)):
    """Get all current (active) SCD records"""
    records = run_query(
        db, "SELECT scd_id, property_id, unit_id, resident_id, property_name, owner, unit_number, unit_status, occupied, rent, first_name, last_name, effective_from, effective_to, current_flag FROM scd WHERE current_flag = true ORDER BY effective_from DESC"
    )
    return [RentRole(scd_id=r[0], property_id=r[1], unit_id=r[2], resident_id=r[3], property_name=r[4], owner=r[5], unit_number=r[6], unit_status=r[7], occupied=r[8], rent=r[9], first_name=r[10], last_name=r[11], effective_from=str(r[12]) if r[12] else None, effective_to=str(r[13]) if r[13] else None, current_flag=r[14]) for r in records]

@app.get("/entities/joined")
def get_joined_entities(db = Depends(get_db)):
    """Get the current joined view of all properties, units, and residents"""
    records = run_query(
        db, "SELECT property_id, property_name, owner, unit_id, unit_number, unit_status, occupied, rent, resident_id, first_name, last_name FROM vw_joined_entities"
    )
    return [RentRole(scd_id=i, property_id=r[0], property_name=r[1], owner=r[2], unit_id=r[3], unit_number=r[4], unit_status=r[5], occupied=r[6], rent=r[7], resident_id=r[8], first_name=r[9], last_name=r[10]) for i, r in enumerate(records, 1)]

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
