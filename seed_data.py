import psycopg2
from faker import Faker
from datetime import datetime, timedelta
import random
import os

fake = Faker()

def get_connection_params(db_name: str) -> dict:
    """Build PostgreSQL connection parameters"""
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432"),
        "database": db_name,
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "postgres"),
    }

def seed_database(db_name: str, num_properties: int = 5, min_units_per_property: int = 5, max_units_per_property: int = 15, num_residents_ratio: float = 0.7):
    """
    Generate and insert realistic fake data into the database.
    
    Args:
        db_name: PostgreSQL database name
        num_properties: Number of properties to generate
        min_units_per_property: Minimum units per property
        max_units_per_property: Maximum units per property
        num_residents_ratio: Fraction of units to have residents (0.0-1.0)
    """
    conn = psycopg2.connect(**get_connection_params(db_name))
    cursor = conn.cursor()
    
    print(f"Seeding {db_name} with fake data...")
    
    try:
        # Disable triggers so seed inserts into base tables don't fire scd2 triggers
        # (seed_data inserts directly into scd2 with historical effective_dates)
        cursor.execute("ALTER TABLE properties DISABLE TRIGGER ALL")
        cursor.execute("ALTER TABLE units      DISABLE TRIGGER ALL")
        cursor.execute("ALTER TABLE residents  DISABLE TRIGGER ALL")

        # Generate properties
        properties = []
        for i in range(1, num_properties + 1):
            property_name = f"{fake.word().title()} {random.choice(['Apartments', 'Towers', 'Estates', 'Complex'])}"
            owner = fake.company()
            properties.append((i, property_name, owner))
        
        cursor.executemany(
            "INSERT INTO properties (property_id, property_name, owner) VALUES (%s, %s, %s) ON CONFLICT (property_id) DO NOTHING",
            properties
        )
        print(f"✓ Inserted {len(properties)} properties")
        
        # Generate units
        units = []
        unit_id = 1
        # track when an inactive unit actually went inactive so we can produce
        # historical residents who moved out on or before that date
        inactive_dates = {}
        for prop_id, _, _ in properties:
            # Randomly select number of units for this property
            num_units = random.randint(min_units_per_property, max_units_per_property)
            units_per_floor = (num_units // 3) + 1  # Distribute across 3 floors
            
            for i in range(num_units):
                floor = (i // units_per_floor) + 1
                unit_pos = (i % units_per_floor) + 1
                unit_number = f"{floor}{unit_pos:02d}"
                unit_status = random.choice(['active', 'active', 'active', 'inactive'])  # 75% active
                if unit_status == 'inactive':
                    # record when the unit became inactive (some past day within last week)
                    inactive_date = fake.date_between(start_date='-7d', end_date='today')
                    inactive_dates[unit_id] = inactive_date
                    occupied = False
                else:
                    occupied = random.random() < num_residents_ratio
                
                units.append((unit_id, unit_number, prop_id, unit_status, occupied))
                unit_id += 1
        
        cursor.executemany(
            "INSERT INTO units (unit_id, unit_number, property_id, unit_status, occupied) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (unit_id) DO NOTHING",
            units
        )
        print(f"✓ Inserted {len(units)} units")
        
        # Seed scd2 tables so the rentroll snapshot can generate one row per
        # unit per day going back 7 days.
        today = datetime.today()
        start_date = (today - timedelta(days=7)).date()

        cursor.executemany(
            "INSERT INTO properties_scd2 (property_id, property_name, effective_date, expiration_date, is_current) VALUES (%s, %s, %s, %s, %s)",
            [(pid, pname, start_date, None, True) for pid, pname, _ in properties]
        )

        cursor.executemany(
            "INSERT INTO units_scd2 (unit_id, property_id, unit_number, unit_status, occupied, effective_date, expiration_date, is_current) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            [(uid, pid, unum, status, occ, start_date, None, True) for uid, unum, pid, status, occ in units]
        )
        conn.commit()
        
        # Generate residents
        residents = []
        resident_id = 1

        for unit_id, unit_num, prop_id, status, occupied in units:
            if status == 'active' and occupied:
                # current resident
                first_name = fake.first_name()
                last_name = fake.last_name()
                rent = round(random.uniform(1000, 5000), 2)

                # Random move-in date within the last 7 days
                move_in_date = fake.date_between(start_date='-7d', end_date='today')
                
                # Some residents have moved out (within last week as well)
                move_out_date = None
                if random.random() < 0.2:  # 20% moved out
                    move_out_date = fake.date_between(start_date=move_in_date, end_date='today')
                
                residents.append((resident_id, unit_id, prop_id, first_name, last_name, rent if move_out_date is None else 0, move_in_date, move_out_date))
                resident_id += 1
            elif status == 'inactive':
                # generate a historical tenant who moved out before or when the
                # unit went inactive; this satisfies the rule that any tenant
                # must depart on/before the inactivation date
                inactive_date = inactive_dates.get(unit_id)
                if inactive_date:
                    first_name = fake.first_name()
                    last_name = fake.last_name()
                    
                    rent = round(random.uniform(1000, 5000), 2)
                    # move in sometime earlier (up to 30 days before inactive)
                    move_out_date = inactive_date
                    move_in_date = fake.date_between(
                        start_date=move_out_date - timedelta(days=30),
                        end_date=move_out_date
                    )
                    residents.append((resident_id, unit_id, prop_id, first_name, last_name, 0, move_in_date, move_out_date))
                    resident_id += 1
        
        cursor.executemany(
            "INSERT INTO residents (resident_id, unit_id, property_id, first_name, last_name, rent, move_in_date, move_out_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (resident_id) DO NOTHING",
            residents
        )
        print(f"✓ Inserted {len(residents)} residents")

        # Correct units whose seeded resident already moved out
        moved_out_unit_ids = [uid for _, uid, _, _, _, _, _, move_out in residents if move_out is not None]
        if moved_out_unit_ids:
            cursor.execute(
                "UPDATE units SET occupied = FALSE WHERE unit_id = ANY(%s)",
                (moved_out_unit_ids,)
            )
            cursor.execute(
                "UPDATE units_scd2 SET occupied = FALSE WHERE unit_id = ANY(%s)",
                (moved_out_unit_ids,)
            )

        # residents_scd2: effective from move_in_date so the snapshot reflects
        # when each resident actually started occupying their unit
        cursor.executemany(
            "INSERT INTO residents_scd2 (resident_id, unit_id, first_name, last_name, move_in_date, move_out_date, rent, effective_date, expiration_date, is_current) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            [(rid, uid, fn, ln, move_in, move_out, rent, move_in, None, True)
             for rid, uid, _, fn, ln, rent, move_in, move_out in residents]
        )

        # Re-enable triggers now that seed data (including scd2 tables) is fully loaded
        cursor.execute("ALTER TABLE properties ENABLE TRIGGER ALL")
        cursor.execute("ALTER TABLE units      ENABLE TRIGGER ALL")
        cursor.execute("ALTER TABLE residents  ENABLE TRIGGER ALL")

        conn.commit()
        print(f"✓ Database seeding complete!")
        return {
            "properties": len(properties),
            "units": len(units),
            "residents": len(residents)
        }
    
    except Exception as e:
        conn.rollback()
        print(f"Error seeding database: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    seed_database("welltower")
