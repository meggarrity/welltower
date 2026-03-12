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
                rent = round(random.uniform(1000, 5000), 2)
                
                units.append((unit_id, unit_number, prop_id, unit_status, occupied, rent))
                unit_id += 1
        
        cursor.executemany(
            "INSERT INTO units (unit_id, unit_number, property_id, unit_status, occupied, rent) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (unit_id) DO NOTHING",
            units
        )
        print(f"✓ Inserted {len(units)} units")
        
        # Optionally create historical SCD/rentroll entries for variety
        # Simulate a few past snapshots within the last week
        today = datetime.today()
        for past_days in [1, 2, 3, 5, 7]:
            hist_date = (today - timedelta(days=past_days)).date()
            # simple example: insert a rentroll record per property for each date
            for prop_id, _, _ in properties:
                # choose a unit for this property if available
                matching_units = [u for u in units if u[2] == prop_id]
                if matching_units:
                    unit = matching_units[0]
                    uid, unum, _, _, _, urent = unit
                else:
                    uid = 0
                    unum = ''
                    urent = 0
                cursor.execute(
                    "INSERT INTO rentroll (rentroll_id, date, property_id, unit_id, unit_number, resident_id, resident_name, rent_amount, unit_status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (rentroll_id) DO NOTHING",
                    (
                        int(f"{past_days}{prop_id}"),
                        hist_date,
                        prop_id,
                        uid,
                        unum,
                        None,
                        None,
                        urent,
                        'active'
                    )
                )
            # for SCD we add a basic entry capturing property snapshot
            for prop_id, prop_name, owner in properties:
                cursor.execute(
                    "INSERT INTO scd (property_id, property_name, owner, effective_from, effective_to, current_flag) VALUES (%s, %s, %s, %s, %s, %s)",
                    (
                        prop_id,
                        prop_name,
                        owner,
                        hist_date,
                        None,
                        False
                    )
                )
        conn.commit()
        
        # Generate residents
        residents = []
        resident_id = 1
        for unit_id, unit_num, prop_id, status, occupied, rent in units:
            if status == 'active' and occupied:
                # current resident
                first_name = fake.first_name()
                last_name = fake.last_name()
                
                # Random move-in date within the last 7 days
                move_in_date = fake.date_between(start_date='-7d', end_date='today')
                
                # Some residents have moved out (within last week as well)
                move_out_date = None
                if random.random() < 0.2:  # 20% moved out
                    move_out_date = fake.date_between(start_date=move_in_date, end_date='today')
                
                residents.append((resident_id, unit_id, prop_id, first_name, last_name, move_in_date, move_out_date))
                resident_id += 1
            elif status == 'inactive':
                # generate a historical tenant who moved out before or when the
                # unit went inactive; this satisfies the rule that any tenant
                # must depart on/before the inactivation date
                inactive_date = inactive_dates.get(unit_id)
                if inactive_date:
                    first_name = fake.first_name()
                    last_name = fake.last_name()
                    # move in sometime earlier (up to 30 days before inactive)
                    move_out_date = inactive_date
                    move_in_date = fake.date_between(
                        start_date=move_out_date - timedelta(days=30),
                        end_date=move_out_date
                    )
                    residents.append((resident_id, unit_id, prop_id, first_name, last_name, move_in_date, move_out_date))
                    resident_id += 1
        
        cursor.executemany(
            "INSERT INTO residents (resident_id, unit_id, property_id, first_name, last_name, move_in_date, move_out_date) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (resident_id) DO NOTHING",
            residents
        )
        print(f"✓ Inserted {len(residents)} residents")
        
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
