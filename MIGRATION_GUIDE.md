# SQLite to PostgreSQL Migration Guide

This project has been successfully migrated from SQLite to PostgreSQL. This document outlines all the changes made and how to set up your environment.

## Summary of Changes

### Files Modified

| File | Changes |
|------|---------|
| `api.py` | Complete rewrite for PostgreSQL with psycopg2 |
| `seed_data.py` | Updated to use PostgreSQL connections |
| `test_api.py` | Test database creation/cleanup via PostgreSQL |
| `requirements.txt` | Added psycopg2-binary and python-dotenv |

### Files Preserved (SQLite versions backed up)
- `api_sqlite.py.bak` - Original SQLite API
- `seed_data_sqlite.py.bak` - Original SQLite seed data
- `test_api_sqlite.py.bak` - Original SQLite tests
- `clear_cache.py` - Still valid, though designed for SQLite cleanup

## Key Technical Differences

### 1. Database Connection

**SQLite (Old)**:
```python
import sqlite3
conn = sqlite3.connect("welltower.db")
```

**PostgreSQL (New)**:
```python
import psycopg2
conn = psycopg2.connect(
    host="localhost",
    port="5432",
    database="welltower",
    user="postgres",
    password="postgres"
)
```

### 2. Parameter Placeholders

**SQLite**: Uses `?` for parameter substitution
```python
cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,))
```

**PostgreSQL**: Uses `%s` for parameter substitution (regardless of data type)
```python
cursor.execute("SELECT * FROM users WHERE id = %s", (user_id,))
```

### 3. Date Functions

**SQLite**:
```sql
DATE('now')           -- Current date
DATE('2023-01-01')    -- Specific date
```

**PostgreSQL**:
```sql
CURRENT_DATE          -- Current date
DATE '2023-01-01'     -- Specific date
```

### 4. Triggers and PL/pgSQL

**SQLite**:
```sql
CREATE TRIGGER check_occupancy
BEFORE INSERT ON units
FOR EACH ROW
WHEN NEW.unit_status = 'inactive' AND NEW.occupied = 1
BEGIN
  SELECT RAISE(ABORT, 'Inactive units cannot be occupied');
END;
```

**PostgreSQL**:
```sql
CREATE FUNCTION check_unit_occupancy()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW.unit_status = 'inactive' AND NEW.occupied = true THEN
    RAISE EXCEPTION 'Inactive units cannot be occupied';
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER units_occupancy_check
BEFORE INSERT ON units
FOR EACH ROW
EXECUTE FUNCTION check_unit_occupancy();
```

### 5. Boolean Values

**SQLite**: Uses `0` and `1`
```python
occupied = 1  -- true
occupied = 0  -- false
```

**PostgreSQL**: Uses `true` and `false` (or `TRUE`/`FALSE`)
```python
occupied = True   # in Python/psycopg2
occupied = true   # in SQL
```

### 6. INSERT Conflict Handling

**SQLite**:
```sql
INSERT OR IGNORE INTO users (id, name) VALUES (?, ?)
```

**PostgreSQL**:
```sql
INSERT INTO users (id, name) VALUES (%s, %s) 
ON CONFLICT (id) DO NOTHING
```

## Environment Setup

### Prerequisites

1. **PostgreSQL Installation**
   - macOS: `brew install postgresql && brew services start postgresql`
   - Linux: `sudo apt-get install postgresql postgresql-contrib`
   - Windows: Download from https://www.postgresql.org/download/windows/
   - Docker: See `.env.example` for Docker Compose example

2. **Python Environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

### Installation Steps

1. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Create PostgreSQL Database**
   ```bash
   createdb welltower
   # Or via psql:
   psql -U postgres -c "CREATE DATABASE welltower;"
   ```

3. **Set Environment Variables**
   ```bash
   export DB_HOST=localhost
   export DB_PORT=5432
   export DB_NAME=welltower
   export DB_USER=postgres
   export DB_PASSWORD=postgres
   export LOAD_FAKE_DATA=true
   ```
   
   Or create a `.env` file (see `.env.example`)

4. **Run the API**
   ```bash
   uvicorn api:app --reload --host 0.0.0.0 --port 8000
   ```

5. **Run Tests**
   ```bash
   pytest test_api.py -v
   ```

## API Endpoints (Unchanged)

All API endpoints remain the same:

- `GET /` — Health check
- `GET /properties` — List all properties
- `GET /properties/{property_id}` — Get a specific property
- `POST /properties` — Create a property
- `DELETE /properties/{property_id}` — Delete a property
- `GET /units` — List all units
- `POST /units` — Create a unit
- `GET /residents` — List all residents
- `POST /residents` — Create a resident
- `GET /scd` — Get SCD (Slowly Changing Dimension) records
- `GET /scd/current` — Get current SCD records
- `GET /scd/history/{entity_type}/{entity_id}` — Get entity change history
- `GET /entities/joined` — Get joined view of all entities
- `GET /rentrole` — Get rent roll history
- `GET /rentrole/{property_id}` — Get rent roll for a property

## Business Rules (Preserved)

1. **Inactive units cannot be occupied** — Enforced via CHECK constraint and trigger
2. **Resident move-out dates** — Tracked in the residents table
3. **SCD tracking** — Automatic tracking of dimension changes in the `scd` table
4. **Historical data** — Rentroll and SCD tables maintain historical snapshots

## Troubleshooting

### Connection Refused
```
Error: could not connect to server: Connection refused
```
Ensure PostgreSQL is running:
```bash
# macOS
brew services start postgresql

# Linux
sudo systemctl start postgresql
```

### Authentication Failed
```
Error: FATAL: password authentication failed for user "postgres"
```
Check your environment variables match your PostgreSQL credentials.

### Test Database Issues
Tests automatically create/drop a `test_welltower` database. Ensure your PostgreSQL user has sufficient privileges:
```sql
ALTER USER postgres CREATEDB;
```

### Natural Language Queries (Database Operations)

The API now uses PostgreSQL's native features:
- **Views**: `vw_joined_entities` for denormalized queries
- **Functions**: `check_unit_occupancy()` and `track_scd_change()` for business logic
- **Triggers**: Automatic enforcement and tracking
- **Constraints**: CHECK constraints for data validation

## Performance Considerations

PostgreSQL advantages over SQLite:

1. **Concurrency**: Multiple writers without locking issues (SQLite allows only one writer)
2. **Scalability**: Better handling of large datasets
3. **Advanced Features**: Full-text search, JSON operations, array types
4. **Replication**: Built-in support for read replicas
5. **Monitoring**: Better logging and query analysis tools

## Rollback to SQLite

If needed, the original SQLite versions are backed up:
- Restore: `mv api_sqlite.py.bak api.py` (similar for other files)
- Revert: `git checkout api.py seed_data.py test_api.py` (if using version control)

## Next Steps

1. Update database schema as needed using PostgreSQL-specific features
2. Add connection pooling with `pgbouncer` for production
3. Implement database backups using `pg_dump`
4. Set up monitoring with tools like `pg_stat_statements`
5. Consider using an ORM like SQLAlchemy for more complex queries

---

**Last Updated**: March 11, 2026
**Migration Version**: PostgreSQL 15+
