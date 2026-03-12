# Slowly Changing Dimension (SCD) Implementation

## Overview

The Welltower API implements automatic SCD Type 2 tracking using SQLite views and triggers. Every change to `properties`, `units`, or `residents` automatically generates a historical record in the `scd` table.

## Architecture

### 1. Dynamic Join View: `vw_joined_entities`

A database view that joins all attributes from the three core tables:

```sql
SELECT
  p.property_id, p.property_name, p.owner,
  u.unit_id, u.unit_number, u.unit_status, u.occupied, u.rent,
  r.resident_id, r.first_name, r.last_name
FROM properties p
LEFT JOIN units u ON p.property_id = u.property_id
LEFT JOIN residents r ON u.unit_id = r.unit_id
```

This view provides a **denormalized, always-current snapshot** of all related data.

### 2. Automatic Triggers

Six database triggers automatically capture changes:

| Trigger | Event | Action |
|---------|-------|--------|
| `properties_insert` | New property | Insert full joined snapshot into SCD |
| `properties_update` | Property changed | Close old SCD record, insert new one |
| `units_insert` | New unit | Insert full joined snapshot into SCD |
| `units_update` | Unit changed | Close old SCD record, insert new one |
| `residents_insert` | New resident | Insert full joined snapshot into SCD |
| `residents_update` | Resident changed | Close old SCD record, insert new one |

### 3. SCD Table Structure

```
scd_id (PK)           - Unique record ID
property_id           - Linked to properties
unit_id               - Linked to units
resident_id           - Linked to residents
property_name         - Denormalized property name
owner                 - Denormalized owner
unit_number           - Denormalized unit number
unit_status           - Denormalized unit status
occupied              - Denormalized occupancy flag
rent                  - Denormalized rent amount
first_name            - Denormalized resident first name
last_name             - Denormalized resident last name
effective_from        - Start date of this record (when change occurred)
effective_to          - End date of this record (NULL = current)
current_flag          - 1 = current version, 0 = historical
```

## How It Works

### Example: Updating a Unit

1. **Initial State**: Unit 5 is active, rent $1500
   ```sql
   SELECT * FROM scd WHERE unit_id = 5 AND current_flag = 1;
   -- Returns: scd_id=42, unit_number='101', unit_status='active', rent=1500, effective_from='2024-01-15', current_flag=1
   ```

2. **Update Command**: Change rent to $1600
   ```python
   curl -X PUT /units/5 -d {"rent": 1600}
   ```

3. **Trigger Actions**:
   - Close current record: `UPDATE scd SET effective_to='2026-03-09', current_flag=0 WHERE unit_id=5 AND current_flag=1`
   - Insert new snapshot: `INSERT INTO scd (...) SELECT ... FROM vw_joined_entities WHERE unit_id=5`

4. **Result**: Two SCD records now exist
   ```sql
   SELECT * FROM scd WHERE unit_id = 5 ORDER BY effective_from DESC;
   -- Record 1: rent=1600, effective_from='2026-03-09', effective_to=NULL, current_flag=1
   -- Record 2: rent=1500, effective_from='2024-01-15', effective_to='2026-03-09', current_flag=0
   ```

## API Endpoints

### View All SCD Records
```bash
GET /scd
# Returns full history with all changes
```

### View Current State Only
```bash
GET /scd/current
# Returns only current_flag=1 records (active versions)
```

### View Change History for Entity
```bash
GET /scd/history/{entity_type}/{entity_id}
# Examples:
GET /scd/history/property/1      # All changes to property 1
GET /scd/history/unit/5          # All changes to unit 5
GET /scd/history/resident/12     # All changes to resident 12
```

### View Denormalized Current State
```bash
GET /entities/joined
# Shows the current state of the vw_joined_entities view
# Useful for seeing how properties, units, and residents are connected
```

## Benefits

✅ **Automatic** - No manual SCD updates needed  
✅ **Complete** - Captures all attribute changes across all tables  
✅ **Denormalized** - SCD contains all related attributes in one row  
✅ **Queryable** - Full audit trail with effective dates  
✅ **Non-intrusive** - Triggers handle it automatically  

## Usage Examples

### Find when a unit's rent changed
```sql
SELECT effective_from, effective_to, rent 
FROM scd 
WHERE unit_id = 5 
ORDER BY effective_from;
```

### Find the state of a unit on a specific date
```sql
SELECT * FROM scd 
WHERE unit_id = 5 
AND effective_from <= '2025-06-15' 
AND (effective_to IS NULL OR effective_to > '2025-06-15')
ORDER BY effective_from DESC 
LIMIT 1;
```

### Find all residents who have lived in property 1
```sql
SELECT DISTINCT first_name, last_name, move_in_date, effective_to
FROM scd 
WHERE property_id = 1 AND resident_id IS NOT NULL 
ORDER BY effective_from;
```

### Track occupancy changes
```sql
SELECT unit_id, unit_number, occupied, effective_from, effective_to
FROM scd 
WHERE unit_id = 3
ORDER BY effective_from;
```

## Important Notes

- **Triggers fire AFTER changes** - They see the new values
- **effective_from is set to TODAY** - Use DATE('now') in SQLite
- **effective_to is NULL for current records** - Makes it easy to find "what's current"
- **All source attributes are captured** - The view join ensures completeness
- **No DELETE tracking** - To log deletes, use UPDATE to mark as inactive instead

## Maintenance

To view the join view definition:
```sql
SELECT sql FROM sqlite_master WHERE type='view' AND name='vw_joined_entities';
```

To see all SCD triggers:
```sql
SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE '%scd%' OR name IN ('properties_insert', 'properties_update', 'units_insert', 'units_update', 'residents_insert', 'residents_update');
```
