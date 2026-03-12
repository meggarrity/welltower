# Welltower Backend Engineering Assessment

## 🚀 Quick Start (PostgreSQL Edition)

⚡ **Upgrade**: This project has been refactored from SQLite to PostgreSQL for better scalability and concurrent write support.

### Prerequisites
- PostgreSQL 15+ (installed and running)
- Python 3.8+
- pip

### Setup (5 minutes)

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Create the database
createdb welltower

# 3. Set environment variables
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=welltower
export DB_USER=postgres
export DB_PASSWORD=postgres

# 4. Run the API
uvicorn api:app --reload --host 0.0.0.0 --port 8000
```

Visit: http://localhost:8000/docs for interactive API documentation

**For detailed PostgreSQL setup, migration notes, and troubleshooting, see [MIGRATION_GUIDE.md](MIGRATION_GUIDE.md)**

---

## Overview

Welcome to the Welltower technical assessment! This project evaluates your ability to work with real-world senior living data and build a foundational reporting system.

**Time estimate:** 2-3 hours (You can spend as much time as you like; however, we don’t expect it to take more than three hours.)
**Tech stack:** Anything and everything as long as it runs locally.

PLEASE write instructions on how to run your code. Your code should run on multiple platforms, as reviewers may have different environments. There should be a README.md file in the root directory of your project which explains how to do this as well as your assumptions and thought process. We are assessing your understanding of the business context, your approach to structuring code for reviewability, and your testing strategy. We expect you to define reasonable data validations based on what would make sense for real-world properties.

It is fine if you are not able to implement everything discussed. You can address any shortcomings or follow-ons in your README.

---

## Context

Your project relates to multifamily commercial real estate. Multifamily buildings have a single owner that manages the property and many units within the property. A typical Multifamily building can have anywhere from 4 to 1000 units in it. Each unit is rented out individually to residents. Property management software helps track which properties are under management, what units are within each property, and who the current residents are. Your system should allow a user to define your properties and units. These are both slowly changing dimensions but do occasionally change. Units can be taken in and out of service. Other core functionality includes moving residents into the building, changing rents over time (think an annual rent increase), and handling moving residents out.

The following JSON examples give you an idea of what the data model might look like. We do not consider what's defined below as complete or exhaustive so feel free to modify and improve upon it in order to accomplish the requirements.

**Example property definition:**
```
{
    "property_id": 1,
    "property_name": "Sunset Gardens"
}
```

**Example unit definition:**
```
{
    "property_id": 1,
    "unit_id": 2,
    "unit_number": "P1-U01"
}
```

**Example resident definition:**
```
{
    "resident_id": 1,
    "property_id": 1,
    "unit_id": 2,
    "first_name": "John",
    "last_name": "Doe",
    "monthly_rent": 3000,
    "move_in_date": "2023-11-01",
    "move_out_date": null
}
```

Besides keeping track of the current state of the properties, the other core purpose of a property management system is to provide reporting to the business. The primary report users want to look at is a **rent roll**. A rent roll is a daily operational report showing the complete occupancy and revenue status of a property. It's the single source of truth for operations and drives financial reporting and business decisions. Rent rolls are used to generate key metrics that reveal occupancy trends, resident movements, and revenue performance.

**Example rent roll output (3-day sample):**
```
[
    {
        "date": "2024-01-01",
        "property_id": 1,
        "unit_id": 1,
        "unit_number": "P1-U01",
        "resident_id": 5,
        "resident_name": "Resident5 Last5",
        "monthly_rent": 3000
    },
    {
        "date": "2024-01-01",
        "property_id": 1,
        "unit_id": 2,
        "unit_number": "P1-U02",
        "resident_id": null,
        "resident_name": null,
        "monthly_rent": 0
    },
    {
        "date": "2024-01-01",
        "property_id": 1,
        "unit_id": 3,
        "unit_number": "P1-U03",
        "resident_id": 12,
        "resident_name": "Resident12 Last12",
        "monthly_rent": 4500
    },
    {
        "date": "2024-01-02",
        "property_id": 1,
        "unit_id": 1,
        "unit_number": "P1-U01",
        "resident_id": 5,
        "resident_name": "Resident5 Last5",
        "monthly_rent": 3000
    },
    {
        "date": "2024-01-02",
        "property_id": 1,
        "unit_id": 2,
        "unit_number": "P1-U02",
        "resident_id": null,
        "resident_name": null,
        "monthly_rent": 0
    },
    {
        "date": "2024-01-02",
        "property_id": 1,
        "unit_id": 3,
        "unit_number": "P1-U03",
        "resident_id": null,
        "resident_name": null,
        "monthly_rent": 0
    },
    {
        "date": "2024-01-03",
        "property_id": 1,
        "unit_id": 1,
        "unit_number": "P1-U01",
        "resident_id": 5,
        "resident_name": "Resident5 Last5",
        "monthly_rent": 3000
    },
    {
        "date": "2024-01-03",
        "property_id": 1,
        "unit_id": 2,
        "unit_number": "P1-U02",
        "resident_id": 8,
        "resident_name": "Resident8 Last8",
        "monthly_rent": 6200
    },
    {
        "date": "2024-01-03",
        "property_id": 1,
        "unit_id": 3,
        "unit_number": "P1-U03",
        "resident_id": null,
        "resident_name": null,
        "monthly_rent": 0
    }
]
```

**What this shows:**
- **Jan 1**: P1-U01 occupied (Resident 5, $3,000), P1-U02 vacant ($0), P1-U03 occupied (Resident 12, $4,500)
- **Jan 2**: P1-U01 still occupied, P1-U02 still vacant, P1-U03 now vacant (Resident 12 moved out)
- **Jan 3**: P1-U01 still occupied, P1-U02 now occupied (Resident 8, $6,200), P1-U03 still vacant

---

## What You Are Building

Your first task is to build an **API** that implements the core functionality listed above. Do not worry about authentication.

Your second task is building a sample rent roll that models the actual rent rolls Welltower uses every day.

Your code should include tests.

---

## Stretch Goals (unordered)

### KPIs
A KPI API that retrieves move-ins, move-outs, and occupancy rates by month, within a given start and end date.

Occupancy rate = (sum of days occupied per unit) / (total units * days in month). For example, in a 30 day month, if a property has 50 units and 39 are occupied for 30 days and 2 are occupied for the 15 days, we expect an occupancy rate of ((39 * 30) + (2 * 15)) / (50 * 30) = 0.8

### Unit Status
Extend the unit definition to include a status (active | inactive), which can change over time. Residents cannot move into or rent an inactive unit. Include the status in the rent roll output. For example, if a unit is taken out of service from 2025-01-01 to 2025-01-02 for renovations and then put back into service on 2025-01-03 the rent roll should reflect that:
```
[
    {
        "date": "2025-01-01",
        "property_id": 1,
        "unit_id": 1,
        "unit_number": "P1-U01",
        "resident_id": null,
        "resident_name": null,
        "monthly_rent": 0,
        "unit_status": "inactive"
    },
    {
        "date": "2025-01-02",
        "property_id": 1,
        "unit_id": 1,
        "unit_number": "P1-U01",
        "resident_id": null,
        "resident_name": null,
        "monthly_rent": 0,
        "unit_status": "inactive"
    },
    {
        "date": "2025-01-03",
        "property_id": 1,
        "unit_id": 1,
        "unit_number": "P1-U01",
        "resident_id": null,
        "resident_name": null,
        "monthly_rent": 0,
        "unit_status": "active"
    }
]
```
---

## What You Will Do

Your task is to:
1. Build APIs for managing properties, units, and residents.
2. Build an API to generate a rent roll (in CSV, JSON, or another format). This API should take in a start_date and end_date and generate the rent roll for that date range.
3. Implement the stretch goals, if time allows.

---

## Hints

### Understanding the Data and APIS
- Think about what constitutes a logical change to the data and encode that into your APIs. For example, you should not be able to set a move in date before a move out date.
- Rents can change over time. For example, a resident moves in on 06/01/2024 and has a rent of $3,000. On 06/01/2025, the rent increases to $3,100. If I fetch rent rolls from 1/1/2024 to the current date, I should see both those rents.
- Data should be amendable. For example, if the move in date, move out date, or unit is changed on a resident, the rent roll should reflect that change.

You need to determine how to use these dates to generate daily rent roll snapshots.

### Vacant Units
Vacant units must appear on the rent roll for every day, showing null resident fields and $0 rent. A unit can be vacant for the entire period or transition between occupied and vacant states.

---

Good luck! 🚀
