# Data Dictionary

This document describes all output columns, their types, and sources.

## Entity Table (entity_YYYYMMDD_HHMM.csv)

Primary fact table containing all ingested entities.

| Column | Type | Description | Source |
|--------|------|-------------|--------|
| facility_id | VARCHAR | Unique facility identifier (PF_SITE_ID or composite) | PA Tanks: PF_SITE_ID |
| facility_name | VARCHAR | Facility name | PA Tanks: PF_NAME or MAILING_NAME |
| address | VARCHAR | Primary address line | PA Tanks: LOCAD_PF_ADDRESS_1 |
| city | VARCHAR | City name | PA Tanks: LOCAD_LOCAD_PF_CITY |
| state | VARCHAR | State abbreviation | PA Tanks: LOCAD_PF_STATE |
| zip | VARCHAR | ZIP code | PA Tanks: LOCAD_PF_ZIP_CODE |
| county | VARCHAR | County name | PA Tanks: PF_COUNTY_NAME |
| product_code | VARCHAR | Product/substance code | PA Tanks: SUBSTANCE_CODE |
| capacity_gal | DOUBLE | Capacity in gallons (numeric) | PA Tanks: CAPACITY (cleaned) |
| status_code | VARCHAR | Status code (raw) | PA Tanks: STATUS_CODE |
| is_diesel_like | BOOLEAN | True if product code is diesel-like | Derived: SUBSTANCE_CODE in DIESEL_LIKE_CODES |
| is_active_like | BOOLEAN | True if status is active (C) | Derived: STATUS_CODE == "C" |
| capacity_bucket | VARCHAR | Capacity bucket: <1K, 1K-5K, 5K-10K, 10K-20K, 20K+ | Derived: capacity_gal ranges |
| latitude | DOUBLE | Latitude (geocoded) | Google Geocoding API |
| longitude | DOUBLE | Longitude (geocoded) | Google Geocoding API |
| source | VARCHAR | Data source identifier | Always "pa_tanks" for PA ingestion |

## Lead Score Table (lead_score_YYYYMMDD_HHMM.csv)

Scoring results for each entity.

| Column | Type | Description | Source |
|--------|------|-------------|--------|
| entity_id | VARCHAR | Entity identifier (FK to entity) | entity.facility_id |
| score | INTEGER | Total lead score (0-100, capped) | Calculated from scoring rules |
| tier | VARCHAR | Score tier: Tier A, Tier B, Tier C, Park | Derived: score ranges |
| reason_codes | VARCHAR | Comma-separated reason codes | Scoring rules applied |
| reason_text | TEXT | Human-readable reason text | Composed from reason codes |
| updated_at | TIMESTAMP | Last update timestamp | Current timestamp |

## Signals Table (signals_YYYYMMDD_HHMM.csv)

Dimension table for signal types.

| Column | Type | Description | Source |
|--------|------|-------------|--------|
| signal_id | VARCHAR | Signal identifier | Predefined |
| signal_name | VARCHAR | Human-readable signal name | Predefined |
| signal_category | VARCHAR | Signal category (tank, status, fleet, infrastructure) | Predefined |

## CRM Sync Table (crm_sync_YYYYMMDD_HHMM.csv)

Bridge table tracking CRM synchronization status.

| Column | Type | Description | Source |
|--------|------|-------------|--------|
| entity_id | VARCHAR | Entity identifier (FK to entity) | entity.facility_id |
| crm_id | VARCHAR | CRM record ID (Bigin) | Bigin API response |
| crm_type | VARCHAR | CRM record type (Account, Contact, Deal) | Always "Account" for accounts |
| synced_at | TIMESTAMP | Sync timestamp | Current timestamp on sync |
| sync_status | VARCHAR | Sync status: success, error, failed | Sync operation result |

## Tier A GeoJSON (tierA_YYYYMMDD_HHMM.geojson)

GeoJSON FeatureCollection of Tier A sites for mapping.

| Property | Type | Description | Source |
|----------|------|-------------|--------|
| facility_id | VARCHAR | Entity identifier | entity.facility_id |
| facility_name | VARCHAR | Facility name | entity.facility_name |
| address | VARCHAR | Address | entity.address |
| city | VARCHAR | City | entity.city |
| state | VARCHAR | State | entity.state |
| score | INTEGER | Lead score | lead_score.score |
| tier | VARCHAR | Score tier | lead_score.tier |
| reason_text | TEXT | Human-readable reasons | lead_score.reason_text |

**Geometry:**
- Type: Point
- Coordinates: [longitude, latitude] (GeoJSON format)

## Scoring Rules Reference

### Positive Signals

| Signal | Points | Code | Description |
|--------|--------|------|-------------|
| D_TANK | +40 | D_TANK | Diesel/fuel oil present |
| CAP_20K | +25 | CAP_20K | Capacity >= 20,000 gallons |
| CAP_10K | +20 | CAP_10K | Capacity 10,000-19,999 gallons |
| CAP_5K | +15 | CAP_5K | Capacity 5,000-9,999 gallons |
| CAP_1K | +8 | CAP_1K | Capacity 1,000-4,999 gallons |
| ACTIVE | +15 | ACTIVE | Active facility status |
| FMCSA_50 | +20 | FMCSA_50 | FMCSA fleet size >= 50 power units |
| FMCSA_10 | +10 | FMCSA_10 | FMCSA fleet size 10-49 power units |
| HOSP | +15 | HOSP | Hospital/healthcare facility |
| SCHOOL | +15 | SCHOOL | School/university/bus depot |
| DCENTER | +15 | DCENTER | Data center |
| ECHO | +10 | ECHO | ECHO facility registry presence |
| NEAR | +10 | NEAR | Distance <= 25 miles from base |
| NEAR40 | +5 | NEAR40 | Distance 25-40 miles from base |
| WEB_INTENT | +10 | WEB_INTENT | Website language indicates intent |

### Negative Signals

| Signal | Points | Code | Description |
|--------|--------|------|-------------|
| INCUMBENT | -10 | INCUMBENT | Incumbent named on site page |
| DNC | -15 | DNC | CRM do not contact flag |

### Score Bands

| Tier | Score Range | Description |
|------|-------------|-------------|
| Tier A | 80-100 | Highest priority leads |
| Tier B | 60-79 | Medium priority leads |
| Tier C | 40-59 | Lower priority leads |
| Park | < 40 | Not actively pursued |

### Score Cap

Maximum total score is capped at 100 points.

## Product Code Classifications

### Diesel-Like Codes
- DIESL
- BIDSL
- HO
- KERO

### Non-Diesel Codes
- GAS
- AVGAS
- JET
- ETHNL
- HZSUB
- OTHER
- USDOL
- NMO
- UNREG
- GSHOL
- NPOIL
- HZPRL

## Status Code Classifications

### Active Status
- C (Active)

### Inactive Status
- T (Terminated)
- All other codes

## Capacity Buckets

| Bucket | Range | Description |
|--------|-------|-------------|
| 20K+ | >= 20,000 | Very large capacity |
| 10K-20K | 10,000 - 19,999 | Large capacity |
| 5K-10K | 5,000 - 9,999 | Medium capacity |
| 1K-5K | 1,000 - 4,999 | Small capacity |
| <1K | < 1,000 or null | Very small or unknown capacity |

