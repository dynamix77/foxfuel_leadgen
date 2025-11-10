# Foxfuel Lead Generation System - User Manual

**Version 1.0**  
**Last Updated:** November 2025

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Getting Started](#2-getting-started)
3. [Configuration](#3-configuration)
4. [Data Sources](#4-data-sources)
5. [Running the System](#5-running-the-system)
6. [Understanding Outputs](#6-understanding-outputs)
7. [Troubleshooting](#7-troubleshooting)
8. [Best Practices](#8-best-practices)
9. [Advanced Usage](#9-advanced-usage)
10. [Appendices](#10-appendices)

---

## 1. Introduction

### 1.1 System Overview

The Foxfuel Lead Generation System is a Python-based data pipeline designed for fuel distributors in Southeast Pennsylvania. The system automates the process of discovering, scoring, and managing potential customers by:

- **Ingesting** data from multiple sources (PA DEP storage tanks, NAICS business data, Google Maps scrapes)
- **Normalizing** and **deduplicating** entities across sources
- **Scoring** leads based on configurable business rules
- **Syncing** high-value leads to Bigin CRM
- **Exporting** data for analysis in Power BI

### 1.2 Purpose and Use Cases

**Primary Use Cases:**
- Identify potential fuel customers from regulatory databases
- Score leads based on capacity, location, and business type
- Prioritize sales efforts on Tier A (highest value) prospects
- Automate CRM data synchronization
- Generate mapping visualizations for territory planning

**Target Users:**
- Sales teams needing prioritized lead lists
- Operations teams managing customer data
- Management requiring territory analytics

### 1.3 Key Features

- **Multi-Source Ingestion**: PA DEP tanks, NAICS business data, Google Maps scrapes, FMCSA fleet data
- **Intelligent Matching**: Fuzzy name matching and geospatial proximity matching
- **Sector Classification**: Automatic business sector identification (Fleet, Healthcare, Construction, etc.)
- **Rule-Based Scoring**: Configurable scoring system with tier assignment (A/B/C/Park)
- **CRM Integration**: Automated sync to Bigin with idempotent updates
- **Geospatial Analysis**: Distance calculations, GeoJSON exports for mapping
- **QA Reporting**: Automated data quality reports
- **Structured Logging**: JSON-formatted logs for monitoring and debugging

---

## 2. Getting Started

### 2.1 System Requirements

**Hardware:**
- CPU: 2+ cores recommended
- RAM: 4GB minimum, 8GB+ recommended
- Storage: 500MB+ free space for database and cache
- Network: Stable internet connection for API calls

**Software:**
- **Operating System**: Windows 10+, macOS 10.15+, or Linux
- **Python**: 3.11 or higher
- **Git**: For cloning the repository (optional)

**Dependencies:**
All Python dependencies are listed in `requirements.txt` and will be installed automatically.

### 2.2 Installation Steps

#### Step 1: Clone or Download the Repository

If you have Git installed:
```bash
git clone https://github.com/dynamix77/foxfuel_leadgen.git
cd foxfuel_leadgen
```

Or download the ZIP file from GitHub and extract it.

#### Step 2: Create a Virtual Environment (Recommended)

**Windows:**
```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

**macOS/Linux:**
```bash
python3 -m venv venv
source venv/bin/activate
```

#### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

This will install all required packages including:
- `pandas` - Data manipulation
- `duckdb` - Embedded database
- `pydantic` - Configuration management
- `rapidfuzz` - Fuzzy string matching
- `googlemaps` - Geocoding API client
- `pytest` - Testing framework
- And others (see `requirements.txt` for complete list)

#### Step 4: Create Required Directories

The system will create these automatically, but you can create them manually:

```bash
mkdir -p data cache out logs
```

**Windows PowerShell:**
```powershell
New-Item -ItemType Directory -Force -Path data, cache, out, logs
```

### 2.3 Initial Setup

#### Create Environment File

Copy the example environment file:

```bash
cp .env.example .env
```

**Windows:**
```powershell
Copy-Item .env.example .env
```

#### Configure API Keys

Edit `.env` and add your API keys:

```env
# Google Maps API Key (optional - only needed for geocoding)
GOOGLE_MAPS_API_KEY=your_google_maps_api_key_here

# Bigin CRM - OAuth (recommended) or Access Token
# Option 1: OAuth (recommended - automatically refreshes tokens)
BIGIN_CLIENT_ID=1000.XXXXXXXXXXXXX
BIGIN_CLIENT_SECRET=your_client_secret_here
BIGIN_REFRESH_TOKEN=1000.XXXXXXXXXXXXXXXXXXXXXXXX
BIGIN_BASE_URL=https://www.zohoapis.com/bigin/v2

# Option 2: Direct Access Token (legacy - expires after ~1 hour)
# BIGIN_ACCESS_TOKEN=your_access_token_here
# BIGIN_BASE_URL=https://www.zohoapis.com/bigin/v2

# Base address for distance calculations
BASE_ADDRESS=2450 Old Welsh Road, Willow Grove, PA 19090

# Target counties (comma-separated)
COUNTIES=Bucks,Montgomery,Philadelphia,Chester,Delaware
```

**Note:** The Google Maps API key is optional if you're using the CodeCanyon Google Maps scraper, which provides coordinates directly.

**See [API Key Registration Guide](#api-key-registration) below for detailed instructions on obtaining these keys.**

#### Verify Installation

Run the test suite to verify everything is working:

```bash
pytest tests/ -v
```

You should see all tests passing. If any fail, check that all dependencies are installed correctly.

---

## 3. Configuration

### 3.1 Environment Variables (.env)

The system uses a `.env` file for sensitive configuration. All values can be overridden via environment variables.

**Required Settings:**
- `BIGIN_ACCESS_TOKEN` - Your Bigin CRM API token

**Optional Settings:**
- `GOOGLE_MAPS_API_KEY` - For geocoding (not needed if using CodeCanyon scraper)
- `BASE_ADDRESS` - Base location for distance calculations (default: "2450 Old Welsh Road, Willow Grove, PA 19090")
- `COUNTIES` - Comma-separated list of target counties
- `NAICS_LOCAL_PATH` - Path to NAICS CSV file (default: `./data/NAICS_PhilaRegion_clean_snapshot.csv`)
- `NAICS_MATCH_RADIUS_METERS` - Matching radius for NAICS data (default: 150)
- `NAICS_NAME_SIMILARITY_MIN` - Minimum name similarity percentage (default: 88)

**Example .env file:**
```env
GOOGLE_MAPS_API_KEY=AIzaSyExample123456789
BIGIN_ACCESS_TOKEN=your_token_here
BASE_ADDRESS=2450 Old Welsh Road, Willow Grove, PA 19090
COUNTIES=Bucks,Montgomery,Philadelphia,Chester,Delaware
NAICS_LOCAL_PATH=./data/NAICS_PhilaRegion_clean_snapshot.csv
NAICS_MATCH_RADIUS_METERS=150
NAICS_NAME_SIMILARITY_MIN=88
```

### 3.2 Config File Settings

The main configuration is managed in `src/config.py` using Pydantic BaseSettings. Default values are provided, and all can be overridden via `.env` or environment variables.

**Key Configuration Classes:**
- `Settings` - Main configuration class with all system settings

**Important Paths:**
- `data_dir` - Data files directory (default: `./data`)
- `cache_dir` - Cache directory (default: `./cache`)
- `out_dir` - Output directory (default: `./out`)
- `db_path` - DuckDB database path (default: `./data/leadgen.duckdb`)

### 3.3 CLI Arguments

Most configuration can be overridden via command-line arguments. See section 5 for detailed usage examples.

**Common CLI Arguments:**
- `--pa-tanks-path` - Path to PA DEP storage tank file
- `--naics-local-path` - Path to NAICS CSV file
- `--counties` - Comma-separated county list
- `--skip-geocode` - Skip geocoding (faster for testing)
- `--geocode-limit` - Maximum records to geocode
- `--geocode-qps` - Geocoding rate limit (queries per second)
- `--maps-extractor-glob` - Glob pattern for Maps Extractor CSV files
- `--qa` - Generate QA report after build

### 3.4 Data Source Paths

**Default Data Paths:**
- PA Tanks: Provided via `--pa-tanks-path` argument
- NAICS: `./data/NAICS_PhilaRegion_clean_snapshot.csv`
- Maps Extractor: `./data/maps_extractor/*.csv`
- FMCSA: `./data/fmcsa_snapshot.csv` (if using)
- ECHO: API-based (no file path)
- EIA: `./data/eia_form860_generators.csv` (if using)

All paths can be customized via environment variables or CLI arguments.

---

## 4. Data Sources

### 4.1 PA DEP Storage Tanks

**Source:** Pennsylvania Department of Environmental Protection Storage Tank Listing

**File Format:** CSV or XLSX

**Required Columns:**
The system uses fuzzy header matching, but expects columns similar to:
- `PF_NAME` or `MAILING_NAME` - Facility name
- `PF_SITE_ID` - Site identifier
- `LOCAD_PF_ADDRESS_1` - Street address
- `LOCAD_LOCAD_PF_CITY` - City
- `LOCAD_PF_STATE` - State
- `LOCAD_PF_ZIP_CODE` - ZIP code
- `PF_COUNTY_NAME` - County name
- `SUBSTANCE_CODE` - Product code (DIESL, BIDSL, HO, KERO, etc.)
- `CAPACITY` - Tank capacity (gallons)
- `STATUS_CODE` - Status (C = Active, T = Terminated)

**Data Processing:**
1. Header mapping (exact match, then fuzzy matching)
2. Product code classification (diesel-like vs. non-diesel)
3. Status classification (active vs. inactive)
4. Capacity cleaning and bucketing
5. Address normalization
6. County filtering
7. Geocoding (if enabled)

**Example Usage:**
```bash
python -m src.jobs.build_universe --pa-tanks-path "data/PAStorage_Tank_Listing.xlsx"
```

### 4.2 NAICS Local Data

**Source:** Local NAICS business data snapshot (Philadelphia region)

**File Format:** CSV

**Required Columns:**
- `COMPANY NAME` or `BUSINESS_NAME` - Business name
- `STREET ADDRESS` or `ADDRESS` - Street address
- `CITY` - City name
- `STATE` - State abbreviation
- `ZIP CODE` or `ZIP` - ZIP code
- `COUNTY` - County name
- `NAICS` or `NAICS_CODE` - NAICS code (6 digits)
- `NAICS DESCRIPTION` or `NAICS_TITLE` - Business description

**Sector Classification:**
The system automatically classifies businesses into sectors:
- **Fleet and Transportation** (NAICS 484, 485, 488) - +20 points
- **Construction** (NAICS 23) - +15 points
- **Healthcare** (NAICS 621, 622, 623) - +15 points
- **Education** (NAICS 611) - +10 points
- **Utilities and Data Centers** (NAICS 22, 518210) - +15 points
- **Industrial and Manufacturing** (NAICS 31, 32, 33) - +10 points
- **Public and Government** (NAICS 92) - +5 points
- **Retail and Commercial Fueling** (NAICS 447110, 447190) - +5 points

**Matching Logic:**
- Matches NAICS businesses to tank facilities within 150 meters
- Requires name similarity ≥88% (RapidFuzz ratio)
- Applies highest confidence sector match
- Stores sector information on entity and in signals table

**Example Usage:**
```bash
python -m src.jobs.build_universe \
  --pa-tanks-path "data/PAStorage_Tank_Listing.xlsx" \
  --naics-local-path "./data/NAICS_PhilaRegion_clean_snapshot.csv"
```

### 4.3 Google Maps Extractor (CodeCanyon)

**Source:** CodeCanyon Google Maps Scraper PRO Plus desktop application

**File Format:** CSV export from the scraper

**Required Columns:**
The system automatically detects CodeCanyon format:
- `OrganizationName` - Business name
- `OrganizationAddress` - Full address (format: "Address: Street, City, ST ZIP")
- `OrganizationLatitude` - Latitude coordinate
- `OrganizationLongitude` - Longitude coordinate
- `OrganizationCategory` - Business category

**Address Parsing:**
The system automatically parses the `OrganizationAddress` field:
- Removes "Address: " prefix if present
- Extracts street, city, state, and ZIP code
- Falls back gracefully if parsing fails

**File Placement:**
Place exported CSV files in `./data/maps_extractor/` directory.

**Automatic File Renaming:**
The system automatically renames files named `organizations.csv` with timestamps (e.g., `organizations_20251110_143022.csv`) to prevent overwrite conflicts. This happens automatically when you run `build_universe`, or you can manually rename files using:

```bash
python -m src.jobs.rename_maps_files
```

This allows you to save multiple exports from the CodeCanyon tool without worrying about filename conflicts.

**Example Usage:**
```bash
# Export from CodeCanyon tool to ./data/maps_extractor/organizations.csv
python -m src.jobs.build_universe \
  --pa-tanks-path "data/PAStorage_Tank_Listing.xlsx" \
  --maps-extractor-glob "./data/maps_extractor/*.csv"
```

**Workflow:**
1. Use CodeCanyon Google Maps Scraper to search for businesses
2. Export results as CSV
3. Save CSV file(s) to `./data/maps_extractor/`
4. Run build_universe with `--maps-extractor-glob` flag
5. System will match Maps data to entities and fill missing coordinates

### 4.4 Other Data Sources (Optional)

**FMCSA (Federal Motor Carrier Safety Administration):**
- Fleet size data for transportation companies
- Provides power unit counts for scoring
- File: `./data/fmcsa_snapshot.csv` (if using)

**ECHO (Enforcement and Compliance History Online):**
- EPA facility registry
- Identifies hospitals, schools, data centers
- API-based (no file path needed)

**EIA Form 860 (Energy Information Administration):**
- Generator data
- File: `./data/eia_form860_generators.csv` (if using)

**OSM (OpenStreetMap):**
- Infrastructure data
- API-based queries

**Procurement and Permits:**
- Watch jobs for new opportunities
- Configurable sources

---

## 5. Running the System

### 5.1 Build Universe Workflow

The `build_universe` job is the main orchestration workflow that:
1. Ingests data from all sources
2. Normalizes and deduplicates entities
3. Merges signals from multiple sources
4. Scores leads
5. Exports results

#### Basic Build (Sample Data)

For testing with sample data:

```bash
python -m src.jobs.build_universe \
  --pa-tanks-path samples/pa_tanks_sample.csv \
  --skip-geocode \
  --counties "Bucks,Montgomery,Philadelphia,Chester,Delaware"
```

#### Full Build (Production)

With all data sources and geocoding:

```bash
python -m src.jobs.build_universe \
  --pa-tanks-path "data/PAStorage_Tank_Listing.xlsx" \
  --naics-local-path "./data/NAICS_PhilaRegion_clean_snapshot.csv" \
  --maps-extractor-glob "./data/maps_extractor/*.csv" \
  --counties "Bucks,Montgomery,Philadelphia,Chester,Delaware" \
  --geocode-limit 250 \
  --geocode-qps 4.0 \
  --geocode-batch-size 100 \
  --qa
```

#### Build with Limited Geocoding

To control API costs, limit geocoding to high-priority records:

```bash
python -m src.jobs.build_universe \
  --pa-tanks-path "data/PAStorage_Tank_Listing.xlsx" \
  --geocode-limit 250 \
  --geocode-qps 4.0 \
  --counties "Bucks,Montgomery,Philadelphia,Chester,Delaware"
```

This will:
- Sort records by `is_diesel_like DESC, capacity_gal DESC`
- Geocode only the first 250 records
- Use remaining coordinates from cache or other sources

#### Build Without Geocoding

If using CodeCanyon Maps Extractor (which provides coordinates):

```bash
python -m src.jobs.build_universe \
  --pa-tanks-path "data/PAStorage_Tank_Listing.xlsx" \
  --naics-local-path "./data/NAICS_PhilaRegion_clean_snapshot.csv" \
  --maps-extractor-glob "./data/maps_extractor/*.csv" \
  --skip-geocode \
  --counties "Bucks,Montgomery,Philadelphia,Chester,Delaware" \
  --qa
```

### 5.2 Daily Rescoring

After initial build, rescore leads daily to reflect updated data:

```bash
python -m src.jobs.rescore_daily
```

This job:
- Reads entities from DuckDB
- Recalculates scores based on current rules
- Updates `lead_score` table
- Exports `daily_scores_YYYYMMDD_HHMM.csv`

**Scheduling:**
Run daily at 7:15 AM (or configure via `SCHEDULE_RESCORE` in `.env`).

### 5.3 CRM Sync

Push Tier A and B leads to Bigin CRM:

```bash
python -m src.jobs.push_to_bigin
```

**Dry Run (Recommended First):**
```bash
python -m src.jobs.push_to_bigin --dry-run --limit 5
```

This will:
- Show what would be created/updated
- Display sample payloads
- Not make any API calls

**Production Run:**
```bash
python -m src.jobs.push_to_bigin
```

**With Limit:**
```bash
python -m src.jobs.push_to_bigin --limit 50
```

**What Gets Synced:**
- **Accounts**: Facility information, scores, sector data
- **Contacts**: Primary contact (if available)
- **Deals**: Opportunity records for Tier A leads

**Custom Fields:**
- `cf_lead_score` - Numeric score (0-100)
- `cf_reason_codes` - Comma-separated reason codes
- `cf_tank_capacity_bucket` - Capacity bucket (<1K, 1K-5K, etc.)
- `cf_generator_flag` - Boolean generator indicator
- `cf_sector_primary` - Business sector (Fleet, Healthcare, etc.)
- `cf_sector_confidence` - Sector confidence (0-100)

### 5.4 Power BI Export

Export data for Power BI dashboards:

```bash
python -m src.dashboards.export_powerbi
```

**Outputs:**
- `entity_YYYYMMDD_HHMM.csv` - Entity fact table
- `lead_score_YYYYMMDD_HHMM.csv` - Scoring results
- `signals_YYYYMMDD_HHMM.csv` - Signals dimension
- `crm_sync_YYYYMMDD_HHMM.csv` - CRM sync status
- `tierA_YYYYMMDD_HHMM.geojson` - Tier A sites for mapping
- `tierA_points_YYYYMMDD_HHMM.csv` - Tier A points (lat, lon, name, score, sector)

**Scheduling:**
Run daily at 7:20 AM (or configure via `SCHEDULE_EXPORT` in `.env`).

### 5.5 QA Reporting

Generate data quality report:

```bash
python -m src.jobs.qa_report
```

Or include in build:

```bash
python -m src.jobs.build_universe --qa ...
```

**Report Sections:**
- **Coverage**: Counts by county (total, diesel_like, active_like, geocoded, sector coverage)
- **Code Mapping**: SUBSTANCE_CODE → diesel_like crosstab, STATUS_CODE → active_like crosstab
- **Sector Composition**: Entities by sector (count, %, avg score)

**Output:**
- `qa_report_YYYYMMDD_HHMM.csv` - Full report
- `qa_summary_YYYYMMDD_HHMM.csv` - Summary statistics

---

## 6. Understanding Outputs

### 6.1 Entity CSV (entity_YYYYMMDD_HHMM.csv)

Primary fact table with all ingested entities.

**Key Columns:**
- `facility_id` - Unique identifier
- `facility_name` - Business name
- `address`, `city`, `state`, `zip`, `county` - Location data
- `latitude`, `longitude` - Coordinates
- `product_code` - Substance code (DIESL, BIDSL, etc.)
- `capacity_gal` - Capacity in gallons
- `status_code` - Status (C = Active, T = Terminated)
- `is_diesel_like` - Boolean diesel indicator
- `is_active_like` - Boolean active indicator
- `capacity_bucket` - Capacity category
- `sector_primary` - Business sector (from NAICS match)
- `sector_confidence` - Sector confidence (0-100)
- `distance_mi` - Distance from base address
- `source` - Data source identifier

**Use Cases:**
- Territory analysis
- Address validation
- Sector distribution analysis

### 6.2 Lead Score CSV (lead_score_YYYYMMDD_HHMM.csv)

Scoring results for each entity.

**Key Columns:**
- `entity_id` - Foreign key to entity
- `score` - Total score (0-100)
- `tier` - Tier assignment (Tier A, Tier B, Tier C, Park)
- `reason_codes` - Comma-separated reason codes
- `reason_text` - Human-readable explanation
- `updated_at` - Last update timestamp

**Tier Ranges:**
- **Tier A**: 80-100 (Highest priority)
- **Tier B**: 60-79 (Medium priority)
- **Tier C**: 40-59 (Lower priority)
- **Park**: < 40 (Not actively pursued)

**Use Cases:**
- Sales prioritization
- Territory planning
- Performance tracking

### 6.3 Signals CSV (signals_YYYYMMDD_HHMM.csv)

Dimension table for signal types.

**Key Columns:**
- `signal_id` - Signal identifier
- `signal_name` - Human-readable name
- `signal_category` - Category (tank, status, fleet, infrastructure, sector, places)
- `entity_id` - Foreign key to entity (for entity-specific signals)
- `signal_value` - Signal value (for entity-specific signals)

**Signal Types:**
- `diesel_like` - Diesel product present
- `active_like` - Active facility status
- `capacity_bucket` - Capacity category
- `sector` - Business sector (Fleet, Healthcare, etc.)
- `places` - Google Maps category

**Use Cases:**
- Signal analysis
- Filtering by signal type
- Understanding scoring factors

### 6.4 Tier A GeoJSON (tierA_YYYYMMDD_HHMM.geojson)

GeoJSON FeatureCollection for mapping Tier A sites.

**Properties:**
- `facility_id`, `facility_name`, `address`, `city`, `state`
- `score`, `tier`, `reason_text`
- `sector_primary`, `capacity_bucket`

**Geometry:**
- Type: Point
- Coordinates: `[longitude, latitude]` (GeoJSON format)

**Use Cases:**
- Import into mapping tools (Google Maps, QGIS, etc.)
- Territory visualization
- Route planning

### 6.5 Tier A Points CSV (tierA_points_YYYYMMDD_HHMM.csv)

Simplified CSV for quick map imports.

**Columns:**
- `latitude`, `longitude` - Coordinates
- `name` - Facility name
- `county` - County name
- `score` - Lead score
- `band` - Tier (Tier A)
- `sector_primary` - Business sector
- `distance_mi` - Distance from base

**Use Cases:**
- Quick import into Excel/Google Sheets
- Simple mapping tools
- Address list for sales teams

### 6.6 QA Report CSV (qa_report_YYYYMMDD_HHMM.csv)

Data quality audit report.

**Sections:**
1. **Coverage**: County-level statistics
2. **Code Mapping**: Product/status code distributions
3. **Sector Composition**: Sector breakdown with average scores

**Use Cases:**
- Data quality monitoring
- Coverage analysis
- Identifying data gaps

---

## 7. Troubleshooting

### 7.1 Common Errors

#### "File not found" Error

**Problem:** System can't find input file.

**Solution:**
- Verify file path is correct
- Use absolute path if relative path fails
- Check file permissions

```bash
# Use absolute path
python -m src.jobs.build_universe --pa-tanks-path "C:/Users/YourName/data/PAStorage_Tank_Listing.xlsx"
```

#### "Missing required headers" Error

**Problem:** CSV file doesn't have expected columns.

**Solution:**
- Check that file has required columns (see section 4.1)
- System uses fuzzy matching, but some columns are required
- Verify column names match expected patterns

#### "GOOGLE_MAPS_API_KEY not set" Error

**Problem:** Geocoding attempted without API key.

**Solution:**
- Add API key to `.env` file, OR
- Use `--skip-geocode` flag, OR
- Use CodeCanyon Maps Extractor (provides coordinates)

```bash
# Skip geocoding
python -m src.jobs.build_universe --skip-geocode ...

# Or add to .env
GOOGLE_MAPS_API_KEY=your_key_here
```

#### DuckDB Lock Error

**Problem:** Database is locked by another process.

**Solution:**
- Close other processes using the database
- Wait for previous build to complete
- Delete lock file if process crashed: `rm data/leadgen.duckdb.wal`

#### Memory Error

**Problem:** Out of memory processing large files.

**Solution:**
- Process in smaller batches
- Use `--geocode-limit` to reduce geocoding
- Increase system RAM
- Close other applications

### 7.2 Debugging Tips

#### Enable Verbose Logging

Check log files in `./logs/`:
- `build_universe.log` - Build process logs
- `rescore_daily.log` - Rescoring logs

Logs are in JSON format for easy parsing.

#### Check Database Contents

Query DuckDB directly:

```python
import duckdb
conn = duckdb.connect('data/leadgen.duckdb')
df = conn.execute('SELECT * FROM raw_pa_tanks LIMIT 10').df()
print(df)
```

#### Verify Data Ingestion

Check row counts:

```python
import duckdb
conn = duckdb.connect('data/leadgen.duckdb')
print("PA Tanks:", conn.execute('SELECT COUNT(*) FROM raw_pa_tanks').fetchone()[0])
print("NAICS:", conn.execute('SELECT COUNT(*) FROM raw_naics_local').fetchone()[0])
print("Maps:", conn.execute('SELECT COUNT(*) FROM raw_maps_extractor').fetchone()[0])
```

#### Test Individual Components

Run unit tests:

```bash
pytest tests/test_rules.py -v
pytest tests/test_addresses.py -v
pytest tests/test_naics_local.py -v
```

### 7.3 Performance Optimization

#### Geocoding Performance

- **Use caching**: System caches geocoding results automatically
- **Limit geocoding**: Use `--geocode-limit` for large datasets
- **Rate limiting**: Adjust `--geocode-qps` to avoid API throttling
- **Batch processing**: System processes in batches automatically

#### Database Performance

- **Indexes**: System creates spatial indexes automatically
- **Cleanup**: Periodically clean old data from database
- **Compression**: DuckDB compresses data automatically

#### Memory Usage

- **Streaming**: System processes data in chunks where possible
- **Cache management**: Clear cache directory if it grows too large
- **Batch size**: Adjust `--geocode-batch-size` if needed

---

## 8. Best Practices

### 8.1 Data Management

#### Regular Backups

**Database Backup:**
```bash
# Copy database file
cp data/leadgen.duckdb data/backups/leadgen_YYYYMMDD.duckdb
```

**Output Backup:**
```bash
# Archive output files
tar -czf backups/outputs_YYYYMMDD.tar.gz out/
```

#### Data Retention

- Keep last 30 days of output files
- Archive older files to cold storage
- Clean cache directory monthly

#### Version Control

- Keep input data files in versioned directories
- Tag database backups with dates
- Document data source versions

### 8.2 Regular Maintenance

#### Daily Tasks

- Run `build_universe` to ingest new data
- Run `rescore_daily` to update scores
- Run `export_powerbi` to refresh dashboards
- Review QA reports for data quality issues

#### Weekly Tasks

- Review CRM sync status
- Check for new data sources
- Verify API quotas/limits
- Clean up old output files

#### Monthly Tasks

- Archive old database backups
- Review and update scoring rules if needed
- Update documentation
- Performance review

### 8.3 Backup Strategies

#### Database Backups

**Automated Backup Script (Windows):**
```powershell
# backup_db.ps1
$date = Get-Date -Format "yyyyMMdd"
Copy-Item "data\leadgen.duckdb" "data\backups\leadgen_$date.duckdb"
```

**Schedule via Task Scheduler:**
- Run daily at 2:00 AM
- Keep last 7 days of backups

#### Configuration Backups

- Version control `.env` file (without secrets)
- Document configuration changes
- Keep backup of `src/config.py` if customized

### 8.4 Security Best Practices

#### API Key Management

- Never commit `.env` file to version control
- Rotate API keys regularly
- Use environment variables in production
- Restrict API key permissions

#### Data Privacy

- Secure database files
- Encrypt sensitive data if required
- Follow data retention policies
- Audit data access

---

## 9. Advanced Usage

### 9.1 Custom Scoring Rules

Scoring rules are defined in `src/score/rules.py`.

**Modifying Rules:**

1. Edit `SCORING_RULES` dictionary:
```python
SCORING_RULES: Dict[str, int] = {
    "D_TANK": 40,      # Diesel tank present
    "CAP_20K": 25,     # Capacity >= 20K gallons
    # Add or modify rules here
}
```

2. Update tier thresholds if needed:
```python
TIER_A_MIN = 80
TIER_B_MIN = 60
TIER_C_MIN = 40
```

3. Rebuild and rescore:
```bash
python -m src.jobs.build_universe ...
python -m src.jobs.rescore_daily
```

**Adding New Signals:**

1. Add signal detection logic in `src/score/scorer.py`
2. Add reason code in `src/score/reasons.py`
3. Update `SCORING_RULES` dictionary
4. Test with unit tests

### 9.2 Adding New Data Sources

**Step 1: Create Ingestion Module**

Create `src/ingest/your_source.py`:

```python
"""Your data source ingestion module."""
import pandas as pd
from typing import Optional

def ingest_your_source(file_path: Optional[str] = None) -> pd.DataFrame:
    """
    Ingest your data source.
    
    Returns:
        Standardized DataFrame
    """
    # Read and process data
    df = pd.read_csv(file_path)
    
    # Normalize to standard format
    result_df = pd.DataFrame({
        'facility_name': df['name'],
        'address': df['address'],
        # ... other fields
    })
    
    return result_df
```

**Step 2: Integrate into Build**

Add to `src/jobs/build_universe.py`:

```python
from src.ingest.your_source import ingest_your_source

# In main():
your_df = ingest_your_source()
# Merge with entities
```

**Step 3: Add Merge Logic**

Create merge function in `src/entity/merge.py`:

```python
def merge_your_source(entity_df: pd.DataFrame, your_df: pd.DataFrame) -> pd.DataFrame:
    """Merge your source data into entities."""
    # Matching logic
    # Signal attachment
    return entity_df
```

### 9.3 API Integration

#### Bigin CRM Custom Fields

**Creating Custom Fields:**

The system automatically detects and creates custom fields on first run. To manually create:

1. Log into Bigin CRM
2. Go to Settings → Custom Fields
3. Create fields:
   - `cf_lead_score` (Number)
   - `cf_reason_codes` (Text)
   - `cf_tank_capacity_bucket` (Picklist)
   - `cf_generator_flag` (Checkbox)
   - `cf_sector_primary` (Picklist)
   - `cf_sector_confidence` (Number)

**Picklist Values for `cf_sector_primary`:**
- Fleet
- Construction
- Healthcare
- Education
- Utilities_DataCenters
- Industrial_Manufacturing
- Public_Government
- Retail_Commercial
- Unknown

#### API Key Registration

##### Google Maps API Key (Optional)

**When Needed:** Only required if you want to geocode addresses. Not needed if using CodeCanyon Google Maps scraper (which provides coordinates).

**Step-by-Step Registration:**

1. **Go to Google Cloud Console**
   - Visit: https://console.cloud.google.com/
   - Sign in with your Google account

2. **Create or Select a Project**
   - Click the project dropdown at the top
   - Click "New Project" or select an existing project
   - Enter project name (e.g., "Foxfuel Lead Gen")
   - Click "Create"

3. **Enable Geocoding API**
   - In the left menu, go to "APIs & Services" → "Library"
   - Search for "Geocoding API"
   - Click on "Geocoding API"
   - Click "Enable"

4. **Create API Key**
   - Go to "APIs & Services" → "Credentials"
   - Click "Create Credentials" → "API Key"
   - Your API key will be displayed
   - **Important:** Copy the key immediately (you won't be able to see it again)

5. **Restrict API Key (Recommended for Security)**
   - Click "Restrict Key" button
   - Under "API restrictions":
     - Select "Restrict key"
     - Check only "Geocoding API"
   - Under "Application restrictions" (optional but recommended):
     - Select "IP addresses" for server-side usage
     - Add your server IP addresses
   - Click "Save"

6. **Add to .env File**
   ```env
   GOOGLE_MAPS_API_KEY=AIzaSyYourActualKeyHere123456789
   ```

**Quota and Pricing:**

- **Free Tier:** $200 credit per month (new accounts)
- **Geocoding Cost:** $5.00 per 1,000 requests
- **Free Requests:** 40,000 requests/month with $200 credit
- **Caching:** System caches results to minimize API calls
- **Monitoring:** Check usage in Cloud Console → "APIs & Services" → "Dashboard"

**Troubleshooting:**

- **"API key not valid"**: Verify key is copied correctly, check API restrictions
- **"Quota exceeded"**: Wait for quota reset or upgrade billing account
- **"Geocoding API not enabled"**: Enable the API in Cloud Console

##### Bigin CRM Authentication (OAuth or Access Token)

**When Needed:** Required for syncing leads to Bigin CRM.

**Two Authentication Methods:**

The system supports both OAuth (recommended) and direct access tokens.

###### Method 1: OAuth (Recommended)

OAuth uses a refresh token to automatically obtain access tokens, which is more secure and doesn't require manual token renewal.

**Step-by-Step OAuth Setup:**

1. **Create Zoho API Application**
   - Go to https://api-console.zoho.com/
   - Sign in with your Zoho account
   - Click "Add Client" or "Create Client"
   - Select "Server-based Applications"
   - Enter application details:
     - Name: "Foxfuel Lead Gen Integration"
     - Homepage URL: Your website or `https://localhost`
     - Authorized Redirect URIs: `https://localhost` (or your callback URL)
   - Click "Create"

2. **Get Client Credentials**
   - After creation, you'll see:
     - **Client ID** (e.g., `1000.XXXXXXXXXXXXX`)
     - **Client Secret** (a long alphanumeric string)
   - Copy both values

3. **Generate Refresh Token**
   - In the API Console, click on your client
   - Go to "Generate Code" or "Generate Refresh Token"
   - Select scopes:
     - `Bigin.bigin.accounts.ALL`
     - `Bigin.bigin.contacts.ALL`
     - `Bigin.bigin.deals.ALL`
     - `Bigin.bigin.settings.ALL` (for custom fields)
   - Click "Generate"
   - You'll be redirected to authorize the application
   - After authorization, you'll receive a **Refresh Token**

4. **Get Base URL**
   - Your Bigin API base URL is typically: `https://www.zohoapis.com/bigin/v2`
   - Some instances may use different URLs (check your Bigin documentation)

5. **Add to .env File**
   ```env
   BIGIN_CLIENT_ID=1000.XXXXXXXXXXXXX
   BIGIN_CLIENT_SECRET=your_client_secret_here
   BIGIN_REFRESH_TOKEN=1000.XXXXXXXXXXXXXXXXXXXXXXXX
   BIGIN_BASE_URL=https://www.zohoapis.com/bigin/v2
   ```

**How OAuth Works:**
- System automatically refreshes access tokens when they expire
- Refresh tokens are long-lived and don't expire (unless revoked)
- Access tokens are cached and refreshed 5 minutes before expiry
- No manual token management required

###### Method 2: Direct Access Token (Legacy)

If you already have an access token, you can use it directly (though it will expire and need manual renewal).

**Step-by-Step Access Token Setup:**

1. **Log into Bigin CRM**
   - Visit your Bigin CRM instance
   - Log in with your administrator account

2. **Navigate to Developer Settings**
   - Click your profile icon (top right)
   - Go to "Settings" → "Developer Settings"

3. **Generate Access Token**
   - Look for "Access Tokens" or "API Tokens" section
   - Click "Generate Token" or "Create Token"
   - Enter a token name (e.g., "Foxfuel Lead Gen Integration")
   - Select scopes/permissions:
     - `Accounts` - Read and Write
     - `Contacts` - Read and Write
     - `Deals` - Read and Write
     - `Custom Fields` - Read and Write (if available)
   - Click "Generate" or "Create"

4. **Copy Access Token**
   - The token will be displayed once
   - **Important:** Copy it immediately (you won't be able to see it again)

5. **Add to .env File**
   ```env
   BIGIN_ACCESS_TOKEN=your_token_here
   BIGIN_BASE_URL=https://www.zohoapis.com/bigin/v2
   ```

**Note:** Access tokens expire (typically after 1 hour). OAuth method is recommended for production use.

**Token Security:**

- **Never commit tokens to version control**
- Store in `.env` file (already in `.gitignore`)
- Rotate refresh tokens periodically (every 90 days recommended)
- Revoke old tokens when generating new ones
- Keep client secret secure (treat like a password)

**Testing Your Configuration:**

```bash
# Test Bigin connection (dry-run)
python -m src.jobs.push_to_bigin --dry-run --limit 1
```

**Troubleshooting:**

- **"Bigin authentication not configured"**: 
  - For OAuth: Ensure all three (CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN) are set
  - For Access Token: Ensure BIGIN_ACCESS_TOKEN is set
  
- **"OAuth token refresh failed"**: 
  - Verify client ID, secret, and refresh token are correct
  - Check that refresh token hasn't been revoked
  - Ensure scopes are correct in Zoho API Console
  
- **"Invalid token"**: 
  - Verify token is copied correctly, check for extra spaces
  - For access tokens: Token may have expired, generate new token or switch to OAuth
  
- **"Unauthorized"**: 
  - Token may have expired (access tokens expire after ~1 hour)
  - Refresh token may have been revoked
  - Check token permissions/scopes
  
- **"Insufficient permissions"**: 
  - Ensure token has required scopes (Accounts, Contacts, Deals, Settings)
  - Regenerate token with correct scopes
  
- **"API endpoint not found"**: 
  - Verify BIGIN_BASE_URL is correct (typically `https://www.zohoapis.com/bigin/v2`)
  - Check your Bigin instance documentation for correct API URL

### 9.4 Automation and Scheduling

#### Windows Task Scheduler

**Create Scheduled Task:**

1. Open Task Scheduler
2. Create Basic Task
3. Set trigger (daily at 6:30 AM)
4. Action: Start a program
5. Program: `python`
6. Arguments: `-m src.jobs.build_universe --pa-tanks-path "..." --qa`
7. Start in: `C:\path\to\foxfuel_leadgen`

#### Linux Cron

**Add to crontab:**

```bash
crontab -e
```

**Example schedule:**
```
# Build universe daily at 6:30 AM
30 6 * * * cd /path/to/foxfuel_leadgen && /usr/bin/python3 -m src.jobs.build_universe --pa-tanks-path "..." --qa

# Rescore daily at 7:15 AM
15 7 * * * cd /path/to/foxfuel_leadgen && /usr/bin/python3 -m src.jobs.rescore_daily

# Export at 7:20 AM
20 7 * * * cd /path/to/foxfuel_leadgen && /usr/bin/python3 -m src.dashboards.export_powerbi
```

#### PowerShell Scheduled Jobs

**Create scheduled job:**

```powershell
$action = New-ScheduledTaskAction -Execute "python" -Argument "-m src.jobs.build_universe --pa-tanks-path `"data\PAStorage_Tank_Listing.xlsx`" --qa" -WorkingDirectory "C:\path\to\foxfuel_leadgen"
$trigger = New-ScheduledTaskTrigger -Daily -At 6:30AM
Register-ScheduledTask -TaskName "Foxfuel Build Universe" -Action $action -Trigger $trigger
```

---

## 10. Appendices

### 10.1 Data Dictionary

See `docs/data_dictionary.md` for complete field descriptions.

### 10.2 Field Mappings

#### PA Tanks Header Mappings

| Expected Field | Common Variations |
|----------------|-------------------|
| facility_name | PF_NAME, MAILING_NAME, NAME |
| facility_id | PF_SITE_ID, SITE_ID, ID |
| address_1 | LOCAD_PF_ADDRESS_1, ADDRESS, STREET |
| city | LOCAD_LOCAD_PF_CITY, CITY |
| state | LOCAD_PF_STATE, STATE |
| zip | LOCAD_PF_ZIP_CODE, ZIP, ZIP_CODE |
| county | PF_COUNTY_NAME, COUNTY |
| product_code | SUBSTANCE_CODE, PRODUCT_CODE |
| capacity | CAPACITY, CAPACITY_GAL |
| status_code | STATUS_CODE, STATUS |

#### NAICS Header Mappings

| Expected Field | Common Variations |
|----------------|-------------------|
| business_name | COMPANY NAME, BUSINESS_NAME, NAME |
| address | STREET ADDRESS, ADDRESS |
| city | CITY |
| state | STATE |
| zip | ZIP CODE, ZIP, POSTAL_CODE |
| county | COUNTY |
| naics_code | NAICS, NAICS_CODE |
| naics_title | NAICS DESCRIPTION, NAICS_TITLE |

### 10.3 Reference Tables

#### Product Code Classifications

**Diesel-Like Codes:**
- DIESL - Diesel
- BIDSL - Biodiesel
- HO - Heating Oil
- KERO - Kerosene

**Non-Diesel Codes:**
- GAS - Gasoline
- AVGAS - Aviation Gasoline
- JET - Jet Fuel
- ETHNL - Ethanol
- HZSUB - Hazardous Substance
- OTHER - Other

#### Status Codes

**Active:**
- C - Active

**Inactive:**
- T - Terminated
- All other codes

#### Capacity Buckets

| Bucket | Range | Points |
|--------|-------|--------|
| 20K+ | >= 20,000 | +25 |
| 10K-20K | 10,000 - 19,999 | +20 |
| 5K-10K | 5,000 - 9,999 | +15 |
| 1K-5K | 1,000 - 4,999 | +8 |
| <1K | < 1,000 | 0 |

#### Sector Classifications

| Sector | NAICS Prefixes | Points | Keywords |
|--------|----------------|--------|----------|
| Fleet and Transportation | 484, 485, 488 | +20 | trucking, bus, logistics |
| Construction | 23 | +15 | construction, site work |
| Healthcare | 621, 622, 623 | +15 | hospital, medical center |
| Education | 611 | +10 | school, university |
| Utilities and Data Centers | 22, 518210 | +15 | utility, data center |
| Industrial and Manufacturing | 31, 32, 33 | +10 | plant, manufacturing |
| Public and Government | 92 | +5 | township, municipal |
| Retail and Commercial Fueling | 447110, 447190 | +5 | gas station, convenience |

### 10.4 Scoring Rules Reference

**Positive Signals:**
- D_TANK: +40 (Diesel/fuel oil present)
- CAP_20K: +25 (Capacity >= 20K)
- CAP_10K: +20 (Capacity 10K-20K)
- CAP_5K: +15 (Capacity 5K-10K)
- CAP_1K: +8 (Capacity 1K-5K)
- ACTIVE: +15 (Active status)
- SECTOR_FLEET: +20 (Fleet sector)
- SECTOR_CONSTR: +15 (Construction sector)
- SECTOR_HEALTH: +15 (Healthcare sector)
- SECTOR_EDU: +10 (Education sector)
- SECTOR_UTIL_DC: +15 (Utilities/Data Centers)
- SECTOR_MFG: +10 (Industrial/Manufacturing)
- SECTOR_PUBLIC: +5 (Public/Government)
- SECTOR_RETAIL: +5 (Retail/Commercial Fueling)

**Negative Signals:**
- INCUMBENT: -10 (Incumbent on site)
- DNC: -15 (Do not contact)

**Score Cap:** 100 points maximum

### 10.5 Command Reference

#### build_universe

```bash
python -m src.jobs.build_universe [OPTIONS]

Required:
  --pa-tanks-path PATH    Path to PA DEP storage tank file

Optional:
  --naics-local-path PATH Path to NAICS CSV file
  --maps-extractor-glob PATTERN  Glob pattern for Maps Extractor CSVs
  --counties LIST         Comma-separated county list
  --base-address ADDRESS  Base address for distance calculations
  --skip-geocode          Skip geocoding
  --geocode-limit N       Maximum records to geocode
  --geocode-qps RATE      Geocoding rate limit (QPS)
  --geocode-batch-size N  Batch size for cache inserts
  --qa                    Generate QA report
  --skip-fmcsa            Skip FMCSA ingestion
  --skip-echo             Skip ECHO ingestion
  --skip-eia              Skip EIA ingestion
  --skip-osm              Skip OSM ingestion
  --skip-procurement      Skip procurement ingestion
  --skip-permits          Skip permits ingestion
```

#### rescore_daily

```bash
python -m src.jobs.rescore_daily
```

#### push_to_bigin

```bash
python -m src.jobs.push_to_bigin [OPTIONS]

Options:
  --dry-run              Preview without making API calls
  --limit N              Limit number of records to sync
```

#### export_powerbi

```bash
python -m src.dashboards.export_powerbi
```

#### qa_report

```bash
python -m src.jobs.qa_report
```

### 10.6 Support and Resources

**Documentation:**
- README.md - Quick start guide
- docs/data_dictionary.md - Complete field reference
- docs/talk_tracks.md - Sales talk track templates

**Testing:**
- Run `pytest tests/` to verify system functionality
- Sample data in `samples/` directory

**Logs:**
- Check `logs/` directory for detailed execution logs
- Logs are in JSON format for easy parsing

**GitHub Repository:**
- https://github.com/dynamix77/foxfuel_leadgen
- Report issues via GitHub Issues
- Check for updates and new features

---

## Document Revision History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | November 2025 | Initial release |

---

**End of User Manual**

