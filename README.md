# Foxfuel Lead Generation System

A Python-based lead generation pipeline for a fuel distributor in Southeast Pennsylvania. The system ingests data from multiple sources, normalizes and deduplicates entities, scores leads, and syncs to Bigin CRM.

## Features

- **PA DEP Storage Tank Ingestion**: Robust header mapping with fuzzy matching, diesel/status classification, capacity bucketing, and geocoding
- **Entity Processing**: Normalization, deduplication by geohash and name similarity, and merging from multiple sources
- **Lead Scoring**: Rule-based scoring system with tier assignment (A, B, C, Park) and human-readable reasons
- **CRM Integration**: Bigin API client with idempotent sync and custom field mapping
- **Power BI Exports**: Star schema CSVs and GeoJSON for Tier A sites

## Requirements

- Python 3.11+
- See `requirements.txt` for dependencies

## Setup

1. **Clone and install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure environment:**
   ```bash
   cp .env.example .env
   # Edit .env and add your API keys:
   # GOOGLE_MAPS_API_KEY=your_key_here
   # BIGIN_ACCESS_TOKEN=your_token_here
   ```

3. **Create directories:**
   ```bash
   mkdir -p data cache out
   ```

## Configuration

All configuration is managed through `src/config.py` using Pydantic BaseSettings. Values can be overridden via:

- `.env` file (see `.env.example`)
- Environment variables
- CLI arguments (where supported)

**Default settings:**
- Base address: "2450 Old Welsh Road, Willow Grove, PA 19090"
- Counties: Bucks, Montgomery, Philadelphia, Chester, Delaware
- Database: `./data/leadgen.duckdb`
- Cache: `./cache`
- Output: `./out`

## Usage

### PA Tanks Ingestion

Ingest PA DEP storage tank data:

```bash
python -m src.jobs.build_universe --pa-tanks-path path/to/PAStorage_Tank_Listing.xlsx
```

**Options:**
- `--pa-tanks-path`: Path to CSV or XLSX file (required)
- `--no-geocode`: Skip geocoding (faster for testing)

**Outputs:**
- DuckDB table: `raw_pa_tanks`
- Preview CSV: `./out/pa_tanks_preview.csv` (first 1000 rows)

### Daily Rescoring

Recompute scores and generate daily CSV:

```bash
python -m src.jobs.rescore_daily
```

### Push to Bigin

Sync Tier A and B leads to Bigin CRM:

```bash
python -m src.jobs.push_to_bigin
```

### Power BI Export

Export star schema CSVs and GeoJSON:

```bash
python -m src.dashboards.export_powerbi
```

## Sample Run

### Basic Build (with sample data)
```bash
python -m src.jobs.build_universe \
  --input samples/pa_tanks_sample.csv \
  --skip-geocode \
  --counties "Bucks,Montgomery,Philadelphia,Chester,Delaware"
```

### Full Build (with geocoding)
```bash
python -m src.jobs.build_universe \
  --input ./data/PA_Tanks_SE_clean_snapshot.csv \
  --skip-geocode false \
  --geocode-limit 250 \
  --geocode-qps 4.0 \
  --counties "Bucks,Montgomery,Philadelphia,Chester,Delaware" \
  --qa
```

### Rescore and Export
```bash
python -m src.jobs.rescore_daily
python -m src.dashboards.export_powerbi
```

### CRM Dry-Run
```bash
python -m src.jobs.push_to_bigin --dry-run true --limit 25
```

### Mock Data Test

Use the provided sample data:

```bash
python -m src.jobs.build_universe --pa-tanks-path samples/pa_tanks_sample.csv --skip-geocode
```

The sample file (`samples/pa_tanks_sample.csv`) contains ~17 rows covering:
- All product code types (DIESL, BIDSL, HO, KERO, GAS, AVGAS, JET, ETHNL, HZSUB)
- Active (C) and inactive (T) status codes
- Capacity edge cases (999, 1000, 5000, 10000, 20000, 25000)
- Duplicate records for deduplication testing
- Counties inside and outside Southeast PA for filtering

Expected output:
- 2 rows processed (1 filtered out - non-diesel or wrong county)
- Preview CSV with diesel classification, status, and capacity buckets
- DuckDB table populated

## Testing

Run unit tests:

```bash
pytest tests/
```

**Test coverage:**
- `test_rules.py`: Classification logic (diesel, active, capacity buckets)
- `test_addresses.py`: Address normalization and street keys

## Data Flow

1. **Ingestion**: PA tanks → header mapping → classification → geocoding → DuckDB
2. **Normalization**: Address standardization, street key creation
3. **Deduplication**: Geohash clustering (precision 7) + name similarity (≥90%)
4. **Scoring**: Rule-based scoring with tier assignment
5. **CRM Sync**: Upsert to Bigin with idempotency tracking
6. **Export**: Power BI star schema + GeoJSON

## Scoring Rules

**Positive Signals:**
- Diesel/fuel oil present: +40
- Capacity buckets: 20K+ (+25), 10K-20K (+20), 5K-10K (+15), 1K-5K (+8)
- Active status: +15
- FMCSA fleet size: ≥50 (+20), 10-49 (+10)
- Critical infrastructure: Hospital (+15), School (+15), Data center (+15)
- ECHO presence: +10
- Distance: ≤25 miles (+10), 25-40 miles (+5)
- Website intent: +10

**Negative Signals:**
- Incumbent on site: -10
- CRM DNC flag: -15

**Score Bands:**
- Tier A: 80-100
- Tier B: 60-79
- Tier C: 40-59
- Park: < 40

**Score Cap:** 100

## Performance

Target performance: Process 25,000 rows in under 3 minutes with caching enabled.

**Optimization strategies:**
- DuckDB caching for geocoding results
- Batch processing with progress bars
- API rate limiting with tenacity retry logic

## Project Structure

```
foxfuel_leadgen/
├── README.md
├── requirements.txt
├── .env.example
├── src/
│   ├── config.py
│   ├── utils/          # Address, geocode, fuzzy, I/O utilities
│   ├── ingest/         # Data ingestion modules
│   ├── entity/         # Normalization, deduplication, merging
│   ├── score/          # Scoring rules, scorer, reasons
│   ├── crm/            # Bigin API client, payloads, sync
│   ├── jobs/           # Orchestration jobs
│   └── dashboards/     # Power BI exports
├── tests/              # Unit tests
├── docs/               # Documentation (talk tracks)
├── data/               # DuckDB database
├── cache/              # API cache files
└── out/                # Output CSVs and GeoJSON
```

## Logging

Structured logging is used throughout. Log levels:
- INFO: General progress and summaries
- DEBUG: Detailed processing information
- WARNING: Non-fatal issues
- ERROR: Failures requiring attention

## API Quotas

The system respects API quotas with:
- Exponential backoff (tenacity)
- Request retries (max 3 attempts)
- DuckDB caching for geocoding results

## License

Proprietary - Internal use only

## Support

For issues or questions, contact the development team.

