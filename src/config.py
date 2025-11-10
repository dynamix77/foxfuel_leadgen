"""Configuration management using Pydantic BaseSettings."""
import os
from pathlib import Path
from typing import List, Dict
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings with environment variable overrides."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # API Keys
    google_maps_api_key: str = Field(default="", alias="GOOGLE_MAPS_API_KEY")
    bigin_access_token: str = Field(default="", alias="BIGIN_ACCESS_TOKEN")
    
    # Base location
    base_address: str = Field(
        default="2450 Old Welsh Road, Willow Grove, PA 19090",
        alias="BASE_ADDRESS"
    )
    
    # Counties (comma-separated in env, or default list)
    counties: List[str] = Field(
        default_factory=lambda: ["Bucks", "Montgomery", "Philadelphia", "Chester", "Delaware"]
    )
    
    # Data paths
    data_dir: Path = Field(default_factory=lambda: Path("./data"))
    cache_dir: Path = Field(default_factory=lambda: Path("./cache"))
    out_dir: Path = Field(default_factory=lambda: Path("./out"))
    
    # Database
    db_path: Path = Field(default_factory=lambda: Path("./data/leadgen.duckdb"))
    
    # NAICS local data
    naics_local_path: Path = Field(
        default_factory=lambda: Path("./data/NAICS_PhilaRegion_clean_snapshot.csv"),
        alias="NAICS_LOCAL_PATH"
    )
    naics_match_radius_meters: int = Field(default=150, alias="NAICS_MATCH_RADIUS_METERS")
    naics_name_similarity_min: int = Field(default=88, alias="NAICS_NAME_SIMILARITY_MIN")
    
    # Geography
    counties_sepa: List[str] = Field(
        default_factory=lambda: ["Bucks", "Montgomery", "Philadelphia", "Chester", "Delaware"],
        alias="COUNTIES_SEPA"
    )
    county_polygons: Dict[str, List] = Field(default_factory=dict, alias="COUNTY_POLYGONS")
    
    # FMCSA
    fmcsa_snapshot_path: Path = Field(
        default_factory=lambda: Path("./data/fmcsa_snapshot.csv"),
        alias="FMCSA_SNAPSHOT_PATH"
    )
    fmcsa_api_base: str = Field(default="https://safer.fmcsa.dot.gov", alias="FMCSA_API_BASE")
    
    # ECHO
    echo_api_base: str = Field(default="https://echo.epa.gov", alias="ECHO_API_BASE")
    echo_naics_filters: List[str] = Field(
        default_factory=lambda: ["622110", "621111", "623110", "611110", "518210", "493110", "493120", 
                                 "485410", "485510", "237110", "237120", "237130", "238910", "221122", "221330"],
        alias="ECHO_NAICS_FILTERS"
    )
    
    # EIA
    eia_form860_path: Path = Field(
        default_factory=lambda: Path("./data/eia_form860_generators.csv"),
        alias="EIA_FORM860_PATH"
    )
    
    # OSM
    overpass_api: str = Field(default="https://overpass-api.de/api/interpreter", alias="OVERPASS_API")
    
    # Procurement and Permits
    procurement_sources: List[str] = Field(default_factory=list, alias="PROCUREMENT_SOURCES")
    permits_sources: List[str] = Field(default_factory=list, alias="PERMITS_SOURCES")
    
    # Cache directories
    cache_geocode_db: Path = Field(
        default_factory=lambda: Path("./cache/geocode_cache.duckdb"),
        alias="CACHE_GEOCODE_DB"
    )
    cache_echo_dir: Path = Field(default_factory=lambda: Path("./cache/echo"), alias="CACHE_ECHO_DIR")
    cache_fmcsa_dir: Path = Field(default_factory=lambda: Path("./cache/fmcsa"), alias="CACHE_FMCSA_DIR")
    cache_eia_dir: Path = Field(default_factory=lambda: Path("./cache/eia"), alias="CACHE_EIA_DIR")
    cache_osm_dir: Path = Field(default_factory=lambda: Path("./cache/osm"), alias="CACHE_OSM_DIR")
    cache_maps_extractor_dir: Path = Field(default_factory=lambda: Path("./cache/maps_extractor"), alias="CACHE_MAPS_EXTRACTOR_DIR")
    cache_procurement_dir: Path = Field(
        default_factory=lambda: Path("./cache/procurement"),
        alias="CACHE_PROCUREMENT_DIR"
    )
    cache_permits_dir: Path = Field(
        default_factory=lambda: Path("./cache/permits"),
        alias="CACHE_PERMITS_DIR"
    )
    
    # Schedules (documented for external schedulers)
    schedule_build: str = Field(default="daily 06:30", alias="SCHEDULE_BUILD")
    schedule_rescore: str = Field(default="daily 07:15", alias="SCHEDULE_RESCORE")
    schedule_export: str = Field(default="daily 07:20", alias="SCHEDULE_EXPORT")
    schedule_crm_push: str = Field(default="weekdays 07:30", alias="SCHEDULE_CRM_PUSH")
    schedule_procurement_watch: str = Field(default="hourly 07:00-19:00", alias="SCHEDULE_PROCUREMENT_WATCH")
    schedule_permits_watch: str = Field(default="daily 05:30", alias="SCHEDULE_PERMITS_WATCH")
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Parse counties from env if provided as comma-separated string
        if "COUNTIES" in os.environ:
            self.counties = [c.strip() for c in os.environ["COUNTIES"].split(",")]
        
        # Ensure directories exist
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.out_dir.mkdir(parents=True, exist_ok=True)
    
    @property
    def duckdb_path(self) -> str:
        """Return DuckDB path as string."""
        return str(self.db_path)


# Global settings instance
settings = Settings()

