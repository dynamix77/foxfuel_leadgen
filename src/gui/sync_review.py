"""Review and select records for Bigin sync."""
import tkinter as tk
from tkinter import ttk, messagebox
import pandas as pd
import duckdb
from typing import List, Dict, Optional
from src.config import settings
from src.crm.sync import is_synced


class SyncReviewDialog:
    """Dialog for reviewing and selecting records to sync to Bigin."""
    
    def __init__(self, parent):
        self.parent = parent
        self.dialog = tk.Toplevel(parent)
        self.dialog.title("Review & Select Records for Bigin Sync")
        self.dialog.geometry("1200x700")
        self.dialog.transient(parent)
        self.dialog.grab_set()
        
        # Data
        self.all_records: pd.DataFrame = pd.DataFrame()
        self.filtered_records: pd.DataFrame = pd.DataFrame()
        self.selected_ids: set = set()
        
        # Results
        self.result: Optional[List[str]] = None
        
        self.setup_ui()
        self.load_records()
        
    def setup_ui(self):
        """Setup the user interface."""
        main_frame = ttk.Frame(self.dialog, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Title
        title_label = ttk.Label(
            main_frame,
            text="Select Records to Sync to Bigin CRM",
            font=("Arial", 12, "bold")
        )
        title_label.pack(pady=(0, 10))
        
        # Filters Section
        filter_frame = ttk.LabelFrame(main_frame, text="Filters", padding="10")
        filter_frame.pack(fill=tk.X, pady=(0, 10))
        
        # Filter row 1
        filter_row1 = ttk.Frame(filter_frame)
        filter_row1.pack(fill=tk.X, pady=5)
        
        ttk.Label(filter_row1, text="Tier:").pack(side=tk.LEFT, padx=5)
        self.tier_var = tk.StringVar(value="All")
        tier_combo = ttk.Combobox(filter_row1, textvariable=self.tier_var, width=15, state="readonly")
        tier_combo['values'] = ("All", "Tier A", "Tier B", "Tier C")
        tier_combo.pack(side=tk.LEFT, padx=5)
        tier_combo.bind("<<ComboboxSelected>>", lambda e: self.apply_filters())
        
        ttk.Label(filter_row1, text="Sector:").pack(side=tk.LEFT, padx=5)
        self.sector_var = tk.StringVar(value="All")
        self.sector_combo = ttk.Combobox(filter_row1, textvariable=self.sector_var, width=20, state="readonly")
        self.sector_combo.pack(side=tk.LEFT, padx=5)
        self.sector_combo.bind("<<ComboboxSelected>>", lambda e: self.apply_filters())
        
        ttk.Label(filter_row1, text="County:").pack(side=tk.LEFT, padx=5)
        self.county_var = tk.StringVar(value="All")
        self.county_combo = ttk.Combobox(filter_row1, textvariable=self.county_var, width=15, state="readonly")
        self.county_combo.pack(side=tk.LEFT, padx=5)
        self.county_combo.bind("<<ComboboxSelected>>", lambda e: self.apply_filters())
        
        # Filter row 2
        filter_row2 = ttk.Frame(filter_frame)
        filter_row2.pack(fill=tk.X, pady=5)
        
        ttk.Label(filter_row2, text="Fuel Type:").pack(side=tk.LEFT, padx=5)
        self.fuel_var = tk.StringVar(value="All")
        fuel_combo = ttk.Combobox(filter_row2, textvariable=self.fuel_var, width=15, state="readonly")
        fuel_combo['values'] = ("All", "Diesel-like", "Non-diesel")
        fuel_combo.pack(side=tk.LEFT, padx=5)
        fuel_combo.bind("<<ComboboxSelected>>", lambda e: self.apply_filters())
        
        ttk.Label(filter_row2, text="Capacity:").pack(side=tk.LEFT, padx=5)
        self.capacity_var = tk.StringVar(value="All")
        capacity_combo = ttk.Combobox(filter_row2, textvariable=self.capacity_var, width=15, state="readonly")
        capacity_combo['values'] = ("All", "Large (10k+)", "Medium (5k-10k)", "Small (<5k)")
        capacity_combo.pack(side=tk.LEFT, padx=5)
        capacity_combo.bind("<<ComboboxSelected>>", lambda e: self.apply_filters())
        
        ttk.Label(filter_row2, text="Search:").pack(side=tk.LEFT, padx=5)
        self.search_var = tk.StringVar()
        search_entry = ttk.Entry(filter_row2, textvariable=self.search_var, width=20)
        search_entry.pack(side=tk.LEFT, padx=5)
        search_entry.bind("<KeyRelease>", lambda e: self.apply_filters())
        
        ttk.Label(filter_row2, text="Limit:").pack(side=tk.LEFT, padx=5)
        self.limit_var = tk.StringVar(value="")
        limit_entry = ttk.Entry(filter_row2, textvariable=self.limit_var, width=10)
        limit_entry.pack(side=tk.LEFT, padx=5)
        limit_entry.bind("<KeyRelease>", lambda e: self.apply_filters())
        
        # Selection controls
        select_frame = ttk.Frame(main_frame)
        select_frame.pack(fill=tk.X, pady=(0, 10))
        
        ttk.Button(select_frame, text="Select All", command=self.select_all).pack(side=tk.LEFT, padx=5)
        ttk.Button(select_frame, text="Deselect All", command=self.deselect_all).pack(side=tk.LEFT, padx=5)
        ttk.Button(select_frame, text="Select Tier A Only", command=self.select_tier_a).pack(side=tk.LEFT, padx=5)
        ttk.Button(select_frame, text="Select Top 10", command=self.select_top_10).pack(side=tk.LEFT, padx=5)
        ttk.Button(select_frame, text="Select Top 50", command=self.select_top_50).pack(side=tk.LEFT, padx=5)
        ttk.Button(select_frame, text="Clear Filters", command=self.clear_filters).pack(side=tk.LEFT, padx=5)
        
        self.count_label = ttk.Label(select_frame, text="0 records selected")
        self.count_label.pack(side=tk.RIGHT, padx=10)
        
        # Records table with scrollbars
        table_frame = ttk.Frame(main_frame)
        table_frame.pack(fill=tk.BOTH, expand=True)
        
        # Treeview with scrollbars
        scrollbar_y = ttk.Scrollbar(table_frame, orient=tk.VERTICAL)
        scrollbar_x = ttk.Scrollbar(table_frame, orient=tk.HORIZONTAL)
        
        self.tree = ttk.Treeview(
            table_frame,
            columns=("facility_name", "county", "tier", "score", "sector", "fuel_type", "capacity", "address"),
            show="tree headings",
            yscrollcommand=scrollbar_y.set,
            xscrollcommand=scrollbar_x.set,
            selectmode="extended"
        )
        
        scrollbar_y.config(command=self.tree.yview)
        scrollbar_x.config(command=self.tree.xview)
        
        # Configure columns
        self.tree.heading("#0", text="☑")
        self.tree.heading("facility_name", text="Facility Name")
        self.tree.heading("county", text="County")
        self.tree.heading("tier", text="Tier")
        self.tree.heading("score", text="Score")
        self.tree.heading("sector", text="Sector")
        self.tree.heading("fuel_type", text="Fuel Type")
        self.tree.heading("capacity", text="Capacity")
        self.tree.heading("address", text="Address")
        
        self.tree.column("#0", width=50, anchor=tk.CENTER)
        self.tree.column("facility_name", width=200)
        self.tree.column("county", width=100)
        self.tree.column("tier", width=80)
        self.tree.column("score", width=80)
        self.tree.column("sector", width=150)
        self.tree.column("fuel_type", width=100)
        self.tree.column("capacity", width=120)
        self.tree.column("address", width=250)
        
        # Bind checkbox click
        self.tree.bind("<Button-1>", self.on_tree_click)
        
        # Pack treeview and scrollbars
        self.tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        scrollbar_y.grid(row=0, column=1, sticky=(tk.N, tk.S))
        scrollbar_x.grid(row=1, column=0, sticky=(tk.W, tk.E))
        table_frame.grid_rowconfigure(0, weight=1)
        table_frame.grid_columnconfigure(0, weight=1)
        
        # Buttons
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=(10, 0))
        
        ttk.Button(button_frame, text="Cancel", command=self.cancel).pack(side=tk.RIGHT, padx=5)
        ttk.Button(button_frame, text="Sync Selected", command=self.sync_selected).pack(side=tk.RIGHT, padx=5)
        
    def load_records(self):
        """Load records from database."""
        conn = duckdb.connect(settings.duckdb_path)
        
        query = """
        SELECT 
            e.facility_id,
            e.facility_name,
            e.county,
            e.is_diesel_like,
            e.capacity_bucket,
            e.address,
            e.city,
            e.state,
            e.zip,
            s.score,
            s.tier,
            s.reason_codes,
            COALESCE(sig_sector.signal_value, 'Unknown') as sector_primary,
            e.sector_confidence
        FROM raw_pa_tanks e
        LEFT JOIN lead_score s ON e.facility_id = s.entity_id
        LEFT JOIN signals sig_sector ON CAST(e.facility_id AS VARCHAR) = CAST(sig_sector.entity_id AS VARCHAR) 
            AND sig_sector.signal_type = 'sector'
        WHERE s.tier IN ('Tier A', 'Tier B')
        ORDER BY s.score DESC, e.facility_name
        """
        
        self.all_records = conn.execute(query).df()
        conn.close()
        
        # Filter out already synced
        self.all_records = self.all_records[
            ~self.all_records["facility_id"].apply(lambda x: is_synced(x, settings.duckdb_path))
        ]
        
        # Populate filter dropdowns
        self.populate_filters()
        
        # Apply initial filters
        self.apply_filters()
        
    def populate_filters(self):
        """Populate filter dropdowns with available values."""
        # Sectors
        sectors = ["All"] + sorted([s for s in self.all_records["sector_primary"].dropna().unique() if s != "Unknown"])
        self.sector_combo['values'] = sectors
        
        # Counties
        counties = ["All"] + sorted([c for c in self.all_records["county"].dropna().unique()])
        self.county_combo['values'] = counties
        
    def apply_filters(self):
        """Apply filters to records."""
        df = self.all_records.copy()
        
        # Tier filter
        tier = self.tier_var.get()
        if tier != "All":
            df = df[df["tier"] == tier]
        
        # Sector filter
        sector = self.sector_var.get()
        if sector != "All":
            df = df[df["sector_primary"] == sector]
        
        # County filter
        county = self.county_var.get()
        if county != "All":
            df = df[df["county"] == county]
        
        # Fuel type filter
        fuel = self.fuel_var.get()
        if fuel == "Diesel-like":
            df = df[df["is_diesel_like"] == True]
        elif fuel == "Non-diesel":
            df = df[df["is_diesel_like"] == False]
        
        # Capacity filter
        capacity = self.capacity_var.get()
        if capacity == "Large (10k+)":
            df = df[df["capacity_bucket"].isin(["Large", "Very Large"])]
        elif capacity == "Medium (5k-10k)":
            df = df[df["capacity_bucket"] == "Medium"]
        elif capacity == "Small (<5k)":
            df = df[df["capacity_bucket"] == "Small"]
        
        # Search filter
        search = self.search_var.get().lower()
        if search:
            mask = (
                df["facility_name"].str.lower().str.contains(search, na=False) |
                df["address"].str.lower().str.contains(search, na=False) |
                df["city"].str.lower().str.contains(search, na=False)
            )
            df = df[mask]
        
        # Limit filter
        limit_str = self.limit_var.get().strip()
        if limit_str:
            try:
                limit = int(limit_str)
                if limit > 0:
                    df = df.head(limit)
            except ValueError:
                pass  # Invalid limit, ignore
        
        self.filtered_records = df
        self.refresh_table()
        
    def refresh_table(self):
        """Refresh the table with filtered records."""
        # Clear existing items
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        # Add records
        for _, row in self.filtered_records.iterrows():
            facility_id = str(row["facility_id"])
            is_selected = facility_id in self.selected_ids
            
            # Format values
            fuel_type = "Diesel-like" if row.get("is_diesel_like") else "Non-diesel"
            capacity = row.get("capacity_bucket", "Unknown")
            address = f"{row.get('address', '')}, {row.get('city', '')}, {row.get('state', '')} {row.get('zip', '')}".strip(", ")
            
            # Insert row
            item_id = self.tree.insert(
                "",
                tk.END,
                text="☑" if is_selected else "☐",
                values=(
                    row.get("facility_name", ""),
                    row.get("county", ""),
                    row.get("tier", ""),
                    f"{row.get('score', 0):.0f}" if pd.notna(row.get("score")) else "",
                    row.get("sector_primary", "Unknown"),
                    fuel_type,
                    capacity,
                    address[:50] + "..." if len(address) > 50 else address
                ),
                tags=(facility_id,)
            )
            
            # Store facility_id in item
            self.tree.set(item_id, "facility_id", facility_id)
        
        # Update count
        selected_count = len([id for id in self.selected_ids if id in self.filtered_records["facility_id"].astype(str).values])
        total_count = len(self.filtered_records)
        self.count_label.config(text=f"{selected_count} of {total_count} records selected")
        
    def on_tree_click(self, event):
        """Handle click on tree (for checkbox toggle)."""
        region = self.tree.identify_region(event.x, event.y)
        if region == "cell":
            item = self.tree.identify_row(event.x, event.y)
            column = self.tree.identify_column(event.x, event.y)
            
            # Only toggle if clicking on checkbox column (#0)
            if column == "#0" and item:
                facility_id = self.tree.set(item, "facility_id")
                if facility_id:
                    if facility_id in self.selected_ids:
                        self.selected_ids.remove(facility_id)
                        self.tree.item(item, text="☐")
                    else:
                        self.selected_ids.add(facility_id)
                        self.tree.item(item, text="☑")
                    
                    self.refresh_table()
    
    def select_all(self):
        """Select all visible records."""
        for facility_id in self.filtered_records["facility_id"].astype(str):
            self.selected_ids.add(facility_id)
        self.refresh_table()
    
    def deselect_all(self):
        """Deselect all visible records."""
        filtered_ids = set(self.filtered_records["facility_id"].astype(str))
        self.selected_ids -= filtered_ids
        self.refresh_table()
    
    def select_tier_a(self):
        """Select only Tier A records."""
        tier_a_ids = set(self.filtered_records[self.filtered_records["tier"] == "Tier A"]["facility_id"].astype(str))
        self.selected_ids.update(tier_a_ids)
        self.refresh_table()
    
    def select_top_10(self):
        """Select top 10 records by score."""
        top_10_ids = set(self.filtered_records.head(10)["facility_id"].astype(str))
        self.selected_ids.update(top_10_ids)
        self.refresh_table()
    
    def select_top_50(self):
        """Select top 50 records by score."""
        top_50_ids = set(self.filtered_records.head(50)["facility_id"].astype(str))
        self.selected_ids.update(top_50_ids)
        self.refresh_table()
    
    def clear_filters(self):
        """Clear all filters."""
        self.tier_var.set("All")
        self.sector_var.set("All")
        self.county_var.set("All")
        self.fuel_var.set("All")
        self.capacity_var.set("All")
        self.search_var.set("")
        self.limit_var.set("")
        self.apply_filters()
    
    def sync_selected(self):
        """Return selected IDs and close dialog."""
        if not self.selected_ids:
            messagebox.showwarning("No Selection", "Please select at least one record to sync.")
            return
        
        self.result = list(self.selected_ids)
        self.dialog.destroy()
    
    def cancel(self):
        """Cancel and close dialog."""
        self.result = None
        self.dialog.destroy()
    
    def show(self) -> Optional[List[str]]:
        """Show dialog and return selected IDs."""
        self.dialog.wait_window()
        return self.result

