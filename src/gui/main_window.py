"""Main GUI window for Foxfuel Lead Generation System."""
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext
import threading
import subprocess
import sys
from pathlib import Path
from datetime import datetime
import os

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.config import settings


class FoxfuelGUI:
    """Main GUI application for Foxfuel Lead Generation System."""
    
    def __init__(self, root):
        self.root = root
        self.root.title("Foxfuel Lead Generation System")
        self.root.geometry("900x700")
        self.root.resizable(True, True)
        
        # Variables
        self.pa_tanks_path = tk.StringVar(value="")
        self.naics_path = tk.StringVar(value=str(settings.naics_local_path))
        self.maps_extractor_dir = tk.StringVar(value="./data/maps_extractor")
        self.counties_var = tk.StringVar(value="Bucks,Montgomery,Philadelphia,Chester,Delaware")
        self.skip_geocode = tk.BooleanVar(value=True)
        self.generate_qa = tk.BooleanVar(value=True)
        
        self.setup_ui()
        
    def setup_ui(self):
        """Setup the user interface."""
        # Main container
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        
        # Title
        title_label = ttk.Label(
            main_frame,
            text="Foxfuel Lead Generation System",
            font=("Arial", 16, "bold")
        )
        title_label.grid(row=0, column=0, columnspan=3, pady=(0, 20))
        
        # File Selection Section
        file_frame = ttk.LabelFrame(main_frame, text="File Selection", padding="10")
        file_frame.grid(row=1, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=5)
        file_frame.columnconfigure(1, weight=1)
        
        # PA Tanks File
        ttk.Label(file_frame, text="PA Storage Tanks File:").grid(row=0, column=0, sticky=tk.W, pady=5)
        ttk.Entry(file_frame, textvariable=self.pa_tanks_path, width=50).grid(row=0, column=1, sticky=(tk.W, tk.E), padx=5)
        ttk.Button(file_frame, text="Browse...", command=self.browse_pa_tanks).grid(row=0, column=2, padx=5)
        
        # NAICS File
        ttk.Label(file_frame, text="NAICS Data File:").grid(row=1, column=0, sticky=tk.W, pady=5)
        ttk.Entry(file_frame, textvariable=self.naics_path, width=50).grid(row=1, column=1, sticky=(tk.W, tk.E), padx=5)
        ttk.Button(file_frame, text="Browse...", command=self.browse_naics).grid(row=1, column=2, padx=5)
        
        # Maps Extractor Directory
        ttk.Label(file_frame, text="Maps Extractor Folder:").grid(row=2, column=0, sticky=tk.W, pady=5)
        ttk.Entry(file_frame, textvariable=self.maps_extractor_dir, width=50).grid(row=2, column=1, sticky=(tk.W, tk.E), padx=5)
        ttk.Button(file_frame, text="Browse...", command=self.browse_maps_dir).grid(row=2, column=2, padx=5)
        
        # Options Section
        options_frame = ttk.LabelFrame(main_frame, text="Options", padding="10")
        options_frame.grid(row=2, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=5)
        
        ttk.Label(options_frame, text="Counties:").grid(row=0, column=0, sticky=tk.W, pady=5)
        ttk.Entry(options_frame, textvariable=self.counties_var, width=60).grid(row=0, column=1, sticky=(tk.W, tk.E), padx=5)
        
        ttk.Checkbutton(
            options_frame,
            text="Skip Geocoding (faster, use if Maps Extractor provides coordinates)",
            variable=self.skip_geocode
        ).grid(row=1, column=0, columnspan=2, sticky=tk.W, pady=5)
        
        ttk.Checkbutton(
            options_frame,
            text="Generate QA Report",
            variable=self.generate_qa
        ).grid(row=2, column=0, columnspan=2, sticky=tk.W, pady=5)
        
        # Actions Section
        actions_frame = ttk.LabelFrame(main_frame, text="Actions", padding="10")
        actions_frame.grid(row=3, column=0, columnspan=3, sticky=(tk.W, tk.E), pady=5)
        
        ttk.Button(
            actions_frame,
            text="1. Build Universe (Import & Process Data)",
            command=self.run_build_universe,
            width=40
        ).grid(row=0, column=0, pady=5, padx=5)
        
        ttk.Button(
            actions_frame,
            text="2. Rescore Leads",
            command=self.run_rescore,
            width=40
        ).grid(row=0, column=1, pady=5, padx=5)
        
        ttk.Button(
            actions_frame,
            text="3. Export for Power BI",
            command=self.run_export,
            width=40
        ).grid(row=1, column=0, pady=5, padx=5)
        
        ttk.Button(
            actions_frame,
            text="4. Sync to Bigin CRM (Dry Run)",
            command=self.run_crm_dry_run,
            width=40
        ).grid(row=1, column=1, pady=5, padx=5)
        
        ttk.Button(
            actions_frame,
            text="5. Sync to Bigin CRM (Live)",
            command=self.run_crm_sync,
            width=40
        ).grid(row=2, column=0, pady=5, padx=5)
        
        ttk.Button(
            actions_frame,
            text="Rename Maps Files",
            command=self.rename_maps_files,
            width=40
        ).grid(row=2, column=1, pady=5, padx=5)
        
        # Status/Log Section
        log_frame = ttk.LabelFrame(main_frame, text="Status & Log", padding="10")
        log_frame.grid(row=4, column=0, columnspan=3, sticky=(tk.W, tk.E, tk.N, tk.S), pady=5)
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)
        main_frame.rowconfigure(4, weight=1)
        
        self.log_text = scrolledtext.ScrolledText(log_frame, height=15, width=80)
        self.log_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Clear log button
        ttk.Button(log_frame, text="Clear Log", command=self.clear_log).grid(row=1, column=0, pady=5)
        
        # Initial message
        self.log("Welcome to Foxfuel Lead Generation System!")
        self.log("Please select your input files and click 'Build Universe' to get started.")
        self.log("")
        
    def log(self, message):
        """Add message to log."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.insert(tk.END, f"[{timestamp}] {message}\n")
        self.log_text.see(tk.END)
        self.root.update_idletasks()
        
    def clear_log(self):
        """Clear the log."""
        self.log_text.delete(1.0, tk.END)
        
    def browse_pa_tanks(self):
        """Browse for PA tanks file."""
        filename = filedialog.askopenfilename(
            title="Select PA Storage Tanks File",
            filetypes=[("Excel files", "*.xlsx *.xls"), ("CSV files", "*.csv"), ("All files", "*.*")]
        )
        if filename:
            self.pa_tanks_path.set(filename)
            self.log(f"Selected PA Tanks file: {Path(filename).name}")
    
    def browse_naics(self):
        """Browse for NAICS file."""
        filename = filedialog.askopenfilename(
            title="Select NAICS Data File",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )
        if filename:
            self.naics_path.set(filename)
            self.log(f"Selected NAICS file: {Path(filename).name}")
    
    def browse_maps_dir(self):
        """Browse for Maps Extractor directory."""
        dirname = filedialog.askdirectory(
            title="Select Maps Extractor Folder",
            initialdir=self.maps_extractor_dir.get()
        )
        if dirname:
            self.maps_extractor_dir.set(dirname)
            self.log(f"Selected Maps Extractor folder: {dirname}")
    
    def run_command(self, cmd_args, description):
        """Run a command in a separate thread."""
        def run():
            self.log(f"Starting: {description}")
            self.log("")
            
            try:
                # Run the command
                process = subprocess.Popen(
                    [sys.executable, "-m"] + cmd_args,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    universal_newlines=True
                )
                
                # Stream output to log
                for line in process.stdout:
                    if line.strip():
                        self.log(line.strip())
                
                process.wait()
                
                if process.returncode == 0:
                    self.log("")
                    self.log(f"✓ {description} completed successfully!")
                    messagebox.showinfo("Success", f"{description} completed successfully!")
                else:
                    self.log("")
                    self.log(f"✗ {description} failed with exit code {process.returncode}")
                    messagebox.showerror("Error", f"{description} failed. Check the log for details.")
                    
            except Exception as e:
                self.log(f"Error: {str(e)}")
                messagebox.showerror("Error", f"Failed to run {description}:\n{str(e)}")
        
        thread = threading.Thread(target=run, daemon=True)
        thread.start()
    
    def run_build_universe(self):
        """Run build universe job."""
        if not self.pa_tanks_path.get():
            messagebox.showwarning("Missing File", "Please select a PA Storage Tanks file.")
            return
        
        if not Path(self.pa_tanks_path.get()).exists():
            messagebox.showerror("File Not Found", f"File not found: {self.pa_tanks_path.get()}")
            return
        
        cmd = ["src.jobs.build_universe", "--pa-tanks-path", self.pa_tanks_path.get()]
        
        if self.naics_path.get() and Path(self.naics_path.get()).exists():
            cmd.extend(["--naics-local-path", self.naics_path.get()])
        
        if self.maps_extractor_dir.get():
            maps_glob = str(Path(self.maps_extractor_dir.get()) / "*.csv")
            cmd.extend(["--maps-extractor-glob", maps_glob])
        
        if self.counties_var.get():
            cmd.extend(["--counties", self.counties_var.get()])
        
        if self.skip_geocode.get():
            cmd.append("--skip-geocode")
        
        if self.generate_qa.get():
            cmd.append("--qa")
        
        # Skip other sources for simplicity
        cmd.extend([
            "--skip-fmcsa", "--skip-echo", "--skip-eia",
            "--skip-osm", "--skip-procurement", "--skip-permits"
        ])
        
        self.run_command(cmd, "Build Universe")
    
    def run_rescore(self):
        """Run rescore daily job."""
        self.run_command(["src.jobs.rescore_daily"], "Rescore Leads")
    
    def run_export(self):
        """Run Power BI export."""
        self.run_command(["src.dashboards.export_powerbi"], "Export for Power BI")
    
    def run_crm_dry_run(self):
        """Run CRM sync dry run."""
        self.run_command(["src.jobs.push_to_bigin", "--dry-run", "--limit", "5"], "CRM Sync (Dry Run)")
    
    def run_crm_sync(self):
        """Run live CRM sync with review dialog."""
        from src.gui.sync_review import SyncReviewDialog
        
        # Show review dialog
        review_dialog = SyncReviewDialog(self.root)
        selected_ids = review_dialog.show()
        
        if not selected_ids:
            return
        
        if not messagebox.askyesno(
            "Confirm Sync",
            f"This will sync {len(selected_ids)} selected records to Bigin CRM.\n\nContinue?"
        ):
            return
        
        # Build command with selected IDs
        # We'll need to modify push_to_bigin to accept entity IDs
        ids_str = ",".join(selected_ids)
        self.run_command(
            ["src.jobs.push_to_bigin", "--entity-ids", ids_str],
            f"CRM Sync (Live) - {len(selected_ids)} records"
        )
    
    def rename_maps_files(self):
        """Rename maps extractor files."""
        maps_dir = self.maps_extractor_dir.get() or "./data/maps_extractor"
        self.run_command(
            ["src.jobs.rename_maps_files", "--directory", maps_dir],
            "Rename Maps Files"
        )


def main():
    """Main entry point for GUI."""
    root = tk.Tk()
    app = FoxfuelGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()

