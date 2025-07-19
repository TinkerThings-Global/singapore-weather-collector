#!/usr/bin/env python3
"""
Script to rebuild the progress tracker JSON with corrected gap analysis
"""

import json
from datetime import date, timedelta
from models import Config
from utils.gap_analyzer import DataGapAnalyzer
from utils.progress_tracker import ProgressTracker
from pathlib import Path

def rebuild_progress():
    """Rebuild progress tracker with corrected gap analysis"""
    config = Config()
    analyzer = DataGapAnalyzer(config)
    tracker = ProgressTracker(config.PROGRESS_FILE)
    
    print("🔄 Rebuilding progress tracker with corrected gap analysis...")
    
    # Get all CSV files in data directory
    csv_files = list(config.DATA_DIR.glob("*.csv"))
    
    if not csv_files:
        print("❌ No CSV files found in data directory")
        return
    
    # Extract unique dates and data types from files
    processed_dates = set()
    for csv_file in csv_files:
        # Parse filename: {data_type}_{station_id}_{date}.csv
        parts = csv_file.stem.split('_')
        if len(parts) >= 3:
            data_type = parts[0]
            date_str = parts[2]
            try:
                file_date = date.fromisoformat(date_str)
                processed_dates.add((file_date, data_type))
            except ValueError:
                print(f"⚠️  Skipping malformed filename: {csv_file.name}")
    
    print(f"📁 Found {len(csv_files)} CSV files covering {len(processed_dates)} date/type combinations")
    
    # Group by date
    dates_to_process = {}
    for file_date, data_type in processed_dates:
        if file_date not in dates_to_process:
            dates_to_process[file_date] = []
        dates_to_process[file_date].append(data_type)
    
    # Process each date
    updated_count = 0
    for target_date in sorted(dates_to_process.keys()):
        data_types = dates_to_process[target_date]
        
        for data_type in data_types:
            print(f"🔍 Analyzing {data_type} for {target_date}...")
            
            # Get corrected completeness summary
            summary = analyzer.get_completeness_summary(target_date)
            data_summary = summary["data_types"].get(data_type, {})
            
            if data_summary.get("file_exists", False):
                # Update progress tracker with corrected analysis
                file_path = config.DATA_DIR / f"{data_type}_{config.STATION_ID}_{target_date.isoformat()}.csv"
                
                if file_path.exists():
                    import polars as pl
                    try:
                        df = pl.read_csv(file_path)
                        gaps = analyzer.detect_gaps(df, target_date)
                        
                        # Convert gaps to string format for JSON storage
                        timestamp_gaps = []
                        for gap in gaps:
                            start_str = gap.start.strftime("%Y-%m-%dT%H:%M:%S")
                            end_str = gap.end.strftime("%Y-%m-%dT%H:%M:%S")
                            timestamp_gaps.append([start_str, end_str])
                        
                        status = "complete" if len(gaps) == 0 else "incomplete"
                        
                        tracker.update_state(
                            target_date,
                            data_type,
                            timestamp_gaps=timestamp_gaps if timestamp_gaps else None,
                            status=status,
                            total_records=df.height,
                            expected_records=1440,
                            last_updated="2025-07-19T21:30:00",  # Current rebuild time
                            last_error=None
                        )
                        
                        updated_count += 1
                        print(f"✅ Updated {data_type} {target_date}: {status} ({df.height} records, {len(gaps)} gaps)")
                        
                    except Exception as e:
                        print(f"❌ Error processing {file_path}: {e}")
    
    print(f"\n🎉 Rebuilt progress for {updated_count} date/type combinations")
    print("📊 Progress tracker JSON has been updated with corrected gap analysis")

if __name__ == "__main__":
    rebuild_progress()