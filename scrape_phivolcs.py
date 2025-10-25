import requests
import pandas as pd
from datetime import datetime
import urllib3
from io import StringIO
import time
import os

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

MONTH_NAMES = ["January", "February", "March", "April", "May", "June",
               "July", "August", "September", "October", "November", "December"]

def scrape_current_month_from_main_page():
    """
    Scrapes the latest earthquake data from the main PHIVOLCS page.
    This is used for the current month that doesn't have a dedicated monthly page yet.
    """
    url = "https://earthquake.phivolcs.dost.gov.ph/"
    
    try:
        print(f"  Fetching from main page (current month)...", end=" ")
        
        session = requests.Session()
        session.verify = False
        
        response = session.get(url, timeout=15)
        response.raise_for_status()
        
        # Parse HTML tables
        tables = pd.read_html(StringIO(response.text), skiprows=1)
        
        # Find the earthquake data table
        df = None
        for table in tables:
            if table.shape[1] >= 5:
                df = table
                break
        
        if df is None or df.empty:
            print(f"✗ No data found")
            return None
        
        # Set column names
        expected_columns = [
            'Date-Time',
            'Latitude',
            'Longitude',
            'Depth',
            'Magnitude',
            'Location'
        ]
        
        if df.shape[1] == 6:
            df.columns = expected_columns
        elif df.shape[1] > 6:
            df = df.iloc[:, :6]
            df.columns = expected_columns
        else:
            print(f"✗ Invalid columns ({df.shape[1]})")
            return None
        
        # Remove header rows
        mask = (
            df['Date-Time'].astype(str).str.contains('Date|Time|Philippine', case=False, na=False) |
            df['Latitude'].astype(str).str.contains('Latitude|ºN|°N', case=False, na=False) |
            df['Longitude'].astype(str).str.contains('Longitude|ºE|°E', case=False, na=False)
        )
        df = df[~mask].reset_index(drop=True)
        
        # Remove summary and month abbreviation rows
        if not df.empty:
            first_col = df.iloc[:, 0].astype(str).str.strip()
            summary_mask = first_col.str.lower().str.contains('total|no. of events', na=False, regex=True)
            month_abbrev_mask = first_col.str.match(r'^[A-Z][a-z]{2}-\d{2}$', na=False)
            df = df[~(summary_mask | month_abbrev_mask)]
        
        # Remove empty rows
        df = df.dropna(how='all').reset_index(drop=True)
        
        # Determine the current month from the data
        current_month = datetime.now().strftime("%B")
        current_year = datetime.now().year
        
        # Add metadata columns
        df['Month'] = current_month
        df['Year'] = current_year
        
        print(f"✓ {len(df)} records")
        
        return df
        
    except Exception as e:
        print(f"✗ Error: {e}")
        return None


def scrape_phivolcs_data_from_html(year, month_name):
    """
    Fetches earthquake data by reading the HTML table from the PHIVOLCS monthly page.
    If the monthly page returns 404, it will try scraping from the main page.
    """
    url = (
        f"https://earthquake.phivolcs.dost.gov.ph/EQLatest-Monthly/"
        f"{year}/{year}_{month_name}.html"
    )
    
    try:
        print(f"  Fetching: {month_name} {year}...", end=" ")
        
        session = requests.Session()
        session.verify = False 
        
        response = session.get(url, timeout=15)
        response.raise_for_status()
        
        # Parse HTML tables
        tables = pd.read_html(StringIO(response.text), skiprows=1)
        
        # Find the table with earthquake data
        df = None
        for table in tables:
            if table.shape[1] >= 5:
                df = table
                break
        
        if df is None or df.empty:
            print(f"✗ No data")
            return None
        
        # Set column names
        expected_columns = [
            'Date-Time',
            'Latitude',
            'Longitude',
            'Depth',
            'Magnitude',
            'Location'
        ]
        
        if df.shape[1] == 6:
            df.columns = expected_columns
        elif df.shape[1] > 6:
            df = df.iloc[:, :6]
            df.columns = expected_columns
        else:
            print(f"✗ Invalid columns ({df.shape[1]})")
            return None
        
        # Remove header rows
        mask = (
            df['Date-Time'].astype(str).str.contains('Date|Time|Philippine', case=False, na=False) |
            df['Latitude'].astype(str).str.contains('Latitude|ºN|°N', case=False, na=False) |
            df['Longitude'].astype(str).str.contains('Longitude|ºE|°E', case=False, na=False)
        )
        df = df[~mask].reset_index(drop=True)
        
        # Remove summary and month abbreviation rows
        if not df.empty:
            first_col = df.iloc[:, 0].astype(str).str.strip()
            summary_mask = first_col.str.lower().str.contains('total|no. of events', na=False, regex=True)
            month_abbrev_mask = first_col.str.match(r'^[A-Z][a-z]{2}-\d{2}$', na=False)
            df = df[~(summary_mask | month_abbrev_mask)]
        
        # Remove empty rows
        df = df.dropna(how='all').reset_index(drop=True)
        
        # Add metadata columns
        df['Month'] = month_name
        df['Year'] = year
        
        print(f"✓ {len(df)} records")
        
        return df
        
    except requests.exceptions.HTTPError as errh:
        # If 404, this month might be the current month - try main page
        if errh.response.status_code == 404:
            print(f"✗ HTTP 404 (trying main page)")
            return scrape_current_month_from_main_page()
        else:
            print(f"✗ HTTP {errh.response.status_code}")
            return None
    except Exception as e:
        print(f"✗ Error: {e}")
        return None


def scrape_year_data(year, output_dir="data"):
    """
    Scrapes earthquake data for all months in a given year.
    Cleans, standardizes, saves the combined DataFrame, and reports on removed rows.
    """
    print(f"\n{'─'*70}")
    print(f"📅 Scraping Year: {year}")
    print(f"{'─'*70}")
    
    all_data = []
    successful_months = []
    failed_months = []
    current_month_found = False
    
    for month_name in MONTH_NAMES:
        # Skip future months if we've already found the current month
        if current_month_found:
            print(f"  Skipping: {month_name} {year} (future month)")
            failed_months.append(month_name)
            continue
            
        df = scrape_phivolcs_data_from_html(year, month_name)
        
        if df is not None and not df.empty:
            all_data.append(df)
            successful_months.append(month_name)
            
            # Check if this data came from the main page (current month indicator)
            if year == datetime.now().year and month_name == datetime.now().strftime("%B"):
                current_month_found = True
                print(f"  ℹ️  Current month detected: {month_name} {year}")
        else:
            failed_months.append(month_name)
            # If we get a failure on the current year, it might be the current month
            if year == datetime.now().year and not current_month_found:
                current_month_found = True
        
        # Be polite to the server
        time.sleep(0.5)
    
    # Combine and save data for this year
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # --- START DATA CLEANING & STANDARDIZATION ---
        print(f"  Cleaning and standardizing {len(combined_df)} total records for {year}...")
        
        # 1. Standardize column names (all lowercase)
        combined_df.columns = [str(col).lower() for col in combined_df.columns]
        
        # 2. Define columns to clean
        numeric_cols = ['latitude', 'longitude', 'depth', 'magnitude']
        
        # 3. Convert to numeric, turning errors (e.g., '---') into NaN
        for col in numeric_cols:
            combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce')
            
        # 4. Find rows to be removed due to NaN (missing lat, long, depth, or mag)
        nan_mask = combined_df[numeric_cols].isnull().any(axis=1)
        removed_nan_rows = combined_df[nan_mask]
        
        # Now, drop them
        combined_df = combined_df.dropna(subset=numeric_cols)
        
        # 5. Find rows to be removed for 'Null Island' (0,0)
        null_island_mask = ((combined_df['latitude'] == 0) & (combined_df['longitude'] == 0))
        removed_null_island_rows = combined_df[~nan_mask & null_island_mask] # Only check rows that weren't already marked for NaN removal
        
        # Now, filter them out
        combined_df = combined_df[~null_island_mask]
        
        # --- START: Report removed rows ---
        all_removed_rows = pd.concat([removed_nan_rows, removed_null_island_rows])
        removed_count = len(all_removed_rows)

        if removed_count > 0:
            print(f"  • Removed {removed_count} invalid/corrupt rows for {year}:")
            # Print the removed rows in a tidy format
            with pd.option_context('display.max_rows', None, 
                                   'display.max_columns', None, 
                                   'display.width', 1000):
                # Show the original columns before they were renamed
                columns_to_show = ['date-time', 'latitude', 'longitude', 'depth', 'magnitude', 'location']
                existing_cols_to_show = [col for col in columns_to_show if col in all_removed_rows.columns]
                
                # Use .to_string() for clean console output
                print(all_removed_rows[existing_cols_to_show].to_string(index=False))
        # --- END: Report removed rows ---

        # 6. Rename columns to match the React app's interface
        combined_df = combined_df.rename(columns={
            'date-time': 'datetime',
            'depth': 'depth_km',
        })
        
        # 7. Create a unique ID
        combined_df['id'] = combined_df.apply(
            lambda row: f"{row['datetime']}-{row['latitude']:.4f}-{row['longitude']:.4f}-{row['magnitude']}",
            axis=1
        )
        
        # 8. Reorder columns
        final_columns = [
            'id', 'datetime', 'latitude', 'longitude', 
            'depth_km', 'magnitude', 'location', 'month', 'year'
        ]
        existing_final_columns = [col for col in final_columns if col in combined_df.columns]
        combined_df = combined_df[existing_final_columns]
        
        print(f"  • {len(combined_df)} valid records remaining for {year}.")
        # --- END DATA CLEANING & STANDARDIZATION ---
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Save to separate file for this year
        output_filename = os.path.join(output_dir, f"phivolcs_earthquake_{year}.csv")
        combined_df.to_csv(output_filename, index=False, encoding='utf-8-sig')
        
        print(f"\n✓ Year {year} Complete:")
        print(f"  • Total records: {len(combined_df)}")
        print(f"  • Successful months: {len(successful_months)}")
        print(f"  • File saved: {output_filename}")
        
        return combined_df
    else:
        print(f"\n✗ No data retrieved for {year}")
        return None


def scrape_multiple_years(years_back=3, output_dir="data"):
    """
    Scrapes earthquake data for the last N years.
    Each year is saved as a separate CSV.
    The final combined data is saved as a single JSON file.
    """
    current_year = datetime.now().year
    start_year = current_year - years_back + 1
    
    print(f"\n{'='*70}")
    print(f"🌏 PHIVOLCS EARTHQUAKE DATA SCRAPER")
    print(f"{'='*70}")
    print(f"📊 Scraping Range: {start_year} - {current_year}")
    print(f"📁 Output Directory: {output_dir}/")
    print(f"{'='*70}")
    
    all_years_data = []
    scrape_summary = {}
    
    # Scrape each year
    for year in range(start_year, current_year + 1):
        df = scrape_year_data(year, output_dir)
        
        if df is not None:
            all_years_data.append(df)
            scrape_summary[year] = len(df)
        else:
            scrape_summary[year] = 0
    
    # Create a combined JSON file with all years
    if all_years_data:
        combined_all = pd.concat(all_years_data, ignore_index=True)
        
        # --- SAVE AS JSON FOR THE REACT APP ---
        # Save as the JSON file the Deck.gl app is expecting
        json_filename = os.path.join(output_dir, f"earthquakes.json")
        combined_all.to_json(json_filename, orient='records', indent=4)
        
        # Also save the combined CSV as before
        combined_csv_filename = os.path.join(output_dir, f"phivolcs_earthquake_all_years.csv")
        combined_all.to_csv(combined_csv_filename, index=False, encoding='utf-8-sig')
        
        # Print final summary
        print(f"\n{'='*70}")
        print(f"✅ SCRAPING COMPLETE!")
        print(f"{'='*70}")
        print(f"\n📊 Summary by Year:")
        for year, count in scrape_summary.items():
            print(f"  • {year}: {count:,} earthquakes")
        print(f"\n📈 Total Records: {len(combined_all):,}")
        print(f"\n📁 Files Created:")
        for year in range(start_year, current_year + 1):
            if scrape_summary.get(year, 0) > 0:
                print(f"  • {output_dir}/phivolcs_earthquake_{year}.csv")
        print(f"  • {combined_csv_filename} (combined CSV)")
        print(f"  ✨ {json_filename} (combined JSON for app)")
        print(f"\n{'='*70}\n")
        
        return combined_all, scrape_summary
    else:
        print(f"\n✗ No data was retrieved for any year.")
        return None, {}


def display_statistics(df):
    """
    Display basic statistics about the scraped data.
    Uses the new standardized (lowercase) column names.
    """
    if df is None or df.empty:
        return
    
    print(f"{'='*70}")
    print(f"📈 DATA STATISTICS")
    print(f"{'='*70}\n")
    
    # Magnitude statistics
    print("🔢 Magnitude Statistics:")
    # Use new 'magnitude' column
    print(df['magnitude'].describe())
    
    # Yearly breakdown
    print(f"\n📅 Earthquakes by Year:")
    # Use new 'year' column
    yearly_counts = df.groupby('year').size().sort_index()
    for year, count in yearly_counts.items():
        print(f"  • {year}: {count:,} earthquakes")
    
    # Top 10 strongest earthquakes
    print(f"\n💥 Top 10 Strongest Earthquakes:")
    # Use new lowercase columns
    top_10 = df.nlargest(10, 'magnitude')[['datetime', 'magnitude', 'location', 'year']]
    for idx, row in top_10.iterrows():
        print(f"  • Mag {row['magnitude']} - {row['location'][:50]} ({row['year']})")
    
    print(f"\n{'='*70}\n")


if __name__ == "__main__":
    # Configuration
    YEARS_TO_SCRAPE = 8  # Last 3 years (including current year)
    OUTPUT_DIR = "data"
    
    # Run the scraper
    combined_df, summary = scrape_multiple_years(
        years_back=YEARS_TO_SCRAPE,
        output_dir=OUTPUT_DIR
    )
    
    # Display statistics
    if combined_df is not None:
        display_statistics(combined_df)
