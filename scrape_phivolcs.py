import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
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
    """
    url = "https://earthquake.phivolcs.dost.gov.ph/"
    
    try:
        print(f"  Fetching from main page (current month)...", end=" ")
        
        session = requests.Session()
        session.verify = False
        
        response = session.get(url, timeout=15)
        response.raise_for_status()
        
        tables = pd.read_html(StringIO(response.text), skiprows=1)
        
        df = None
        for table in tables:
            if table.shape[1] >= 5:
                df = table
                break
        
        if df is None or df.empty:
            print(f"✗ No data found")
            return None
        
        expected_columns = ['Date-Time', 'Latitude', 'Longitude', 'Depth', 'Magnitude', 'Location']
        
        if df.shape[1] == 6:
            df.columns = expected_columns
        elif df.shape[1] > 6:
            df = df.iloc[:, :6]
            df.columns = expected_columns
        else:
            print(f"✗ Invalid columns ({df.shape[1]})")
            return None
        
        mask = (
            df['Date-Time'].astype(str).str.contains('Date|Time|Philippine', case=False, na=False) |
            df['Latitude'].astype(str).str.contains('Latitude|ºN|°N', case=False, na=False) |
            df['Longitude'].astype(str).str.contains('Longitude|ºE|°E', case=False, na=False)
        )
        df = df[~mask].reset_index(drop=True)
        
        if not df.empty:
            first_col = df.iloc[:, 0].astype(str).str.strip()
            summary_mask = first_col.str.lower().str.contains('total|no. of events', na=False, regex=True)
            month_abbrev_mask = first_col.str.match(r'^[A-Z][a-z]{2}-\d{2}$', na=False)
            df = df[~(summary_mask | month_abbrev_mask)]
        
        df = df.dropna(how='all').reset_index(drop=True)
        
        current_month = datetime.now().strftime("%B")
        current_year = datetime.now().year
        
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
        
        tables = pd.read_html(StringIO(response.text), skiprows=1)
        
        df = None
        for table in tables:
            if table.shape[1] >= 5:
                df = table
                break
        
        if df is None or df.empty:
            print(f"✗ No data")
            return None
        
        expected_columns = ['Date-Time', 'Latitude', 'Longitude', 'Depth', 'Magnitude', 'Location']
        
        if df.shape[1] == 6:
            df.columns = expected_columns
        elif df.shape[1] > 6:
            df = df.iloc[:, :6]
            df.columns = expected_columns
        else:
            print(f"✗ Invalid columns ({df.shape[1]})")
            return None
        
        mask = (
            df['Date-Time'].astype(str).str.contains('Date|Time|Philippine', case=False, na=False) |
            df['Latitude'].astype(str).str.contains('Latitude|ºN|°N', case=False, na=False) |
            df['Longitude'].astype(str).str.contains('Longitude|ºE|°E', case=False, na=False)
        )
        df = df[~mask].reset_index(drop=True)
        
        if not df.empty:
            first_col = df.iloc[:, 0].astype(str).str.strip()
            summary_mask = first_col.str.lower().str.contains('total|no. of events', na=False, regex=True)
            month_abbrev_mask = first_col.str.match(r'^[A-Z][a-z]{2}-\d{2}$', na=False)
            df = df[~(summary_mask | month_abbrev_mask)]
        
        df = df.dropna(how='all').reset_index(drop=True)
        
        df['Month'] = month_name
        df['Year'] = year
        
        print(f"✓ {len(df)} records")
        return df
        
    except requests.exceptions.HTTPError as errh:
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
    """
    print(f"\n{'─'*70}")
    print(f"📅 Scraping Year: {year}")
    print(f"{'─'*70}")
    
    all_data = []
    successful_months = []
    failed_months = []
    current_month_found = False
    
    for month_name in MONTH_NAMES:
        if current_month_found:
            print(f"  Skipping: {month_name} {year} (future month)")
            failed_months.append(month_name)
            continue
            
        df = scrape_phivolcs_data_from_html(year, month_name)
        
        if df is not None and not df.empty:
            all_data.append(df)
            successful_months.append(month_name)
            
            if year == datetime.now().year and month_name == datetime.now().strftime("%B"):
                current_month_found = True
                print(f"  ℹ️  Current month detected: {month_name} {year}")
        else:
            failed_months.append(month_name)
            if year == datetime.now().year and not current_month_found:
                current_month_found = True
        
        time.sleep(0.5)
    
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        
        print(f"  Cleaning and standardizing {len(combined_df)} total records for {year}...")
        
        combined_df.columns = [str(col).lower() for col in combined_df.columns]
        
        if 'location' in combined_df.columns:
            combined_df['location'] = combined_df['location'].astype(str).str.replace('Â', '')
        
        numeric_cols = ['latitude', 'longitude', 'depth', 'magnitude']
        
        for col in numeric_cols:
            combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce')
            
        nan_mask = combined_df[numeric_cols].isnull().any(axis=1)
        removed_nan_rows = combined_df[nan_mask]
        combined_df = combined_df.dropna(subset=numeric_cols)
        
        null_island_mask = ((combined_df['latitude'] == 0) & (combined_df['longitude'] == 0))
        removed_null_island_rows = combined_df[~nan_mask & null_island_mask]
        combined_df = combined_df[~null_island_mask]
        
        all_removed_rows = pd.concat([removed_nan_rows, removed_null_island_rows])
        removed_count = len(all_removed_rows)

        if removed_count > 0:
            print(f"  • Removed {removed_count} invalid/corrupt rows for {year}")

        combined_df = combined_df.rename(columns={
            'date-time': 'datetime',
            'depth': 'depth_km',
        })
        
        combined_df['id'] = combined_df.apply(
            lambda row: f"{row['datetime']}-{row['latitude']:.4f}-{row['longitude']:.4f}-{row['magnitude']}",
            axis=1
        )
        
        final_columns = [
            'id', 'datetime', 'latitude', 'longitude', 
            'depth_km', 'magnitude', 'location', 'month', 'year'
        ]
        existing_final_columns = [col for col in final_columns if col in combined_df.columns]
        combined_df = combined_df[existing_final_columns]
        
        print(f"  • {len(combined_df)} valid records remaining for {year}.")
        
        os.makedirs(output_dir, exist_ok=True)
        output_filename = os.path.join(output_dir, f"phivolcs_earthquake_{year}.csv")
        combined_df.to_csv(output_filename, index=False, encoding='utf-8-sig')
        
        print(f"\n✓ Year {year} Complete.")
        return combined_df
    else:
        print(f"\n✗ No data retrieved for {year}")
        return None

def scrape_multiple_years(years_back=3, output_dir="data"):
    """
    Scrapes earthquake data for the last N years.
    Saves CSV, JSON, and FlatGeobuf.
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
    
    for year in range(start_year, current_year + 1):
        df = scrape_year_data(year, output_dir)
        if df is not None:
            all_years_data.append(df)
            scrape_summary[year] = len(df)
        else:
            scrape_summary[year] = 0
    
    if all_years_data:
        combined_all = pd.concat(all_years_data, ignore_index=True)
        
        # 1. Save JSON
        json_filename = os.path.join(output_dir, f"earthquakes.json")
        combined_all.to_json(json_filename, orient='records', indent=4)
        
        # 2. Save CSV
        combined_csv_filename = os.path.join(output_dir, f"phivolcs_earthquake_all_years.csv")
        combined_all.to_csv(combined_csv_filename, index=False, encoding='utf-8-sig')

        # 3. Save FLATGEOBUF
        print(f"\n🌎 Generating FlatGeobuf (3D)...")
        try:
            # Create 3D Geometry (Lon, Lat, Depth)
            geometry = [
                Point(xyz) for xyz in zip(
                    combined_all.longitude, 
                    combined_all.latitude, 
                    combined_all.depth_km
                )
            ]
            
            # Convert to GeoDataFrame
            gdf = gpd.GeoDataFrame(combined_all, geometry=geometry, crs="EPSG:4326")
            
            # Save .fgb file
            fgb_filename = os.path.join(output_dir, "earthquakes.fgb")
            gdf.to_file(fgb_filename, driver="FlatGeobuf")
            
            fgb_size = os.path.getsize(fgb_filename) / (1024 * 1024)
            print(f"  ✓ Saved {fgb_filename} ({fgb_size:.2f} MB)")
            
        except Exception as e:
            print(f"  ✗ Failed to save FlatGeobuf: {e}")
        # ----------------------------

        print(f"\n{'='*70}")
        print(f"✅ SCRAPING COMPLETE!")
        print(f"{'='*70}")
        print(f"📈 Total Records: {len(combined_all):,}")
        print(f"✨ Files Created: JSON, CSV, and FGB")
        
        return combined_all, scrape_summary
    else:
        print(f"\n✗ No data was retrieved for any year.")
        return None, {}

def display_statistics(df):
    if df is None or df.empty:
        return
    print(f"{'='*70}")
    print(f"📈 DATA STATISTICS")
    print(f"{'='*70}")
    print("🔢 Magnitude Statistics:")
    print(df['magnitude'].describe())
    print(f"\n📅 Earthquakes by Year:")
    yearly_counts = df.groupby('year').size().sort_index()
    for year, count in yearly_counts.items():
        print(f"  • {year}: {count:,} earthquakes")

if __name__ == "__main__":
    YEARS_TO_SCRAPE = 8 
    OUTPUT_DIR = "data"
    combined_df, summary = scrape_multiple_years(years_back=YEARS_TO_SCRAPE, output_dir=OUTPUT_DIR)
    if combined_df is not None:
        display_statistics(combined_df)