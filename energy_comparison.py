#!/usr/bin/env python3
"""
Energy Tariff Comparison Script with Hourly Dynamic Pricing

This script compares fixed-price and dynamic energy tariffs using hourly data from Home Assistant.
It matches hourly energy consumption with ENTSO-E day-ahead prices for accurate dynamic tariff calculation.

Requirements:
- pip install entsoe-py pandas pytz
- Home Assistant database with cumulative energy entities

Configuration:
Set the variables below to match your setup.
"""

import sqlite3
import datetime
from datetime import timedelta
import pandas as pd
import pytz
from entsoe import EntsoePandasClient

# ==================== USER-DEFINED VARIABLES ====================
DB_PATH = "/path/to/your/home-assistant_v2.db"
GRID_IMPORT_ENTITY = "sensor.your_import_entity"
GRID_EXPORT_ENTITY = "sensor.your_export_entity"

# Fixed tariff settings
FIXED_IMPORT_PRICE = 0.2500  # Fixed import price per kWh
FIXED_EXPORT_PRICE = 0.1000  # Fixed export price per kWh
BASE_FIXED_MONTHLY = 15.00   # Monthly base fee for fixed tariff

# Dynamic tariff settings
BASE_DYNAMIC_MONTHLY = 17.01  # Monthly base fee for dynamic tariff
BASE_DYNAMIC_KWH = 0.1492     # Base markup per kWh (added to ENTSO-E price)
ENTSOE_API_KEY = "your_entsoe_api_key_here"  # API key from ENTSO-E
ENTSOE_COUNTRY_CODE = "DE_LU"  # Country code (NL, DE, DK, etc.)
TIMEZONE = "Europe/Amsterdam"  # Your local timezone

# Period (exactly 365 days)
now_utc = datetime.datetime.now(pytz.UTC)
start_period = now_utc - timedelta(days=365)
end_period = now_utc
num_months = 12
# ================================================================

print(f"Energy Tariff Comparison - Using Long-Term Statistics")
print(f"Period: {start_period.strftime('%Y-%m-%d %H:%M')} to {end_period.strftime('%Y-%m-%d %H:%M')} (UTC)")
print("=" * 70)

def get_statistics_data(db_conn, entity_id, start_dt, end_dt):
    """
    Extract hourly energy data from Home Assistant long-term statistics.
    """
    cursor = db_conn.cursor()
    
    start_ts = start_dt.timestamp()
    end_ts = end_dt.timestamp()
    
    # Query to get statistics data
    query = """
        SELECT s.start_ts, s.sum, s.state
        FROM statistics s
        JOIN statistics_meta sm ON s.metadata_id = sm.id
        WHERE sm.statistic_id = ?
        AND s.start_ts >= ?
        AND s.start_ts <= ?
        ORDER BY s.start_ts ASC
    """
    
    cursor.execute(query, (entity_id, start_ts, end_ts))
    results = cursor.fetchall()
    
    if not results:
        # Try alternative query for older HA versions
        query_old = """
            SELECT s.created_ts as start_ts, s.sum, s.state
            FROM statistics s
            JOIN statistics_meta sm ON s.metadata_id = sm.id
            WHERE sm.statistic_id = ?
            AND s.created_ts >= ?
            AND s.created_ts <= ?
            ORDER BY s.created_ts ASC
        """
        cursor.execute(query_old, (entity_id, start_ts, end_ts))
        results = cursor.fetchall()
    
    if not results:
        raise ValueError(f"No statistics data found for entity '{entity_id}' in the specified period")
    
    # Convert to DataFrame
    df = pd.DataFrame(results, columns=['start_ts', 'sum', 'state'])
    
    # Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['start_ts'], unit='s', utc=True)
    df = df.sort_values('timestamp')
    df = df.set_index('timestamp')
    
    # Use sum column if available, otherwise use state
    if df['sum'].notna().any():
        # Sum represents cumulative consumption - calculate hourly difference
        df['sum'] = pd.to_numeric(df['sum'], errors='coerce')
        hourly_consumption = df['sum'].diff()
        # First value is NaN from diff, use the sum value itself
        hourly_consumption.iloc[0] = df['sum'].iloc[0]
    else:
        # Use state column
        df['state'] = pd.to_numeric(df['state'], errors='coerce')
        hourly_consumption = df['state']
    
    # Handle negative values (resets)
    hourly_consumption[hourly_consumption < 0] = 0
    
    # Drop NaN values
    hourly_consumption = hourly_consumption.dropna()
    
    return hourly_consumption

def list_available_statistics(db_conn):
    """
    List all available statistics entities in the database.
    """
    cursor = db_conn.cursor()
    query = """
        SELECT sm.statistic_id, sm.source, sm.unit_of_measurement, 
               COUNT(s.id) as record_count,
               MIN(s.start_ts) as first_record,
               MAX(s.start_ts) as last_record
        FROM statistics_meta sm
        LEFT JOIN statistics s ON sm.id = s.metadata_id
        WHERE sm.statistic_id LIKE '%grid%' OR sm.statistic_id LIKE '%energy%'
        GROUP BY sm.id
        ORDER BY sm.statistic_id
    """
    cursor.execute(query)
    results = cursor.fetchall()
    return results

def fetch_entsoe_prices(api_key, country_code, start_dt, end_dt):
    """
    Fetch hourly day-ahead prices from ENTSO-E.
    """
    client = EntsoePandasClient(api_key=api_key)
    
    try:
        # Convert datetime to pandas Timestamp with timezone
        start_pd = pd.Timestamp(start_dt)
        end_pd = pd.Timestamp(end_dt)
        
        prices = client.query_day_ahead_prices(
            country_code=country_code,
            start=start_pd,
            end=end_pd
        )
        
        if prices.empty:
            raise ValueError("No price data retrieved from ENTSO-E")
        
        # Convert from EUR/MWh to EUR/kWh
        prices = prices / 1000
        
        # Ensure timezone aware
        if prices.index.tz is None:
            prices.index = prices.index.tz_localize('UTC')
        else:
            prices.index = prices.index.tz_convert('UTC')
        
        # Resample to hourly
        prices = prices.resample('1H').mean().fillna(method='ffill')
        
        return prices
        
    except Exception as e:
        raise ValueError(f"Error fetching ENTSO-E data: {e}")

# Connect to database
conn = sqlite3.connect(DB_PATH)
tz = pytz.timezone(TIMEZONE)

try:
    print("\nStep 0: Checking available energy statistics...")
    available_stats = list_available_statistics(conn)
    
    if available_stats:
        print(f"  Found {len(available_stats)} energy-related statistics:")
        for stat in available_stats[:10]:  # Show first 10
            statistic_id, source, unit, count, first, last = stat
            if count > 0:
                first_dt = datetime.datetime.fromtimestamp(first, tz=pytz.UTC).strftime('%Y-%m-%d')
                last_dt = datetime.datetime.fromtimestamp(last, tz=pytz.UTC).strftime('%Y-%m-%d')
                print(f"    - {statistic_id}")
                print(f"      Unit: {unit}, Records: {count}, Range: {first_dt} to {last_dt}")
    
    print("\nStep 1: Extracting hourly energy data from statistics...")
    
    # Get hourly import and export data
    import_hourly = get_statistics_data(
        conn, GRID_IMPORT_ENTITY, start_period, end_period
    )
    export_hourly = get_statistics_data(
        conn, GRID_EXPORT_ENTITY, start_period, end_period
    )
    
    total_import_kwh = import_hourly.sum()
    total_export_kwh = export_hourly.sum()
    
    print(f"  ✓ Total imported energy: {total_import_kwh:.2f} kWh")
    print(f"  ✓ Total exported energy: {total_export_kwh:.2f} kWh")
    print(f"  ✓ Hours with data: {len(import_hourly)}")
    
    print("\nStep 2: Fetching ENTSO-E day-ahead prices...")
    
    # Fetch ENTSO-E prices
    entsoe_prices = fetch_entsoe_prices(
        ENTSOE_API_KEY, ENTSOE_COUNTRY_CODE, start_period, end_period
    )
    
    print(f"  ✓ Price data retrieved: {len(entsoe_prices)} hours")
    print(f"  ✓ Average price: {entsoe_prices.mean():.4f} EUR/kWh")
    print(f"  ✓ Min price: {entsoe_prices.min():.4f} EUR/kWh")
    print(f"  ✓ Max price: {entsoe_prices.max():.4f} EUR/kWh")
    
    print("\nStep 3: Calculating tariff costs...")
    
    # ============ FIXED TARIFF CALCULATION ============
    fixed_import_cost = total_import_kwh * FIXED_IMPORT_PRICE
    fixed_export_credit = total_export_kwh * FIXED_EXPORT_PRICE
    fixed_base_cost = BASE_FIXED_MONTHLY * num_months
    total_fixed_cost = fixed_import_cost - fixed_export_credit + fixed_base_cost
    
    print(f"\n{'FIXED TARIFF':-^70}")
    print(f"  Import cost:      {total_import_kwh:>10.2f} kWh × {FIXED_IMPORT_PRICE:.4f} = {fixed_import_cost:>10.2f} EUR")
    print(f"  Export credit:    {total_export_kwh:>10.2f} kWh × {FIXED_EXPORT_PRICE:.4f} = -{fixed_export_credit:>9.2f} EUR")
    print(f"  Base fee:         {num_months:>10} months × {BASE_FIXED_MONTHLY:.2f} = {fixed_base_cost:>10.2f} EUR")
    print(f"  {'-'*70}")
    print(f"  TOTAL COST:                                          {total_fixed_cost:>10.2f} EUR")
    
    # ============ DYNAMIC TARIFF CALCULATION ============
    # Align hourly consumption with prices
    consumption_df = pd.DataFrame({
        'import_kwh': import_hourly,
        'export_kwh': export_hourly
    })
    
    # Join with prices
    combined = consumption_df.join(entsoe_prices.rename('price_eur_kwh'), how='inner')
    
    # Calculate hourly costs
    combined['dynamic_price_total'] = combined['price_eur_kwh'] + BASE_DYNAMIC_KWH
    combined['hourly_import_cost'] = combined['import_kwh'] * combined['dynamic_price_total']
    combined['hourly_export_credit'] = combined['export_kwh'] * FIXED_EXPORT_PRICE
    
    dynamic_import_cost = combined['hourly_import_cost'].sum()
    dynamic_export_credit = combined['hourly_export_credit'].sum()
    dynamic_base_cost = BASE_DYNAMIC_MONTHLY * num_months
    total_dynamic_cost = dynamic_import_cost - dynamic_export_credit + dynamic_base_cost
    
    weighted_avg_price = (combined['hourly_import_cost'].sum() / combined['import_kwh'].sum()) if combined['import_kwh'].sum() > 0 else 0
    
    print(f"\n{'DYNAMIC TARIFF (Hourly Matching)':-^70}")
    print(f"  Avg ENTSO-E price:                                   {entsoe_prices.mean():>10.4f} EUR/kWh")
    print(f"  Base markup:                                         +{BASE_DYNAMIC_KWH:>9.4f} EUR/kWh")
    print(f"  Weighted avg price (consumption-matched):            {weighted_avg_price:>10.4f} EUR/kWh")
    print(f"  ")
    print(f"  Import cost:      {combined['import_kwh'].sum():>10.2f} kWh (hourly rates) = {dynamic_import_cost:>10.2f} EUR")
    print(f"  Export credit:    {combined['export_kwh'].sum():>10.2f} kWh × {FIXED_EXPORT_PRICE:.4f} = -{dynamic_export_credit:>9.2f} EUR")
    print(f"  Base fee:         {num_months:>10} months × {BASE_DYNAMIC_MONTHLY:.2f} = {dynamic_base_cost:>10.2f} EUR")
    print(f"  {'-'*70}")
    print(f"  TOTAL COST:                                          {total_dynamic_cost:>10.2f} EUR")
    
    # ============ COMPARISON ============
    savings = total_fixed_cost - total_dynamic_cost
    savings_pct = (savings / total_fixed_cost * 100) if total_fixed_cost > 0 else 0
    
    print(f"\n{'COMPARISON':-^70}")
    print(f"  Fixed tariff total:                                  {total_fixed_cost:>10.2f} EUR")
    print(f"  Dynamic tariff total:                                {total_dynamic_cost:>10.2f} EUR")
    print(f"  {'-'*70}")
    
    if savings > 0:
        print(f"  💰 DYNAMIC SAVES:                                     {savings:>10.2f} EUR ({savings_pct:+.1f}%)")
    elif savings < 0:
        print(f"  💰 FIXED SAVES:                                       {abs(savings):>10.2f} EUR ({abs(savings_pct):+.1f}%)")
    else:
        print(f"  ⚖️  EQUAL COST")
    
    # ============ ADDITIONAL STATISTICS ============
    print(f"\n{'STATISTICS':-^70}")
    print(f"  Hours analyzed:                                      {len(combined):>10}")
    print(f"  Best hourly price:                                   {entsoe_prices.min():>10.4f} EUR/kWh")
    print(f"  Worst hourly price:                                  {entsoe_prices.max():>10.4f} EUR/kWh")
    print(f"  Price volatility (std dev):                          {entsoe_prices.std():>10.4f} EUR/kWh")
    
    # Find hours with highest consumption
    if len(combined) > 0:
        top_hours = combined.nlargest(5, 'import_kwh')[['import_kwh', 'price_eur_kwh', 'dynamic_price_total']]
        print(f"\n  Top 5 consumption hours:")
        for idx, row in top_hours.iterrows():
            print(f"    {idx.strftime('%Y-%m-%d %H:%M')} | {row['import_kwh']:.2f} kWh @ {row['dynamic_price_total']:.4f} EUR/kWh")
    
    # Export detailed CSV
    output_file = "energy_comparison_hourly.csv"
    combined.to_csv(output_file)
    print(f"\n  📊 Detailed hourly data exported to: {output_file}")
    
except Exception as e:
    print(f"\n❌ Error: {e}")
    print("\nTroubleshooting:")
    print("  - Check the entity IDs listed above")
    print("  - Ensure entities have statistics enabled in Home Assistant")
    print("  - Verify ENTSO-E API key is valid")
    import traceback
    print("\nDetailed error:")
    traceback.print_exc()
    
finally:
    conn.close()

print("\n" + "=" * 70)
print("Analysis complete!")
