#!/usr/bin/env python3
"""
Fetch OpenWeather Air Pollution History for Delhi (2024-09-01 -> 2025-09-30)
Parameters: NO2, SO2, CO, O3, NH3
Outputs:
 - raw_hourly_openaq.csv      : one row per measurement (timestamp local/utc, location, parameter, value, unit, coordinates)
 - daily_station_avg.csv     : daily averages per station per parameter
 - monthly_city_avg.csv      : monthly city-average per parameter (month start YYYY-MM)
"""

import requests
import pandas as pd
import time
import sys
from datetime import datetime, timezone

# CONFIG
BASE = "http://api.openweathermap.org/data/2.5/air_pollution/history"
LAT = 28.6520
LON = 77.3155
CITY = "Anand Vihar, New Delhi"
DATE_FROM = "2024-09-01T00:00:00Z"
DATE_TO   = "2025-09-30T23:59:59Z"
PARAMETERS = ["no2","so2","co","o3","nh3","pm2_5","pm10","benzene","toluene","xylene","nox"]
OUT_RAW = "raw_hourly_openaq.csv"
OUT_DAILY = "daily_station_avg.csv"
OUT_MONTHLY = "monthly_city_avg.csv"

API_KEY = "9a69dca6291c4cd360a5b2942dcd849b"

def iso_to_unix(iso):
    dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    return int(dt.timestamp())

def fetch_openweather_history():
    start = iso_to_unix(DATE_FROM)
    end = iso_to_unix(DATE_TO)
    params = {
        "lat": LAT,
        "lon": LON,
        "start": start,
        "end": end,
        "appid": API_KEY
    }
    print(f"Fetching OpenWeather Air Pollution History for {CITY} ({DATE_FROM} to {DATE_TO})")
    r = requests.get(BASE, params=params, timeout=60)
    if r.status_code != 200:
        print("ERROR", r.status_code, r.text)
        raise SystemExit("OpenWeather request failed. Inspect the response.")
    j = r.json()
    # OpenWeather returns a 'list' of hourly measurements
    results = j.get("list", [])
    rows = []
    for rec in results:
        dt_utc = datetime.utcfromtimestamp(rec["dt"]).replace(tzinfo=timezone.utc)
        # OpenWeather provides all parameters in one record
        comps = rec.get("components", {})
        for param in PARAMETERS:
            if param in comps:
                rows.append({
                    "parameter": param,
                    "value": comps[param],
                    "unit": "µg/m³",  # OpenWeather uses µg/m³ for all
                    "date_utc": dt_utc.isoformat(),
                    "date_local": dt_utc.astimezone().isoformat(),
                    "location": CITY,
                    "country": "IN",
                    "city": CITY,
                    "latitude": LAT,
                    "longitude": LON,
                    "sourceName": "OpenWeather"
                })
    return pd.DataFrame(rows)

def save_citywise_wide(df, out_csv="City_wise_data.csv"):
    # Pivot to wide format: one row per timestamp, columns for each parameter
    wide = df.pivot_table(
        index="date_local",
        columns="parameter",
        values="value",
        aggfunc="mean"
    ).reset_index()

    # Optional: Rename columns to match your sample file
    rename_map = {
        "pm2_5": "PM2.5 (µg/m³)",
        "pm10": "PM10 (µg/m³)",
        "no2": "NO2 (µg/m³)",
        "so2": "SO2 (µg/m³)",
        "co": "CO (µg/m³)",
        "o3": "Ozone (µg/m³)",
        "nh3": "NH3 (µg/m³)",
        # Add more mappings as needed
    }
    wide.rename(columns=rename_map, inplace=True)

    # Rename date column to Timestamp for consistency
    wide.rename(columns={"date_local": "Timestamp"}, inplace=True)

    # Sort by Timestamp
    wide = wide.sort_values("Timestamp")

    # Save to CSV
    wide.to_csv(out_csv, index=False)
    print(f"Saved city-wise wide data -> {out_csv} (rows={len(wide)})")

def main():
    df = fetch_openweather_history()
    if df.empty:
        print("No data fetched. Exiting.")
        sys.exit(1)
    # Convert datetimes
    df['date_utc'] = pd.to_datetime(df['date_utc'], errors='coerce')
    df['date_local'] = pd.to_datetime(df['date_local'], errors='coerce')
    # Save raw
    df.to_csv(OUT_RAW, index=False)
    print(f"Saved raw measurements -> {OUT_RAW}  (rows={len(df)})")

    # Save city-wise wide format
    save_citywise_wide(df, out_csv="City_wise_data.csv")

    # Create daily station averages
    df['date_local_date'] = df['date_local'].dt.date
    daily = df.groupby(['date_local_date', 'location', 'parameter', 'unit']).agg(
        value_mean=('value','mean'),
        value_median=('value','median'),
        value_count=('value','count'),
        lat=('latitude','first'),
        lon=('longitude','first'),
        city=('city','first'),
        country=('country','first'),
    ).reset_index()
    daily.rename(columns={'date_local_date':'date'}, inplace=True)
    daily.to_csv(OUT_DAILY, index=False)
    print(f"Saved daily station averages -> {OUT_DAILY} (rows={len(daily)})")

    # City-monthly averages
    df['month'] = df['date_local'].dt.to_period('M').astype(str)
    monthly = df.groupby(['month','parameter','unit']).agg(
        city_value_mean=('value','mean'),
        city_value_count=('value','count')
    ).reset_index()
    monthly.to_csv(OUT_MONTHLY, index=False)
    print(f"Saved monthly city averages -> {OUT_MONTHLY} (rows={len(monthly)})")

    # City-yearly averages
    df['year'] = df['date_local'].dt.year
    yearly = df.groupby(['year', 'parameter', 'unit']).agg(
        city_value_mean=('value', 'mean'),
        city_value_count=('value', 'count')
    ).reset_index()
    yearly.to_csv("yearly_city_avg.csv", index=False)
    print(f"Saved yearly city averages -> yearly_city_avg.csv (rows={len(yearly)})")

if __name__ == "__main__":
    main()
