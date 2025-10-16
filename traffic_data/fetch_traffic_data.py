#!/usr/bin/env python3
"""
Clean & Reliable Delhi OTD Real-Time Traffic + Emissions Monitor

Outputs:
 - raw_traffic_emissions.csv       : vehicle positions + estimated emissions
 - vehicle_count_log.csv           : total active vehicles per run
 - traffic_pollution_summary.csv   : total estimated pollutants per run
"""

import requests
import pandas as pd
import sys
import csv
import logging
from datetime import datetime, timezone
from google.transit import gtfs_realtime_pb2
from pathlib import Path

# LOGGING SETUP
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# CONFIG
BASE = "https://otd.delhi.gov.in/api/realtime/VehiclePositions.pb"
API_KEY = "ljxlGhsj0O9iBPhMnLGZ1E5UBCd2spj7"
CITY = "Anand Vihar, New Delhi"

OUT_RAW = "raw_traffic_emissions.csv"
OUT_VEHICLE_COUNT = "vehicle_count_log.csv"
OUT_POLLUTION_SUMMARY = "traffic_pollution_summary.csv"

# Average emission factors for diesel buses (g/km)
EMISSION_FACTORS = {"CO": 6.0, "NOx": 8.0, "PM2.5": 0.5, "CO2": 1100.0}

# Data validation thresholds
SPEED_MAX_M_S = 30.0  # ~108 km/h - reasonable bus max speed
LAT_MIN, LAT_MAX = 28.5, 28.8  # Delhi bounds
LON_MIN, LON_MAX = 77.0, 77.4


def validate_location(lat, lon):
    """Validate latitude/longitude are within Delhi bounds."""
    return LAT_MIN <= lat <= LAT_MAX and LON_MIN <= lon <= LON_MAX


def validate_speed(speed):
    """Validate speed is reasonable (0-30 m/s)."""
    return 0.0 <= speed <= SPEED_MAX_M_S


def fetch_traffic_data():
    """Fetch real-time vehicle positions from OTD feed with retry logic."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            r = requests.get(BASE, params={"key": API_KEY}, timeout=30)
            r.raise_for_status()
            logger.info(f"✅ API request successful (attempt {attempt + 1})")
            break
        except requests.Timeout:
            logger.warning(f"⚠️ Timeout on attempt {attempt + 1}/{max_retries}")
            if attempt == max_retries - 1:
                logger.error("❌ Max retries exceeded")
                sys.exit(1)
        except requests.RequestException as e:
            logger.error(f"❌ OTD API request failed: {e}")
            sys.exit(1)

    try:
        feed = gtfs_realtime_pb2.FeedMessage()  # type: ignore
        feed.ParseFromString(r.content) # type: ignore
    except Exception as e:
        logger.error(f"❌ Failed to parse protobuf: {e}")
        sys.exit(1)

    entities = [e for e in feed.entity if e.HasField("vehicle")]
    if not entities:
        logger.error("❌ No vehicles found in feed")
        sys.exit(1)

    logger.info(f"✅ Fetched {len(entities)} vehicles from OTD feed")
    return entities


def process_vehicles(entities):
    """Convert vehicle feed to DataFrame with estimated emissions and validation."""
    rows = []
    skipped = 0

    for entity in entities:
        v = entity.vehicle
        
        # Validate required fields
        if not v.HasField("position") or not v.HasField("timestamp"):
            skipped += 1
            continue

        lat, lon = v.position.latitude, v.position.longitude
        speed = getattr(v.position, "speed", 0.0)  # m/s

        # Validate location and speed
        if not validate_location(lat, lon):
            skipped += 1
            continue

        if not validate_speed(speed):
            skipped += 1
            continue

        try:
            ts = datetime.fromtimestamp(v.timestamp, timezone.utc)
        except (ValueError, OSError):
            skipped += 1
            continue

        # Get vehicle ID safely
        vehicle_id = getattr(v, "id", None) or entity.id or "unknown"

        # Estimate distance traveled per minute (km)
        distance_km = (speed * 3.6) / 60.0

        # Calculate emissions (rounded to 2 decimals)
        emissions = {p: round(f * distance_km, 2) for p, f in EMISSION_FACTORS.items()}

        rows.append(
            {
                "vehicle_id": vehicle_id,
                "timestamp": ts.astimezone().isoformat(),
                "latitude": round(lat, 6),
                "longitude": round(lon, 6),
                "speed_m_s": round(speed, 2),
                **emissions,
            }
        )

    if skipped > 0:
        logger.info(f"⚠️ Skipped {skipped} invalid vehicle records")

    df = pd.DataFrame(rows)
    # Remove duplicates based on vehicle_id and timestamp
    df = df.drop_duplicates(subset=["vehicle_id", "timestamp"], keep="first")
    logger.info(f"Processing complete: {len(df)} valid records")
    return df


def save_raw_data(df):
    """Save raw data with overwrite (append mode not suitable for raw data)."""
    try:
        df.to_csv(OUT_RAW, index=False)
        logger.info(f"✅ Saved raw traffic + emissions → {OUT_RAW} ({len(df)} rows)")
    except Exception as e:
        logger.error(f"❌ Failed to save raw data: {e}")
        sys.exit(1)


def log_vehicle_count(df):
    """Log vehicle count with header check."""
    vehicle_count = df["vehicle_id"].nunique()
    try:
        file_exists = Path(OUT_VEHICLE_COUNT).exists()
        with open(OUT_VEHICLE_COUNT, "a", newline="") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["timestamp", "vehicle_count"])
            writer.writerow([datetime.now().isoformat(), vehicle_count])
        logger.info(f"✅ Logged vehicle count: {vehicle_count}")
    except Exception as e:
        logger.error(f"❌ Failed to log vehicle count: {e}")


def save_pollution_summary(df):
    """Save pollution summary with header check."""
    try:
        total_pollutants = df[["CO", "NOx", "PM2.5", "CO2"]].sum()
        summary = {
            "timestamp": datetime.now().isoformat(),
            "vehicle_count": df["vehicle_id"].nunique(),
            "CO_total_g": round(total_pollutants["CO"], 2),
            "NOx_total_g": round(total_pollutants["NOx"], 2),
            "PM2.5_total_g": round(total_pollutants["PM2.5"], 2),
            "CO2_total_g": round(total_pollutants["CO2"], 2),
        }
        
        file_exists = Path(OUT_POLLUTION_SUMMARY).exists()
        mode = "a" if file_exists else "w"
        write_header = not file_exists
        
        pd.DataFrame([summary]).to_csv(
            OUT_POLLUTION_SUMMARY, mode=mode, header=write_header, index=False
        )
        logger.info(f"✅ Saved pollution summary → {OUT_POLLUTION_SUMMARY}")
        logger.info(f"Summary: {summary}")
    except Exception as e:
        logger.error(f"❌ Failed to save pollution summary: {e}")


def main():
    try:
        entities = fetch_traffic_data()
        df = process_vehicles(entities)

        if df.empty:
            logger.error("❌ No valid vehicle data after processing")
            sys.exit(1)

        save_raw_data(df)
        log_vehicle_count(df)
        save_pollution_summary(df)
        logger.info("✅ All operations completed successfully")
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
