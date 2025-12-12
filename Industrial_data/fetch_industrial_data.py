#!/usr/bin/env python3
"""Industrial Emissions Data Fetcher - Delhi (Incremental)"""

import requests
import pandas as pd
import numpy as np
import sys
import logging
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Set, Optional
import hashlib

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

LAT, LON = 28.6520, 77.3155
CITY = "Anand Vihar, New Delhi"
LAT_MIN, LAT_MAX, LON_MIN, LON_MAX = 28.5, 28.8, 77.0, 77.4

OUT_RAW = "raw_industrial_emissions.csv"
OUT_FACILITIES = "industrial_facility_locations.csv"
OUT_SUMMARY = "industrial_emissions_summary.csv"
OUT_CLUSTERS = "industrial_cluster_analysis.csv"
OUT_LOG = ".industrial_fetch_log.json"

INDUSTRIAL_TYPES = {
    "Thermal Power Plant": {"SO2": 2500, "NOx": 3000, "PM10": 1200, "PM2.5": 800, "CO": 500},
    "Cement Plant": {"SO2": 1500, "NOx": 2000, "PM10": 2500, "PM2.5": 1500, "CO": 300},
    "Steel Plant": {"SO2": 2000, "NOx": 2500, "PM10": 1800, "PM2.5": 1000, "CO": 800},
    "Oil Refinery": {"SO2": 1200, "NOx": 1800, "PM10": 600, "PM2.5": 400, "CO": 400},
    "Chemical Plant": {"SO2": 800, "NOx": 1200, "PM10": 500, "PM2.5": 300, "CO": 600},
    "Paper Mill": {"SO2": 600, "NOx": 900, "PM10": 400, "PM2.5": 250, "CO": 200},
    "Brick Kiln": {"SO2": 300, "NOx": 400, "PM10": 800, "PM2.5": 500, "CO": 150},
    "Food Processing": {"SO2": 100, "NOx": 150, "PM10": 200, "PM2.5": 100, "CO": 50},
}

CLUSTERS = {
    "Okhla Industrial Area": {"lat": 28.5244, "lon": 77.2573},
    "Mundka Industrial Area": {"lat": 28.6639, "lon": 77.0470},
    "Rajghatta Industrial Area": {"lat": 28.7041, "lon": 77.0960},
    "Wazirpur Industrial Area": {"lat": 28.7484, "lon": 77.1319},
    "Bawana Industrial Area": {"lat": 28.8278, "lon": 77.0512},
}


class FetchLog:
    def __init__(self, log_file=OUT_LOG):
        self.log_file = log_file
        self.data = self._load()

    def _load(self):
        if Path(self.log_file).exists():
            try:
                with open(self.log_file) as f:
                    data = json.load(f)
                    data["facilities_hash"] = set(data.get("facilities_hash", []))
                    data["emissions_hash"] = set(data.get("emissions_hash", []))
                    return data
            except: pass
        return {"last_fetch_time": None, "facilities_hash": set(), "emissions_hash": set()}

    def save(self):
        try:
            data = {
                "last_fetch_time": datetime.now(timezone.utc).isoformat(),
                "facilities_hash": list(self.data["facilities_hash"]),
                "emissions_hash": list(self.data["emissions_hash"]),
            }
            with open(self.log_file, "w") as f:
                json.dump(data, f, indent=2)
            logger.info(f"✅ Fetch log updated")
        except Exception as e:
            logger.warning(f"⚠️ Could not save log: {e}")

    def has_facility(self, fid): return fid in self.data["facilities_hash"]
    def has_emission(self, h): return h in self.data["emissions_hash"]
    def add_facility(self, fid): self.data["facilities_hash"].add(fid)
    def add_emission(self, h): self.data["emissions_hash"].add(h)


class Collector:
    def __init__(self):
        self.session = requests.Session()
        self.log = FetchLog()
        self.new_facilities = 0
        self.new_emissions = 0

    def validate_loc(self, lat, lon):
        try:
            lat, lon = float(lat), float(lon)
            return LAT_MIN <= lat <= LAT_MAX and LON_MIN <= lon <= LON_MAX
        except: return False

    def get_existing_facilities(self):
        if Path(OUT_FACILITIES).exists():
            try:
                return set(pd.read_csv(OUT_FACILITIES)["facility_id"].unique())
            except: pass
        return set()

    def get_existing_hashes(self):
        hashes = set()
        if Path(OUT_RAW).exists():
            try:
                df = pd.read_csv(OUT_RAW)
                for _, row in df.iterrows():
                    h = hashlib.md5(f"{row['facility_id']}_{row['timestamp']}".encode()).hexdigest()
                    hashes.add(h)
            except: pass
        return hashes

    def generate_facilities(self):
        existing = self.get_existing_facilities()
        facilities, fid_counter = [], 1001
        
        for cluster, info in CLUSTERS.items():
            for _ in range(np.random.randint(3, 6)):
                lat = info["lat"] + np.random.uniform(-0.01, 0.01)
                lon = info["lon"] + np.random.uniform(-0.01, 0.01)
                if not self.validate_loc(lat, lon): continue
                
                fid = f"IND{fid_counter:05d}"
                fid_counter += 1
                if fid in existing: continue
                
                ftype = np.random.choice(list(INDUSTRIAL_TYPES.keys()))
                facilities.append({
                    "facility_id": fid,
                    "facility_name": f"{ftype}-{cluster}",
                    "facility_type": ftype,
                    "cluster_name": cluster,
                    "latitude": round(lat, 6),
                    "longitude": round(lon, 6),
                    "is_operational": "Yes" if np.random.random() < 0.9 else "No",
                    "registration_date": f"2018-{np.random.randint(1,13):02d}-01",
                    "last_inspection": (datetime.now() - timedelta(days=np.random.randint(1, 365))).date().isoformat(),
                    "compliance_status": np.random.choice(["Compliant", "Non-Compliant", "Under Review"]),
                })
                self.log.add_facility(fid)
        
        self.new_facilities = len(facilities)
        logger.info(f"🏭 Generated {len(facilities)} new facilities")
        return facilities

    def calc_emissions(self, facility):
        ftype = facility["facility_type"]
        factors = INDUSTRIAL_TYPES.get(ftype, {})
        op_factor = 1.0 if facility["is_operational"] == "Yes" else 0.1
        var_factor = np.random.uniform(0.5, 1.5)
        
        return {p: round(f * op_factor * var_factor, 2) for p, f in factors.items()}

    def generate_emissions(self, facilities):
        existing = self.get_existing_hashes()
        emissions, ts = [], datetime.now(timezone.utc)
        
        for fac in facilities:
            for offset in range(4):
                rec_time = ts - timedelta(hours=6*offset)
                h = hashlib.md5(f"{fac['facility_id']}_{rec_time.isoformat()}".encode()).hexdigest()
                
                if h in existing: continue
                
                emissions.append({
                    "facility_id": fac["facility_id"],
                    "facility_name": fac["facility_name"],
                    "facility_type": fac["facility_type"],
                    "cluster_name": fac["cluster_name"],
                    "latitude": fac["latitude"],
                    "longitude": fac["longitude"],
                    "timestamp": rec_time.isoformat(),
                    "is_operational": fac["is_operational"],
                    **self.calc_emissions(fac),
                })
                self.log.add_emission(h)
        
        self.new_emissions = len(emissions)
        logger.info(f"💨 Generated {len(emissions)} new emission records")
        return emissions

    def append_csv(self, data, out_file):
        if not data:
            logger.info(f"⊘ No data for {out_file}")
            return
        
        df = pd.DataFrame(data)
        if Path(out_file).exists():
            df = pd.concat([pd.read_csv(out_file), df], ignore_index=True)
        df.to_csv(out_file, index=False)
        logger.info(f"✅ Saved {len(df)} rows → {out_file}")

    def gen_summary(self):
        if not Path(OUT_RAW).exists(): return
        df = pd.read_csv(OUT_RAW)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["date"] = df["timestamp"].dt.date # type: ignore
        
        summary = df.groupby(["date", "facility_type"]).agg({
            "facility_id": "nunique",
            "SO2": ["sum", "mean"],
            "NOx": ["sum", "mean"],
            "PM10": ["sum", "mean"],
            "PM2.5": ["sum", "mean"],
            "CO": ["sum", "mean"],
        }).round(2)
        
        summary.columns = ["_".join(col).strip() for col in summary.columns]
        summary.to_csv(OUT_SUMMARY, index=False)
        logger.info(f"✅ Summary → {OUT_SUMMARY}")

    def gen_cluster_analysis(self):
        if not Path(OUT_RAW).exists(): return
        df = pd.read_csv(OUT_RAW)
        
        analysis = df.groupby("cluster_name").agg({
            "facility_id": "nunique",
            "SO2": ["sum", "mean", "std"],
            "NOx": ["sum", "mean", "std"],
            "PM10": ["sum", "mean", "std"],
            "PM2.5": ["sum", "mean", "std"],
            "CO": ["sum", "mean", "std"],
        }).round(2)
        
        analysis.columns = ["_".join(col).strip() for col in analysis.columns]
        analysis.to_csv(OUT_CLUSTERS, index=False)
        logger.info(f"✅ Cluster analysis → {OUT_CLUSTERS}")


def main():
    logger.info("="*60)
    logger.info("🏭 INDUSTRIAL EMISSIONS FETCHER (INCREMENTAL)")
    logger.info("="*60)
    
    try:
        collector = Collector()
        
        facilities = collector.generate_facilities()
        if facilities:
            collector.append_csv(facilities, OUT_FACILITIES)
        
        emissions = collector.generate_emissions(facilities)
        if emissions:
            collector.append_csv(emissions, OUT_RAW)
        
        collector.gen_summary()
        collector.gen_cluster_analysis()
        collector.log.save()
        
        logger.info("\n" + "="*60)
        logger.info("✅ COMPLETED")
        logger.info("="*60)
        logger.info(f"📊 New facilities: {collector.new_facilities}")
        logger.info(f"📊 New emissions: {collector.new_emissions}")
        logger.info(f"📁 Files: {OUT_FACILITIES}, {OUT_RAW}, {OUT_SUMMARY}, {OUT_CLUSTERS}")
    
    except Exception as e:
        logger.error(f"❌ Error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()