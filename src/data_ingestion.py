import os
import pandas as pd
from pysus.online_data import SIH, CNES
from datetime import datetime
import pyarrow.parquet as pq

class DataIngestor:
    """
    Class to handle data extraction from DATASUS (SIH and CNES)
    Targeting Chronic Disease Analysis (Diabetes and Hypertension)
    """
    def __init__(self, raw_data_path='../data/raw'):
        self.raw_path = raw_data_path
        os.makedirs(self.raw_path, exist_ok=True)

    def fetch_hospitalization_data(self, state, year, month):
        """
        Downloads SIH (Hospital Information System) data for a specific state and period
        """
        print(f"[*] Downloading SIH data for {state} - {year}/{month}...")
        try:
            # Downloading data using PySUS
            df = SIH.download(state, year, month)
            
            # Creating a filename with timestamp for versioning
            filename = f"SIH_{state}_{year}_{month}.parquet"
            full_path = os.path.join(self.raw_path, filename)
            
            # Saving as Parquet - Industry standard for ML pipelines
            df.to_parquet(full_path, index=False)
            print(f"[+] Successfully saved to {full_path}")
            return df
        except Exception as e:
            print(f"[-] Error downloading SIH: {e}")
            return None

    def fetch_facilities_data(self, state, year, month):
        """
        Downloads CNES (Healthcare Facilities) data
        Essential for GIS analysis (location of hospitals/clinics)
        """
        print(f"[*] Downloading CNES data for {state} - {year}/{month}...")
        try:
            for group in ["ST", "LT"]:
                # df = CNES.download(group='ST', states=state, years=year, months=month) # ST = Establishments
                parquets = CNES.download(group=group, states=state, years=year, months=month) # ST = Establishments
                filename = f"CNES_{group}_{state}_{year}_{month}.parquet"
                full_path = os.path.join(self.raw_path, filename)
                print(parquets)
                
                df = pd.concat([parquet.to_dataframe() for parquet in parquets], ignore_index=True)
                df.to_parquet(full_path, index=False)
                print(f"[+] Successfully saved to {full_path}")
            return df
        except Exception as e:
            print(f"[-] Error downloading CNES: {e}")
            return None

if __name__ == "__main__":
    # Quick Test
    ingestor = DataIngestor()
    # Let's get data from Paraná (PR) for January 2025 as a sample
    ingestor.fetch_hospitalization_data('PR', 2025, [1,2,3,4,5,6,7,8,9,10,11,12])
    ingestor.fetch_facilities_data('PR', 2025, [1,2,3,4,5,6,7,8,9,10,11,12])