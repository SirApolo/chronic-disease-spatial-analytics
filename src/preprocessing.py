import pandas as pd
import numpy as np
import sidrapy
import os

class HealthPreprocessor:
    def __init__(self, raw_path='../data/raw', processed_path='../data/processed'):
        self.raw_path = raw_path
        self.processed_path = processed_path
        os.makedirs(self.processed_path, exist_ok=True)

    def get_census_population(self):
        """
        Fetch 2022 Census data (Table 4709) for Paraná municipalities using Sidrapy.
        Variable 93: Resident Population.
        """
        print("[*] Fetching 2022 Census population data via Sidrapy...")
        try:
            # Table 4709: Population (2022 Census)
            # territorial_level 6 = Municipalities
            # ibge_territorial_code 41 = Paraná
            # variable 93 = Resident Population
            data = sidrapy.get_table(
                table_code="4709",
                territorial_level="6",
                # ibge_territorial_code="41",
                ibge_territorial_code="all",
                variable="93"
            )

            # Cleanup sidrapy output (first row is header)
            df_pop = data[1:].copy()
            df_pop[["D1N", "UF"]] = df_pop["D1N"].str.split(" - ", expand=True)
            df_pop = df_pop[df_pop['UF'] == 'PR']  # Filter for Paraná
            df_pop = df_pop.reset_index(drop=True)
            df_pop = df_pop[['D1C', "D1N", 'V']] # Code and Value
            df_pop.columns = ['ibge_7', 'municipality', 'population']
            
            # Convert types and create 6-digit code for DATASUS join
            df_pop['population'] = pd.to_numeric(df_pop['population'], errors='coerce')
            df_pop['ibge_6'] = df_pop['ibge_7'].astype(str).str[:6].astype(float)
            
            print(f"[+] Population data retrieved for {len(df_pop)} municipalities.")
            # return df_pop[['ibge_6', 'population']]
            return df_pop
        except Exception as e:
            print(f"[-] Error fetching population: {e}")
            return None

    def process_sih(self, df_sih):
        """Filters for Chronic Diseases and aggregates by municipality."""
        # CID-10 Filters: Diabetes (E10-E14) and Hypertension (I10-I15)
        chronic_cids = ['E10', 'E11', 'E12', 'E13', 'E14', 'I10', 'I11', 'I12', 'I13', 'I14', 'I15']
        df_sih['DIAG_PRINC'] = df_sih['DIAG_PRINC'].astype(str)
        df_filtered = df_sih[df_sih['DIAG_PRINC'].str.startswith(tuple(chronic_cids))].copy()
        
        df_filtered['VAL_TOT'] = pd.to_numeric(df_filtered['VAL_TOT'], errors='coerce')
        df_filtered['MUNIC_RES'] = df_filtered['MUNIC_RES'].astype(float)
        df_filtered['DIAS_PERM'] = pd.to_numeric(df_filtered['DIAS_PERM'], errors='coerce')

        agg_sih = df_filtered.groupby('MUNIC_RES').agg(
            total_admissions=('DIAG_PRINC', 'count'),
            avg_cost=('VAL_TOT', 'mean'),
            total_cost=('VAL_TOT', 'sum'),
            avg_stay=('DIAS_PERM', 'mean')
        ).reset_index()
        return agg_sih

    def run_pipeline(self, state, year, month):
        print(f"[*] Starting Pipeline for {state} {year}/{month}")
        
        # 1. Load Raw Data (Saved by ingestion script)
        try:
            df_sih = pd.read_parquet(os.path.join(self.raw_path, f"SIH_RD_{state}_{year}_{month}.parquet"))
            df_st = pd.read_parquet(os.path.join(self.raw_path, f"CNES_ST_{state}_{year}_{month}.parquet"))
            df_lt = pd.read_parquet(os.path.join(self.raw_path, f"CNES_LT_{state}_{year}_{month}.parquet"))
        except FileNotFoundError as e:
            print(f"[-] Missing raw files. Run ingestion first. {e}")
            return

        # 2. Process SIH
        sih_agg = self.process_sih(df_sih)
        
        # 3. Process CNES Infrastructure
        df_lt['QT_EXIST'] = pd.to_numeric(df_lt['QT_EXIST'], errors='coerce')
        infra_agg = df_st.groupby('CODUFMUN').size().reset_index(name='facility_count')
        beds_agg = df_lt.groupby('CODUFMUN')['QT_EXIST'].sum().reset_index(name='total_beds')
        infra_final = pd.merge(infra_agg, beds_agg, on='CODUFMUN').astype({'CODUFMUN': float})

        # 4. Integrate Population (Sidrapy)
        df_pop = self.get_census_population()
        if df_pop is None: return

        # 5. Master Join
        # Join Health + Infrastructure
        df_merged = pd.merge(sih_agg, infra_final, left_on='MUNIC_RES', right_on='CODUFMUN', how='inner')
        # Join with Population
        df_final = pd.merge(df_merged, df_pop, left_on='MUNIC_RES', right_on='ibge_6', how='inner')

        # 6. Feature Engineering (The model's requirements)
        print("[*] Engineering normalized features...")
        df_final['admissions_per_100k'] = (df_final['total_admissions'] / df_final['population']) * 100000
        df_final['beds_per_1k'] = (df_final['total_beds'] / df_final['population']) * 1000
        df_final['log_population'] = np.log1p(df_final['population'])
        
        # Additional metadata for the App/GIS
        df_final['beds_per_admission'] = df_final['total_beds'] / (df_final['total_admissions'] + 1)

        # 7. Save Final Dataset
        output_file = os.path.join(self.processed_path, f"final_features_{state}.parquet")
        df_final.to_parquet(output_file, index=False)
        print(f"[+] Pipeline finished. Final features saved to: {output_file}")

if __name__ == "__main__":
    preprocessor = HealthPreprocessor()
    # Running for Paraná 2025
    preprocessor.run_pipeline('PR', 2025, [1,2,3,4,5,6,7,8,9,10,11,12])