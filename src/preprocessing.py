import pandas as pd
import os

class HealthPreprocessor:
    def __init__(self, raw_path='../data/raw', processed_path='../data/processed'):
        self.raw_path = raw_path
        self.processed_path = processed_path
        os.makedirs(self.processed_path, exist_ok=True)

    def process_sih(self, df_sih):
        """
        Filters for Chronic Diseases and aggregates by municipality.
        """
        # 1. Filter by CID-10 (Diabetes E10-E14 and Hypertension I10-I15)
        # The column 'DIAG_PRINC' contains the primary diagnosis
        chronic_cids = ['E10', 'E11', 'E12', 'E13', 'E14', 'I10', 'I11', 'I12', 'I13', 'I14', 'I15']
        
        # Ensure string type and filter
        df_sih['DIAG_PRINC'] = df_sih['DIAG_PRINC'].astype(str)
        df_filtered = df_sih[df_sih['DIAG_PRINC'].str.startswith(tuple(chronic_cids))].copy()

        # 2. Convert value columns to numeric (DATAsus often brings them as strings/objects)
        df_filtered['VAL_TOT'] = pd.to_numeric(df_filtered['VAL_TOT'], errors='coerce')

        # 3. Convert length of stay to numeric
        df_filtered['DIAS_PERM'] = pd.to_numeric(df_filtered['DIAS_PERM'], errors='coerce')
        
        # 4. Aggregate by Municipality (MUNIC_RES)
        agg_sih = df_filtered.groupby('MUNIC_RES').agg(
            total_admissions=('DIAG_PRINC', 'count'),
            avg_cost=('VAL_TOT', 'mean'),
            total_cost=('VAL_TOT', 'sum'),
            avg_stay=('DIAS_PERM', 'mean')
        ).reset_index()

        return agg_sih

    def process_cnes(self, df_st, df_lt):
        """
        Processes Infrastructure data: Number of facilities and total beds.
        """
        # Count facilities per municipality
        facilities_count = df_st.groupby('CODUFMUN').size().reset_index(name='facility_count')
        
        # Sum beds per municipality
        df_lt['QT_EXIST'] = pd.to_numeric(df_lt['QT_EXIST'], errors='coerce')
        beds_count = df_lt.groupby('CODUFMUN')['QT_EXIST'].sum().reset_index(name='total_beds')

        # Merge infrastructure
        infra = pd.merge(facilities_count, beds_count, on='CODUFMUN', how='outer').fillna(0)
        return infra

    def run_pipeline(self, state, year, month):
        """
        Main method to join SIH and CNES data.
        """
        # Loading files (assumes they exist from ingestion)
        sih_file = os.path.join(self.raw_path, f"SIH_RD_{state}_{year}_{month}.parquet")
        st_file = os.path.join(self.raw_path, f"CNES_ST_{state}_{year}_{month}.parquet")
        lt_file = os.path.join(self.raw_path, f"CNES_LT_{state}_{year}_{month}.parquet")

        df_sih = pd.read_parquet(sih_file)
        df_st = pd.read_parquet(st_file)
        df_lt = pd.read_parquet(lt_file)

        # Processing
        sih_agg = self.process_sih(df_sih)
        infra_agg = self.process_cnes(df_st, df_lt)

        # Final Join
        # Note: SIH uses 'MUNIC_RES' (6 digits) and CNES uses 'CODUFMUN' (6 digits)
        final_df = pd.merge(
            sih_agg, 
            infra_agg, 
            left_on='MUNIC_RES', 
            right_on='CODUFMUN', 
            how='inner'
        )

        # Feature Engineering: Beds per Admission (proxy for pressure on system)
        final_df['beds_per_admission'] = final_df['total_beds'] / (final_df['total_admissions'] + 1)

        # Save processed data
        output_name = f"final_processed_{state}_{year}_{month}.parquet"
        final_df.to_parquet(os.path.join(self.processed_path, output_name))
        print(f"[+] Processed data saved for {state}!")

if __name__ == "__main__":
    preprocessor = HealthPreprocessor()
    preprocessor.run_pipeline('PR', 2025, [1,2,3,4,5,6,7,8,9,10,11,12])