import geobr
import pandas as pd
import geopandas as gpd
import os

class SpatialProcessor:
    def __init__(self, processed_path='../data/processed', shapefile_path='../data/shapefiles'):
        self.processed_path = processed_path
        self.shapefile_path = shapefile_path
        os.makedirs(self.shapefile_path, exist_ok=True)

    def fetch_map_data(self, state, year=2020):
        """
        Downloads municipality boundaries for a specific state using geobr.
        Year 2020 is a stable version for IBGE codes.
        """
        print(f"[*] Fetching geographic boundaries for {state}...")
        try:
            # geobr returns a GeoDataFrame automatically
            gdf = geobr.read_municipality(code_muni=state, year=year)
            return gdf
        except Exception as e:
            print(f"[-] Error fetching map data: {e}")
            return None

    def perform_spatial_join(self, state, year, month):
        """
        Joins Health Data (6-digit IBGE) with Map Data (7-digit IBGE).
        """
        # 1. Load Processed Health Data
        health_file = os.path.join(self.processed_path, f"final_processed_{state}_{year}_{month}.parquet")
        if not os.path.exists(health_file):
            print("[-] Processed health file not found. Run preprocessing first.")
            return
        
        df_health = pd.read_parquet(health_file)

        # 2. Get Map Data
        gdf_map = self.fetch_map_data(state)
        
        # 3. Handle IBGE Codes
        # Health data (MUNIC_RES) is 6 digits. Map data (code_muni) is 7 digits.
        # We'll create a 6-digit version of the map code for the join.
        gdf_map['code_muni_6'] = gdf_map['code_muni'].astype(str).str[:6].astype(float)
        df_health['MUNIC_RES'] = df_health['MUNIC_RES'].astype(float)

        # 4. The Join (Attribute-based join on geographic shapes)
        gdf_final = gdf_map.merge(df_health, left_on='code_muni_6', right_on='MUNIC_RES', how='left')

        # Fill NaN for municipalities with no admissions (important for the map)
        gdf_final['total_admissions'] = gdf_final['total_admissions'].fillna(0)
        
        print(f"[+] Spatial Join completed for {state}!")
        return gdf_final

    def save_geopackage(self, gdf, filename):
        """
        Saves as GeoPackage (.gpkg) - More modern and professional than Shapefile.
        """
        output_path = os.path.join(self.shapefile_path, f"{filename}.gpkg")
        gdf.to_file(output_path, driver="GPKG")
        print(f"[+] GeoPackage saved at {output_path}")

if __name__ == "__main__":
    spatial = SpatialProcessor()
    # Execute the join for PR 2025
    gdf_result = spatial.perform_spatial_join('PR', 2025, [1,2,3,4,5,6,7,8,9,10,11,12])
    
    if gdf_result is not None:
        spatial.save_geopackage(gdf_result, "health_map_pr_2025")