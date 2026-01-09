import pandas as pd
import numpy as np

class FeatureEngineer:
    def __init__(self):
        # Professional standard: use per 100,000 for health metrics
        self.POP_BASIS = 100000 

    def create_normalized_features(self, df):
        """
        Normalizes health data using population and state-wide benchmarks.
        """
        # Join with population data (crucial for normalization)
        # df = df.merge(df_population, left_on='MUNIC_RES', right_on='code_muni_6', how='inner')

        # 1. Prevalence Rate (Admissions per 100k)
        df['admissions_per_100k'] = (df['total_admissions'] / df['population']) * self.POP_BASIS

        # 2. Infrastructure Density (Beds per 1k)
        df['beds_per_1k'] = (df['total_beds'] / df['population']) * 1000

        # 3. Relative Cost Index (City Cost / State Mean Cost)
        state_avg_cost = df['avg_cost'].mean()
        df['relative_cost_index'] = df['avg_cost'] / state_avg_cost

        # 4. Stay-Cost Interaction
        # Does longer stay always mean higher cost in this city?
        df['cost_per_day'] = df['total_cost'] / (df['avg_stay'] * df['total_admissions'] + 1)

        return df

    def apply_scaling(self, df, columns_to_scale):
        """
        Applies Log Transformation to handle skewed healthcare costs.
        """
        for col in columns_to_scale:
            # Use log1p to handle zeros safely
            df[f'log_{col}'] = np.log1p(df[col])
        
        return df
    

if __name__ == "__main__":
    engineer = FeatureEngineer()
    