import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error
import os

class EfficiencyAnalyzer:
    def __init__(self, data_path='../data/processed/final_processed_PR_2025_[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12].parquet'):
        self.df = pd.read_parquet(data_path)
        
    def identify_inefficiencies(self):
        # 1. Prepare Features (X) and Target (y)
        # We want to predict 'total_cost' based on infrastructure and volume
        features = ['total_admissions', 'facility_count', 'total_beds', 'avg_stay']
        X = self.df[features].fillna(0)
        y = self.df['total_cost'].fillna(0)

        # 2. Train a Baseline Model (Random Forest is good for non-linear residuals)
        model = RandomForestRegressor(n_estimators=100, random_state=42)
        model.fit(X, y)

        # 3. Predict and Calculate Residuals
        self.df['predicted_cost'] = model.predict(X)
        self.df['residual'] = self.df['total_cost'] - self.df['predicted_cost']
        
        # 4. Normalize Residual (Percentage deviation)
        # This shows HOW MUCH above the expected the city is
        self.df['efficiency_deviation_%'] = (self.df['residual'] / (self.df['predicted_cost'] + 1)) * 100

        # 5. Rank the most "Inefficient" (Higher positive residuals)
        inefficient_cities = self.df.sort_values(by='efficiency_deviation_%', ascending=False)
        
        # return inefficient_cities[['name_muni', 'total_cost', 'predicted_cost', 'efficiency_deviation_%']].head(10)
        return inefficient_cities[['MUNIC_RES', 'total_cost', 'predicted_cost', 'efficiency_deviation_%']].head(10)

if __name__ == "__main__":
    analyzer = EfficiencyAnalyzer()
    top_inefficient = analyzer.identify_inefficiencies()
    print("--- Top 10 Inefficient Municipalities (Cost Overruns) ---")
    print(top_inefficient)