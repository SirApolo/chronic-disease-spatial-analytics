import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import root_mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

def train_and_log():
    print("[*] Configuring MLflow...")
    # Using the service name from docker-compose
    mlflow.set_tracking_uri("http://mlflow-server:5000")
    mlflow.set_experiment("Chronic Disease Efficiency")
    
    # Enable autologging but disable manual log to avoid duplication
    # Autolog will capture: params, model, and basic metrics
    mlflow.sklearn.autolog(log_models=True) 

    print("[*] Starting MLflow run...")

    with mlflow.start_run() as run:
        # 1. Load Data
        df = pd.read_parquet('../data/processed/final_features_PR.parquet')
        
        # 2. Define Features & Target
        features = ['admissions_per_100k', 'beds_per_1k', 'facility_count', 'log_population']
        X = df[features]
        y = df['total_cost']

        # 3. Train/Test Split (80/20)
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # 4. Model Definition
        n_estimators = 100
        max_depth = 5
        model = RandomForestRegressor(n_estimators=n_estimators, max_depth=max_depth, random_state=42)
        
        # 5. Train (Autolog happens here)
        model.fit(X_train, y_train)
        
        # 6. Evaluation on Test Set
        predictions = model.predict(X_test)
        rmse = root_mean_squared_error(y_test, predictions)
        r2 = r2_score(y_test, predictions)

        # 7. Manual Logging for Test Metrics (Autolog focus mostly on train)
        mlflow.log_metric("test_rmse", rmse)
        mlflow.log_metric("test_r2", r2)
        
        # 8. Artifact: Residual Analysis Plot
        plt.figure(figsize=(10,6))
        sns.scatterplot(x=y_test, y=(y_test - predictions))
        plt.axhline(0, color='red', linestyle='--')
        plt.xlabel("Actual Total Cost")
        plt.ylabel("Residuals (Actual - Predicted)")
        plt.title("Residual Analysis - Chronic Disease Costs (PR)")
        
        plot_path = "../plots/residuals_test.png"
        plt.savefig(plot_path)
        mlflow.log_artifact(plot_path)
        os.remove(plot_path) # Clean up local file

        print(f"Run ID: {run.info.run_id}")
        print(f"Test R2: {r2:.4f} | Test RMSE: {rmse:.2f}")

if __name__ == "__main__":
    train_and_log()