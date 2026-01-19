# 🏥 Chronic Disease Efficiency Analytics: Paraná Case Study

This project delivers a full-stack **Machine Learning and Geospatial (GIS)** pipeline designed to identify financial and operational inefficiencies in chronic disease management (Diabetes and Hypertension) across all 399 municipalities of Paraná, Brazil.

By integrating legacy healthcare data (**DATASUS**) with high-resolution demographic data (**2022 IBGE Census**), the system predicts expected hospital costs and highlights "Efficiency Gaps"—regions where actual spending significantly exceeds benchmarks based on local infrastructure and population.

## 🛠 Tech Stack

* **Data Ingestion:** `PySUS` for DATASUS (FTP) and `Sidrapy` for IBGE (Official 2022 Census API).
* **GIS & Spatial Analysis:** `GeoPandas` for handling municipality boundaries, spatial joins, and thematic mapping.
* **ML Engineering:** `Scikit-Learn` (RandomForest Regressor) for cost prediction and residual analysis.
* **MLOps:** `MLflow` for experiment tracking, model versioning, and schema enforcement.
* **App/Visualization:** `Streamlit` for interactive GIS dashboards and `Plotly` for dynamic charts.
* **Environment:** `Docker` & `Docker-compose` for reproducibility.

## 📈 Key Insights & Business Value

* **Cost Drivers:** Analysis revealed that **Average Hospital Stay** (`avg_stay`) has a higher impact on total costs ($r = 0.83$) than raw admission volume alone
* **Infrastructure Pressure:** Identified "Healthcare Deserts" where high admission rates coincide with low bed density per capita.
* **Anomaly Detection:** Used **Residual Analysis** ($Actual - Predicted$) to pinpoint municipalities with >20% unexplained cost overruns, providing actionable targets for health insurance audits.
* **Fair Comparison:** Implementação de normalização baseada na população (métricas por 100 mil habitantes) para remover o viés de grandes centros urbanos como Curitiba.
* **Fair Comparison:** Implemented population-based normalization (metrics per 100k inhabitants) to remove the bias of large urban centers like Curitiba.

---

## 📂 Project Structure

```plaintext
├── app/
│   └── Dockerfile           # Streamlit specific container definition and environment setup
│   └── main.py              # Streamlit Interactive Dashboard
├── data/
│   ├── processed/           # Cleaned data with engineered features
│   ├── raw/                 # Original DATASUS .parquet files
│   └── shapefiles/          # GeoPackage (.gpkg) for GIS
├── notebooks/
│   ├── 01_EDA_SIH.ipynb     # Exploratory Data Analysis and initial SIH spatial mapping
│   └── 02_EDA_SIDRA.ipynb   # Sidrapy API testing and 2022 Census data validation
├── src/
│   ├── data_ingestion.py    # Multi-source data extraction (FTP + API)
│   ├── preprocessing.py     # Cleaning, CID filtering, and Join with Census 2022
│   ├── spatial_analysis.py  # Spatial joins and GeoPackage generation for GIS mapping
│   └── train_mlflow.py      # Model training and Experiment tracking
├── .env                     # Environment variables for sensitive configurations
├── .gitattributes           # Git LFS and path-specific settings
├── .gitignore               # Specification of intentionally untracked files
├── docker-compose.yml       # Orchestration for App + MLflow
├── Dockerfile               # Environment definition
├── LICENSE                  # Project legal licensing (MIT)
└── README.md                # Technical and business documentation
```

## 🚀 How to Run

1. Requirements
* Docker & Docker Compose installed.

2. Setup & Execution
Clone the repository and run:
```Bash
docker-compose up --build
```
* **Streamlit App**: http://localhost:8501
* **MLflow UI**: http://localhost:5000

3. Pipeline Flow
* The project follows a modular execution:
    1. **Ingestion**: Downloads SIH/SUS and CNES data.
    2. **Preprocessing**: Merges healthcare records with Sidrapy (Censo 2022).
    3. **Training**: Trains the model and logs artifacts (Residual plots, metrics) to MLflow.
    4. **Inference**: The Streamlit app loads the .pkl model and performs Live Inference on the geographic map.3.

## ⚖️ License

Distributed under the MIT License. See LICENSE for more information.

