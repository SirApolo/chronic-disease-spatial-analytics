import streamlit as st
import pandas as pd
import geopandas as gpd
import plotly.express as px
import pickle
import numpy as np

# 1. Carregamento do Modelo e Dados
@st.cache_resource # cache_resource é para objetos como modelos de ML
def load_ml_model():
    # Aponte para onde o MLflow ou você salvou o .pkl
    with open("models/model.pkl", "rb") as f:
        model = pickle.load(f)
    return model

@st.cache_data
def load_geo_data():
    gdf = gpd.read_file("data/shapefiles/health_map_pr_2025.gpkg")
    return gdf

# --- Execução ---
gdf = load_geo_data()
model = load_ml_model()

# 2. Gerando a coluna de Predição "ao vivo"
# Definimos as mesmas features usadas no treinamento
features = ['admissions_per_100k', 'beds_per_1k', 'facility_count', 'log_population']

# Criamos a coluna de predição no GeoDataFrame
# Fillna(0) para garantir que cidades sem dados não quebrem o modelo
gdf['predicted_cost'] = model.predict(gdf[features].fillna(0))

# 3. Calculando a Eficiência (Resíduo %)
# Reutilizamos a lógica do '+1' para estabilidade numérica
gdf['efficiency_gap'] = ((gdf['total_cost'] - gdf['predicted_cost']) / (gdf['predicted_cost'] + 1)) * 100

# 4. Visualização no Streamlit
st.subheader("📊 Model Insight: Real vs Predicted Costs")

# Criando duas colunas para comparar os mapas
# map_col1, map_col2 = st.columns(2)

# with map_col1:
#     st.markdown("**Actual Total Cost**")
#     fig_real = px.choropleth_mapbox(gdf, geojson=gdf.geometry, locations=gdf.index,
#                                     color="total_cost", mapbox_style="carto-positron",
#                                     center={"lat": -24.8, "lon": -51.5}, zoom=5, opacity=0.5)
#     st.plotly_chart(fig_real, use_container_width=True)

st.markdown("**Actual Total Cost**")
fig_real = px.choropleth_mapbox(gdf, geojson=gdf.geometry, locations=gdf.index,
                                color="total_cost", mapbox_style="carto-positron",
                                center={"lat": -24.8, "lon": -51.5}, zoom=5, opacity=0.5,
                                hover_name="name_muni")
st.plotly_chart(fig_real, use_container_width=True)

# with map_col2:
#     st.markdown("**Predicted Cost (ML Model)**")
#     fig_pred = px.choropleth_mapbox(gdf, geojson=gdf.geometry, locations=gdf.index,
#                                     color="predicted_cost", mapbox_style="carto-positron",
#                                     center={"lat": -24.8, "lon": -51.5}, zoom=5, opacity=0.5)
#     st.plotly_chart(fig_pred, use_container_width=True)

st.markdown("**Predicted Cost (ML Model)**")
fig_pred = px.choropleth_mapbox(gdf, geojson=gdf.geometry, locations=gdf.index,
                                color="predicted_cost", mapbox_style="carto-positron",
                                center={"lat": -24.8, "lon": -51.5}, zoom=5, opacity=0.5,
                                hover_name="name_muni")
st.plotly_chart(fig_pred, use_container_width=True)

# 5. Tabela de Municípios "Ineficientes"
st.subheader("🚨 Top Efficiency Gaps (Potential Overspending)")
high_gap = gdf.sort_values(by='efficiency_gap', ascending=False).head(30)
st.dataframe(high_gap[['name_muni', 'total_cost', 'predicted_cost', 'efficiency_gap']],
             column_config={
                 'name_muni': st.column_config.TextColumn("Municipality"),
                 'total_cost': st.column_config.NumberColumn("Actual Cost", format="dollar"),
                 'predicted_cost': st.column_config.NumberColumn("Predicted Cost", format="dollar"),
                 'efficiency_gap': st.column_config.NumberColumn("Efficiency Gap (%)", format="%.2f %%")
             })