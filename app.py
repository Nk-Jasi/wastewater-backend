from fastapi import FastAPI, Query, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from openlocationcode import openlocationcode as olc
import os

# -------------------------------
# App Setup
# -------------------------------
app = FastAPI(title="Wastewater GIS API v2")

# -------------------------------
# CORS Middleware for frontend
# -------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://portfolio23.infinityfreeapp.com"],  # frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------------
# Database connection (Render DB)
# -------------------------------
DB_URL = "postgresql://blue_whsc_user:UyXWzfhOFyMxmUckWi2CQWYWS3DQSDfe@dpg-d6a0v8o6fj8s73crhp6g-a.oregon-postgres.render.com:5432/blue_whsc"
engine = create_engine(DB_URL)

# -------------------------------
# Helper: Generate Plus Code
# -------------------------------
def get_plus_code(geom):
    if geom is None:
        return None
    return olc.encode(geom.y, geom.x)

# -------------------------------
# Upload directory
# -------------------------------
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

# -------------------------------
# Root route to confirm API is live
# -------------------------------
@app.get("/")
def root():
    return {"message": "Wastewater GIS API is live!"}

# -------------------------------
# Manholes endpoint
# -------------------------------
@app.get("/manholes")
def get_manholes():
    try:
        gdf = gpd.read_postgis("SELECT * FROM waste_water_manhole", engine, geom_col='geom')
        gdf['plus_code'] = gdf['geom'].apply(get_plus_code)
        return gdf.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------------------
# Pipes endpoint
# -------------------------------
@app.get("/pipes")
def get_pipes():
    try:
        gdf = gpd.read_postgis("SELECT * FROM waste_water_pipeline", engine, geom_col='geom')
        gdf['plus_code'] = gdf['geom'].apply(lambda line: get_plus_code(line.interpolate(0.5, normalized=True)))
        return gdf.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------------------
# All data combined endpoint
# -------------------------------
@app.get("/all-data")
def get_all_data(user_id: str = None):
    result = {}
    try:
        # Manholes
        gdf_manholes = gpd.read_postgis("SELECT * FROM waste_water_manhole", engine, geom_col='geom')
        gdf_manholes['plus_code'] = gdf_manholes['geom'].apply(get_plus_code)
        result['manholes'] = gdf_manholes.to_dict(orient='records')
    except Exception as e:
        result['manholes_error'] = str(e)

    try:
        # Pipes
        gdf_pipes = gpd.read_postgis("SELECT * FROM waste_water_pipeline", engine, geom_col='geom')
        gdf_pipes['plus_code'] = gdf_pipes['geom'].apply(lambda line: get_plus_code(line.interpolate(0.5, normalized=True)))
        result['pipes'] = gdf_pipes.to_dict(orient='records')
    except Exception as e:
        result['pipes_error'] = str(e)

    try:
        # Favorites
        if user_id:
            df_favorites = pd.read_sql(text("SELECT * FROM favorites WHERE user_id = :user_id"), engine, params={"user_id": user_id})
            result['favorites'] = df_favorites.to_dict(orient='records')
        else:
            result['favorites'] = []
    except Exception as e:
        result['favorites_error'] = str(e)

    try:
        # Dashboard stats
        query_stats = """
            SELECT suburb, 
                   COUNT(*) AS total_manholes, 
                   SUM(CASE WHEN status='Needs Maintenance' THEN 1 ELSE 0 END) AS needs_maintenance
            FROM waste_water_manhole
            GROUP BY suburb
        """
        df_stats = pd.read_sql(query_stats, engine)
        result['dashboard_stats'] = df_stats.to_dict(orient='records')
    except Exception as e:
        result['dashboard_stats_error'] = str(e)

    return result


