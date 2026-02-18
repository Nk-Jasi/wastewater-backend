from fastapi import FastAPI, Query, HTTPException, UploadFile, File, Depends
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from openlocationcode import openlocationcode as olc
from passlib.context import CryptContext
from jose import JWTError, jwt
from datetime import datetime, timedelta
import os

# -------------------------------
# App Setup
# -------------------------------
app = FastAPI(title="Wastewater GIS API v2")

# -------------------------------
# CORS Middleware for InfinityFree Frontend
# -------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://portfolio23.infinityfreeapp.com"],  # InfinityFree frontend URL
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
# JWT Auth Setup
# -------------------------------
SECRET_KEY = "replace_with_a_secure_random_key"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 120
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# -------------------------------
# Helper: Password & Token
# -------------------------------
def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password):
    return pwd_context.hash(password)

def authenticate_user(email: str, password: str):
    query = text("SELECT * FROM users WHERE email = :email")
    user = engine.execute(query, {"email": email}).fetchone()
    if not user or not verify_password(password, user.password_hash):
        return False
    return user

def create_access_token(data: dict, expires_delta=None):
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta if expires_delta else timedelta(minutes=15))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(status_code=401, detail="Could not validate credentials")
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        email = payload.get("sub")
        if email is None:
            raise credentials_exception
        user = engine.execute(text("SELECT * FROM users WHERE email = :email"), {"email": email}).fetchone()
        if user is None:
            raise credentials_exception
        return user
    except JWTError:
        raise credentials_exception

# -------------------------------
# Login / Token endpoint
# -------------------------------
@app.post("/token")
def login(form_data: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    access_token = create_access_token(
        data={"sub": user.email},
        expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    )
    return {"access_token": access_token, "token_type": "bearer"}

# -------------------------------
# Helper: Generate Plus Code
# -------------------------------
def get_plus_code(geom):
    if geom is None:
        return None
    return olc.encode(geom.y, geom.x)

# -------------------------------
# 1. Get All Manholes
# -------------------------------
@app.get("/manholes")
def get_manholes(current_user: dict = Depends(get_current_user)):
    try:
        gdf = gpd.read_postgis("SELECT * FROM waste_water_manhole", engine, geom_col='geom')
        gdf['plus_code'] = gdf['geom'].apply(get_plus_code)
        return gdf.to_json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------------------
# 2. Get All Pipes
# -------------------------------
@app.get("/pipes")
def get_pipes(current_user: dict = Depends(get_current_user)):
    try:
        gdf = gpd.read_postgis("SELECT * FROM waste_water_pipeline", engine, geom_col='geom')
        gdf['plus_code'] = gdf['geom'].apply(lambda line: get_plus_code(line.interpolate(0.5, normalized=True)))
        return gdf.to_json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------------------
# 3. Maintenance Records
# -------------------------------
@app.get("/maintenance/{asset_type}/{asset_id}")
def get_maintenance(asset_type: str, asset_id: str, current_user: dict = Depends(get_current_user)):
    try:
        query = text("""
            SELECT *
            FROM maintenance
            WHERE asset_type = :asset_type
              AND asset_id = :asset_id
        """)
        df = pd.read_sql(query, engine, params={"asset_type": asset_type.upper(), "asset_id": asset_id})
        return df.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------------------
# 4. Manhole Radius Search
# -------------------------------
@app.get("/manholes/search")
def search_manholes(
    lat: float = Query(..., description="Latitude of center point"),
    lon: float = Query(..., description="Longitude of center point"),
    radius: float = Query(500, description="Search radius in meters"),
    current_user: dict = Depends(get_current_user)
):
    try:
        point_wkt = Point(lon, lat).wkt
        query = text("""
            SELECT *, ST_AsText(geom) as geom_wkt
            FROM waste_water_manhole
            WHERE ST_DWithin(
                geom::geography,
                ST_GeomFromText(:point_wkt, 4326)::geography,
                :radius
            )
        """)
        gdf = gpd.read_postgis(query, engine, geom_col='geom', params={"point_wkt": point_wkt, "radius": radius})
        gdf['plus_code'] = gdf['geom'].apply(get_plus_code)
        return gdf.to_json()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------------------
# 5. Favorites / Bookmarks
# -------------------------------
@app.post("/favorites")
def add_favorite(user_id: str, asset_type: str, asset_id: str, current_user: dict = Depends(get_current_user)):
    try:
        engine.execute(text("""
            INSERT INTO favorites(user_id, asset_type, asset_id)
            VALUES(:user_id, :asset_type, :asset_id)
            ON CONFLICT DO NOTHING
        """), {"user_id": user_id, "asset_type": asset_type.upper(), "asset_id": asset_id})
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/favorites/{user_id}")
def get_favorites(user_id: str, current_user: dict = Depends(get_current_user)):
    try:
        df = pd.read_sql(text("SELECT * FROM favorites WHERE user_id = :user_id"), engine, params={"user_id": user_id})
        return df.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------------------
# 6. Dashboard Statistics
# -------------------------------
@app.get("/dashboard/stats")
def dashboard_stats(current_user: dict = Depends(get_current_user)):
    try:
        query = """
            SELECT suburb, 
                   COUNT(*) AS total_manholes, 
                   SUM(CASE WHEN status='Needs Maintenance' THEN 1 ELSE 0 END) AS needs_maintenance
            FROM waste_water_manhole
            GROUP BY suburb
        """
        df = pd.read_sql(query, engine)
        return df.to_dict(orient='records')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------------------
# 7. Image Upload for Assets
# -------------------------------
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

@app.post("/assets/{asset_id}/upload")
def upload_image(asset_id: str, file: UploadFile = File(...), current_user: dict = Depends(get_current_user)):
    try:
        filepath = os.path.join(UPLOAD_DIR, f"{asset_id}_{file.filename}")
        with open(filepath, "wb") as f:
            f.write(file.file.read())
        engine.execute(text("""
            UPDATE waste_water_manhole
            SET image_path = :path
            WHERE gid = :asset_id
        """), {"path": filepath, "asset_id": asset_id})
        return {"status": "success", "file_path": filepath}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------------------
# 8. Update / Edit Manholes
# -------------------------------
@app.put("/manholes/{asset_id}")
def update_manhole(asset_id: int, data: dict, current_user: dict = Depends(get_current_user)):
    try:
        query = text("""
            UPDATE waste_water_manhole
            SET status = :status,
                notes = :notes
            WHERE gid = :asset_id
        """)
        engine.execute(query, {
            "status": data.get("status"),
            "notes": data.get("notes"),
            "asset_id": asset_id
        })
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
