import geopandas as gpd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import numpy as np
from pyproj import Geod
from shapely.geometry import Polygon
from geopy.distance import geodesic
from dotenv import load_dotenv
import os, pandas as pd
from geoalchemy2 import Geometry
from shapely.geometry import Point
geod = Geod(ellps="WGS84")
# Load environment variables from .env
load_dotenv()

# Function to fetch data from PostgreSQL and return as DataFrame
def connect_to_pg(dbname, user, password, host, port):
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    engine = create_engine(connection_string)
    return engine


def geodesic_buffer_geopy(point, radius_m, n_segs=64):
    #Returns a Shapely Polygon around a lon/lat Point, with radius in meters.
    lon, lat = point.x, point.y
    #print(f"lon={lon}, lat={lat}, radius_m={radius_m!r}")
    # generate bearings from 0 to 360°
    azimuths = np.linspace(0, 360, n_segs, endpoint=False)
    # compute all destination points at once
    bearings = np.linspace(0, 360, n_segs, endpoint=False)
    coords = []
    for b in bearings:
        # compute destination point at this bearing and distance
        dest = geodesic(meters=radius_m).destination((lat, lon), b)
        coords.append((dest.longitude, dest.latitude))

    # close the ring by repeating the first point
    coords.append(coords[0])
    return Polygon(coords)


#connection parameters PostgresSQL database
connection_params_pg = {
    'dbname': 'maritime_assets',
    'user': 'analyst_ddl',
    'password': quote_plus(os.getenv('PG_PASSWORD')),
    'host': "maritime-assets-db1-dev-geospatial.cluster-cinsmmsxwkgg.eu-west-1.rds.amazonaws.com",
    'port': '5432'
}
#connection parameters to Static database
connection_params_static = {
    'dbname': 'staticdata',
    'user': 'stselepis',
    'password': quote_plus(os.getenv('STATIC_PASSWORD')),
    'host': "staticdata.c1p9jbnfjvlx.eu-west-1.rds.amazonaws.com",
    'port': '5432'
}

# Define SQL query
zone_query = "select id, name, type, range, point from public.zone where type in ('port','anchorage','checkpoint','canal')"
berth_query = "select  id, name, type, port_id, range, point from public.berth"
installation_query = "select  id, type, status, name, port_id, point from public.installation"

# Create engine
engine_pg = connect_to_pg(**connection_params_pg)
engine_static = connect_to_pg(**connection_params_static)
print (engine_pg)
print (engine_static)


# Read query into GeoDataFrame  and set WGS84 as CRS
gdf_zone = gpd.read_postgis(zone_query, con=engine_static, geom_col='point')
gdf_zone.set_crs(epsg=4326, inplace=True)
gdf_berth = gpd.read_postgis(berth_query, con=engine_static, geom_col='point')
gdf_berth.set_crs(epsg=4326, inplace=True)
gdf_installation= gpd.read_postgis(installation_query, con=engine_static, geom_col='point')
gdf_installation.set_crs(epsg=4326, inplace=True)

# create column with circular geometry based on range field
#ports, create the geodesic polygons
gdf_zone['range'] = pd.to_numeric(gdf_zone['range'], errors='coerce')
gdf_zone = gdf_zone[gdf_zone['range'].gt(0) & gdf_zone['range'].notna()]
buffers = [
    geodesic_buffer_geopy(pt, r)
    for pt, r in zip(gdf_zone.geometry, gdf_zone['range'])
]
gdf_zone['polygon_geom'] = buffers
# switch the active geometry to your new buffer
gdf_zone.set_geometry('polygon_geom', inplace=True)
gdf_zone.set_crs(epsg=4326, inplace=True, allow_override=True)
print (gdf_zone.crs)


#berths
gdf_berth['range'] = pd.to_numeric(gdf_berth['range'], errors='coerce')
gdf_berth = gdf_berth[gdf_berth['range'].gt(0) & gdf_berth['range'].notna()]
buffers = [
    geodesic_buffer_geopy(pt, r)
    for pt, r in zip(gdf_berth.geometry, gdf_berth['range'])
]
gdf_berth['polygon_geom'] = buffers
# switch the active geometry to your new buffer
gdf_berth.set_geometry('polygon_geom', inplace=True)
gdf_berth.set_crs(epsg=4326, inplace=True, allow_override=True)
print (gdf_berth.crs)

# Define schema and table name
schema_name = "sandbox"
table_Port = "st_ports"
table_Berth = "st_berths"
table_installations = 'st_installations'


# Function to empty db
def empty_table(table_name):
    with engine_pg.begin() as conn:
        conn.execute(text(f'TRUNCATE TABLE sandbox."{table_name}";'))

#Create or Replace the tables  to PostgreSQL
####Port
empty_table(table_Port)

gdf_zone.to_postgis(table_Port, engine_pg, schema=schema_name, if_exists="append", index=False, dtype={
        'polygon_geom': Geometry(geometry_type='POLYGON', srid=4326)
    })
####Berth
empty_table(table_Berth)
gdf_berth.to_postgis(table_Berth, engine_pg, schema=schema_name, if_exists="append", index=False)
####Installations
#empty_table(table_installations)
#gdf_installation.to_postgis(table_installations, engine_pg, schema=schema_name, if_exists="append", index=False)
print("Tables successfully created/replaced in PostgreSQL!")