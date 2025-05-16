import geopandas as gpd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import numpy as np
from pyproj import Geod
from shapely.geometry import Polygon
geod = Geod(ellps="WGS84")


# Function to fetch data from PostgreSQL and return as DataFrame
def connect_to_pg(dbname, user, password, host, port):
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    engine = create_engine(connection_string)
    return engine


def geodesic_buffer(point, radius_m, n_segs=64):
    #Returns a Shapely Polygon around a lon/lat Point, with radius in meters.
    lon, lat = point.x, point.y
    # generate bearings from 0 to 360°
    azimuths = np.linspace(0, 360, n_segs)
    # compute all destination points at once
    lons, lats, _ = geod.fwd(
        np.full(n_segs, lon),
        np.full(n_segs, lat),
        azimuths,
        np.full(n_segs, radius_m)
    )
    return Polygon(zip(lons, lats))


#connection parameters PostgresSQL database
connection_params_pg = {
    'dbname': 'maritime_assets',
    'user': 'analyst_ddl',
    'password': quote_plus(''),
    'host': "maritime-assets-db1-dev-geospatial.cluster-cinsmmsxwkgg.eu-west-1.rds.amazonaws.com",
    'port': '5432'
}
#connection parameters to Static database
connection_params_static = {
    'dbname': 'staticdata',
    'user': 'stselepis',
    'password': quote_plus(''),
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

# create column with circular geometry based on range field convert to 3857 and then back to 4326
#ports, create the geodesic polygons
gdf_zone['polygon_geom'] = gdf_zone.apply(
    lambda row: geodesic_buffer(row.geometry, row['range']),
    axis=1
)
# switch the active geometry to your new buffer
gdf_zone = gdf_zone.set_geometry('polygon_geom')
#berths
gdf_berth = gdf_berth.to_crs(epsg=3857)  # Web Mercator (units in meters)
gdf_berth['polygon_geom'] = gdf_berth.geometry.buffer(gdf_berth['range'])
gdf_berth = gdf_berth.set_geometry('polygon_geom').to_crs(epsg=4326)


# Define schema and table name
schema_name = "sandbox"
table_Port = "st_ports"
table_Berth = "st_berths"
table_installations = 'st_installations'


# Function to empty db
def empty_table(table_name):
    with engine_pg.begin() as conn:
        conn.execute(text(f'TRUNCATE TABLE sandbox."{table_name}";'))
print ("Testing")
#Create or Replace the tables  to PostgreSQL
####Port
#empty_table(table_Port)
gdf_zone.to_postgis(table_Port, engine_pg, schema=schema_name, if_exists="append", index=False)
####Berth
#empty_table(table_Berth)
gdf_berth.to_postgis(table_Berth, engine_pg, schema=schema_name, if_exists="append", index=False)
####Installations
#empty_table(table_installations)
gdf_installation.to_postgis(table_installations, engine_pg, schema=schema_name, if_exists="append", index=False)
print("Tables successfully created/replaced in PostgreSQL!")