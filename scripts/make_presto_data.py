import xarray as xr
import rioxarray as rxr
import geopandas as gpd
import pandas as pd
from pyproj import Transformer

from pathlib import Path
import os
import numpy as np

from warnings import simplefilter
simplefilter(action="ignore", category=pd.errors.PerformanceWarning)

# Read gpd data
aoi = gpd.read_file('../data/AOI.geojson').to_crs('epsg:3067')
plots = gpd.read_file('../data/koealat_kaikki3101.shp', bbox=tuple(aoi.total_bounds)).to_crs('epsg:3067')
roadpoints = gpd.read_file('../data/mtkpisteet.gpkg', layer='tiet')
roadpoints = roadpoints[roadpoints.kohdeluokka.isin([12111,12112,12121,12122])].copy()
buildings = gpd.read_file('../data/mtkpisteet.gpkg', layer='rakennus')

print(f'Total of {len(plots)} field plots located in AOI')

# Wrangle data to look nicer

plots = plots[['Inventoint', 'InvLK', 'geometry']]
plots.loc[plots.Inventoint == '430','Inventoint'] = '430 Järvi tai lampi'
plots['Inventoint'] = plots.Inventoint.apply(lambda row: row[4:])
plots.rename(columns={'Inventoint': 'label'}, inplace=True)

# Sample 100 road and building points to the data
print('Sampling roads and buildings')

sample_roads = roadpoints.sample(100, random_state=66)[['kohdeluokka', 'geometry']]
sample_buildings = buildings.sample(100, random_state=66)[['kohdeluokka', 'geometry']]

sample_roads['label'] = 'Tie'
sample_buildings['label'] = 'Rakennus'
sample_roads.rename(columns={'kohdeluokka': 'InvLK'}, inplace=True)
sample_buildings.rename(columns={'kohdeluokka': 'InvLK'}, inplace=True)

sample_buildings = sample_buildings.to_crs('epsg:3067')
sample_roads = sample_roads.to_crs('epsg:3067')
df = pd.concat((plots, sample_buildings, sample_roads))

# Sample DTM values

print('Adding height and slope')

xs = xr.DataArray(df.geometry.x.to_numpy(), dims='points', coords={'points': np.arange(len(df))})
ys = xr.DataArray(df.geometry.y.to_numpy(), dims='points', coords={'points': np.arange(len(df))})

dtm = xr.open_dataset('../data/dtm.zarr')
elevations = dtm['elevation'].sel(x=xs, y=ys, method='nearest')
slopes = dtm['slope'].sel(x=xs, y=ys, method='nearest')

df['elevation'] = elevations.values
df['slope'] = slopes.values

# Sample temperature and precipitation values

print('Adding precipitation and temperature')

fmi_data = xr.open_dataset('../data/fmi.zarr').sel(x=xs, y=ys, method='nearest')

for t in ['precipitation', 'temperature']:
    for y in fmi_data.year.values:
        for m in fmi_data.month.values:
            df[f'{t}_{y}_{m}'] = fmi_data[t].sel({'year': y, 'month': m})


# Sample S2 data, they be in epsg:32635

xs_s2 = xr.DataArray(df.to_crs('epsg:32635').geometry.x.to_numpy(), dims='points', coords={'points': np.arange(len(df))})
ys_s2 = xr.DataArray(df.to_crs('epsg:32635').geometry.y.to_numpy(), dims='points', coords={'points': np.arange(len(df))})

months = [1,2,3,4,5,6,7,8,9,10,11,12]

print('Adding S2 data from 2020')
s2_2020 = xr.open_dataset('../s2-data/medians/2020/35WNT.zarr').sel(x=xs_s2, y=ys_s2, method='nearest')
for b in s2_2020.data_vars:
    for m in months:
        if m not in s2_2020.month.values: df[f'{b}_2020_{m}'] = np.nan
        else: df[f'{b}_2020_{m}'] = s2_2020[b].sel({'month': m})

print('Adding S2 data from 2021')
s2_2021 = xr.open_dataset('../s2-data/medians/2021/35WNT.zarr').sel(x=xs_s2, y=ys_s2, method='nearest')
for b in s2_2021.data_vars:
    for m in months:
        if m not in s2_2021.month.values: df[f'{b}_2021_{m}'] = np.nan
        else: df[f'{b}_2021_{m}'] = s2_2021[b].sel({'month': m})

print('Adding S2 data from 2022')
s2_2022 = xr.open_dataset('../s2-data/medians/2022/35WNT.zarr').sel(x=xs_s2, y=ys_s2, method='nearest')
for b in s2_2022.data_vars:
    for m in months:
        if m not in s2_2022.month.values: df[f'{b}_2022_{m}'] = np.nan
        else: df[f'{b}_2022_{m}'] = s2_2022[b].sel({'month': m})


df.to_file('../data/sampled_data.gpkg')
