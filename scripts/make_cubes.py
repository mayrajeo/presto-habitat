from pathlib import Path 
import rioxarray as rxr
import xarray as xr
import os
import pandas as pd

from fastcore.script import *

BAND_NAMES = ['B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 
              'B08', 'B8A', 'B09', 'B11', 'B12', 'SCL']

@call_parse
def make_monthly_medians(
        datapath:Path, # Location for processed medians
        outpath:Path, # Where to save the final datacubes
):
    "Convert L2A data to monthly medians and save them to a datacube"

    files = [f for f in os.listdir(datapath) if f.endswith('tif')]
    arrays = []
    dates = []
    files.sort(key=lambda x: x.split('_')[2].split('T')[0])
    for f in files: 
        date = f.split('_')[2].split('T')[0]
        dates.append(pd.to_datetime(date))
        da = rxr.open_rasterio(datapath/f, chunks={'x': 1024, 'y': 1024})
        da = da.assign_coords(band=BAND_NAMES)
        mask = da.sel(band='SCL')
        da = da.where(~mask.isin([0,1,8,9,10]))
        da = da.sel(band=da.band.values[:-1])
        arrays.append(da)
    cube = xr.concat(arrays, dim='time').assign_coords(time=dates)
    cube = cube.chunk({'time': -1, 'band': -1, 'y': 1024, 'x': 1024})
    monthly = cube.groupby('time.month').median(skipna=True)
    monthly = monthly.to_dataset(dim='band')
    monthly.to_zarr(outpath, mode='w')