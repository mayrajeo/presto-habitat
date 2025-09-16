import numpy as np 
import glob
import rasterio as rio
from rasterio.enums import Resampling
import os
from pathlib import Path

from fastcore.script import *


def resample_mosaic(input:Path, # Input SAFE directory root
                    outfile:Path, # Path to output file
                    save_scl:bool=False, # Whether to save SCL as the last band, only valid for L2A data
                    ): 
    "Create a 10 band image from 10, 20 and 60m bands"
    print(f'Converting {input}')
    processing_level = input.name.split('_')[1]
    print(processing_level)
    match processing_level:
        case 'MSIL1C':
            bands = 13
            xmlroot = glob.glob(os.path.join(input, f'MTD_{processing_level}.xml'))[0]

            print('Reading data')
            with rio.open(xmlroot, 'r') as src:
                data_10m = src.subdatasets[0]
                data_20m = src.subdatasets[1]
                data_60m = src.subdatasets[2]

            # Read 10m bands
            with rio.open(data_10m, 'r') as src:
                B04 = src.read(1)
                B03 = src.read(2)
                B02 = src.read(3)
                B08 = src.read(4)
                prof = src.profile

            # Read 20m bands:
            with rio.open(data_20m, 'r') as src:
                B05 = src.read(1, out_shape=(1, int(src.height * 2), int(src.width*2)),
                            resampling=Resampling.bilinear)
                B06 = src.read(2, out_shape=(1, int(src.height * 2), int(src.width*2)),
                            resampling=Resampling.bilinear)
                B07 = src.read(3, out_shape=(1, int(src.height * 2), int(src.width*2)),
                            resampling=Resampling.bilinear)
                B08A = src.read(4, out_shape=(1, int(src.height * 2), int(src.width*2)),
                            resampling=Resampling.bilinear)
                B11 = src.read(5, out_shape=(1, int(src.height * 2), int(src.width*2)),
                            resampling=Resampling.bilinear)
                B12 = src.read(6, out_shape=(1, int(src.height * 2), int(src.width*2)),
                            resampling=Resampling.bilinear)

            # Read 60m bands
            with rio.open(data_60m, 'r') as src:
                B01 = src.read(1, out_shape=(1, B02.shape[0], B02.shape[1]), resampling=Resampling.nearest)
                B09 = src.read(2, out_shape=(1, B02.shape[0], B02.shape[1]), resampling=Resampling.nearest)
                B10 = src.read(3, out_shape=(1, B02.shape[0], B02.shape[1]), resampling=Resampling.nearest)

            image = np.stack([B01, B02, B03, B04, B05, B06, B07, B08, B08A, B09, B10, B11, B12])

        case 'MSIL2A':
            bands = 12
            xmlroot = glob.glob(os.path.join(input, f'MTD_{processing_level}.xml'))[0]

            print('Reading data')
            with rio.open(xmlroot, 'r') as src:
                data_10m = src.subdatasets[0]
                data_20m = src.subdatasets[1]
                data_60m = src.subdatasets[2]

            # Read 10m bands
            with rio.open(data_10m, 'r') as src:
                B04 = src.read(1)
                B03 = src.read(2)
                B02 = src.read(3)
                B08 = src.read(4)
                prof = src.profile

            # Read 20m bands:
            with rio.open(data_20m, 'r') as src:
                B05 = src.read(1, out_shape=(1, int(src.height * 2), int(src.width*2)),
                            resampling=Resampling.bilinear)
                B06 = src.read(2, out_shape=(1, int(src.height * 2), int(src.width*2)),
                            resampling=Resampling.bilinear)
                B07 = src.read(3, out_shape=(1, int(src.height * 2), int(src.width*2)),
                            resampling=Resampling.bilinear)
                B08A = src.read(4, out_shape=(1, int(src.height * 2), int(src.width*2)),
                            resampling=Resampling.bilinear)
                B11 = src.read(5, out_shape=(1, int(src.height * 2), int(src.width*2)),
                            resampling=Resampling.bilinear)
                B12 = src.read(6, out_shape=(1, int(src.height * 2), int(src.width*2)),
                            resampling=Resampling.bilinear)
                if save_scl:
                    SCL = src.read(9, out_shape=(1, int(src.height * 2), int(src.width*2)),
                            resampling=Resampling.bilinear)

            # Read 60m bands
            with rio.open(data_60m, 'r') as src:
                B01 = src.read(1, out_shape=(1, B02.shape[0], B02.shape[1]), resampling=Resampling.nearest)
                B09 = src.read(2, out_shape=(1, B02.shape[0], B02.shape[1]), resampling=Resampling.nearest)

            if save_scl:
                bands += 1
                image = np.stack([B01, B02, B03, B04, B05, B06, B07, B08, B08A, B09, B11, B12, SCL])

            else:
                image = np.stack([B01, B02, B03, B04, B05, B06, B07, B08, B08A, B09, B11, B12])

    prof.update(
        count=bands, 
        driver='GTiff', 
        compress='deflate',
        predictor=2,
        BIGTIFF='YES'
    )

    with rio.open(outfile, 'w', **prof) as dst:
        dst.write(image)

@call_parse
def make_10m_mosaic(input:Path, # Input SAFE directory root
                    outfile:Path, # Path to output file
                    save_scl:bool=False, # Whether to save SCL as the last band, only valid for L2A data
                    ): 
    "Create a 10 band image from 10, 20 and 60m bands"
    resample_mosaic(input, outfile, save_scl)