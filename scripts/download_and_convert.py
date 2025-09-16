from cdse_odata import FileDownloader
from make_10m_mosaic import resample_mosaic
from multiprocessing import Pool
import os
from fastcore.script import *
from shutil import rmtree
from itertools import cycle
import time
from pathlib import Path


def run_chain(product_name, downloader, dl_path, outpath, save_scl):
    outfile = f'{product_name.replace("SAFE", "tif")}'
    num_downloads = 0
    if not os.path.exists(f'{outpath}/{outfile}'):
        while num_downloads < 5:
            try:
                downloader.refresh_token()
                downloader.query_product_by_name(name=product_name)
                downloader.download_latest_response(target_path=dl_path)

            except:
                num_downloads += 1
                print(f'Error while processing {product_name}, retry {num_downloads}/5')
                time.sleep(15) # Wait for 15 seconds
            else:
                break
        resample_mosaic(input=Path(f'{dl_path}/{product_name}'), 
                        outfile=Path(f'{outpath}/{outfile}'),
                        save_scl=save_scl)
        rmtree(f'{dl_path}/{product_name}')
    else: print(f'{outfile} exist, skipping...')
    return

@call_parse
def download_and_convert(
    product_name_file:Path, # Path to txt file containing the product ids
    creds_file:Path, # Path to credential files
    dl_path:Path, # tmp path to download to
    outpath:Path, # Path to output mosaic folder
    save_scl:bool=False, # Whether to save SCL to mosaics
):
    "Wrapper to both download and convert data with multiprocessing"

    with open(product_name_file) as f:
        lines = [line.rstrip() for line in f]

    downloaders = [FileDownloader(username=None, password=None, creds_file=creds_file) for _ in range(4)]

    inps = [(product_name, 
            downloader, 
            dl_path, 
            outpath,
            save_scl) for product_name, downloader in zip(lines, cycle(downloaders))]
    print(f'{len(inps)} products to download')
    with Pool(4) as pool:
        pool.starmap(run_chain, inps)
