import os, re, ast
import geojson, boto3, rasterio, pystac
import rasterio.transform

import numpy as np
import pandas as pd
import geopandas as gpd

from rasterio.profiles import DefaultGTiffProfile
from datetime import datetime
from pandas import Timestamp
from pystac import Catalog
from shapely.wkt import loads
from shapely.geometry import shape, Point, Polygon
from GOSTRocks.misc import tPrint
from botocore.config import Config
from botocore import UNSIGNED


def search_aws(s3client, bucket, endswith='', prefix=''):
    ''' Search AWS bucket for all files matching pattern
    
    Args:
        s3client [boto3 client] - created from boto3.client('s3', region_name=region, config=Config(signature_version=UNSIGNED))
        bucket [string] - aws s3 bucket to search
        endswith [optional, string] - filter to 
    '''
    # Loop through the S3 bucket and get all the keys for files that are .tif 
    more_results = True
    try:
        del(token)
    except:
        pass
    loops = 0
    good_file = []    
    while more_results:
        print(f"Completed loop: {loops}")
        if loops > 0:
            objects = s3client.list_objects_v2(Bucket=bucket, Prefix=prefix, ContinuationToken=token)
        else:
            objects = s3client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        more_results = objects['IsTruncated']
        if more_results:
            token = objects['NextContinuationToken']
        loops += 1
        for res in objects['Contents']:
            if res['Key'].endswith(endswith):
                good_file.append(res)
    return(good_file)
    
class v_file():
    def __init__(self, v_files, prefix):
        '''
        
        Args:
            v_files [list of strings] - filenames to include in pystac catalog
            prefix [string] - directory (month) fo images
        '''
        
        # loop through v_files
        for cFile in v_files:
            if cFile.startswith("SVDNB"):
                name = cFile        
                month = name.split("_")[2][1:7]
                year = month[:4]
                date = datetime.strptime(name.split("_")[2][1:9], "%Y%m%d")
                aws_href = f'https://globalnightlight.s3.amazonaws.com/{prefix}/{name}'
                with rasterio.open(aws_href) as ds:
                    bounds = ds.bounds
                    bbox = [bounds.left, bounds.bottom, bounds.right, bounds.top]
                    footprint = Polygon([
                        [bounds.left, bounds.bottom],
                        [bounds.left, bounds.top],
                        [bounds.right, bounds.top],
                        [bounds.right, bounds.bottom]
                    ])
        
        res = pystac.Item(id=name, 
                         geometry=footprint.__geo_interface__, bbox=bbox,
                         datetime = date,
                         properties = {})
        for cFile in v_files:
            print(cFile)
            cur_href = f'https://globalnightlight.s3.amazonaws.com/{prefix}/{cFile}'
            res.add_asset(key=cFile.split("_")[0], asset=pystac.Asset(
                    href=cur_href,
                    media_type=pystac.MediaType.COG
                ))        
        self.month = month
        self.colid = f'VIIRS_DNB_rade9_npp_{month}'
        self.date = date
        self.href = aws_href
        self.bbox = bbox
        self.polygon = footprint
        self.pystac_item = res
        self.cEntry = [self.colid,aws_href.replace(".tif", ".json"),name,aws_href,bbox,footprint.__geo_interface__,date.strftime('%Y/%m/%d'),str(date)]