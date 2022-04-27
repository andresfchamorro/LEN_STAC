import os, re, ast
import geojson, boto3, rasterio
import rasterio.transform

import numpy as np
import pandas as pd
import geopandas as gpd

from rasterio.profiles import DefaultGTiffProfile
from datetime import datetime
from pandas import Timestamp
from shapely.wkt import loads
from shapely.geometry import shape, Point
from GOSTRocks.misc import tPrint
from botocore.config import Config
from botocore import UNSIGNED
import urlib.request

pd.set_option('display.max_colwidth', None)
repo_dir = os.path.dirname(os.path.realpath(__file__)) # if Notebooks could also access thorugh ..

# Read in quality values from metadata
with open(os.path.join(repo_dir, "catalog", "new_good_vflag_ints.txt")) as inData:
    good_viirs_flags = [int(line) for line in inData]

class Catalog(object):
    '''
    '''
    def __init__(self, scenes = None):
        def load_scenes():
            catalog_path = os.path.join(repo_dir, "catalog/VIIRS_Catalog_Final.csv")
            if os.path.exits(catalog_path):
                scenes = pd.read_csv(catalog_path, index_col = 0) # nrows=5000
            else:
                print("Downloading catalog from: https://globalnightlight.s3.amazonaws.com/metadata/VIIRS_Catalog_Final.csv")
                print("Saving at LEN_STAC/src/catalog/VIIRS_Catalog_Final.csv")
                urllib.request.urlretrieve("https://globalnightlight.s3.amazonaws.com/metadata/VIIRS_Catalog_Final.csv", catalog_path)
                scenes = pd.read_csv(catalog_path, index_col = 0) # Downloading seemed more efficient instead of streaming everytime
            try:
                scenes['date'] = pd.to_datetime(scenes.date)
                scenes['datetime'] = pd.to_datetime(scenes.datetime)
                scenes['ym'] = scenes.date.dt.strftime('%Y-%m')
                scenes['bbox'] = scenes.bbox.apply(lambda x: ast.literal_eval(x))
                scenes['minx'] = scenes.bbox.apply(lambda x: x[0])
                scenes['miny'] = scenes.bbox.apply(lambda x: x[1])
                scenes['maxx'] = scenes.bbox.apply(lambda x: x[2])
                scenes['maxy'] = scenes.bbox.apply(lambda x: x[3])
                # scenes['geometry_geojson'] = scenes.geometry.apply(lambda x: geojson.loads(x.replace("'", '"')))
                # scenes['geometry'] = scenes.geometry.apply(lambda x: shape(geojson.loads(x.replace("'", '"'))))
                scenes['geometry'] = scenes.geometry.apply(loads)
                return(scenes)
            except:
                return(scenes)
        self.scenes = load_scenes() if scenes is None else scenes

    def __str__(self):
        return f"Catalog with {len(self.scenes)} scenes"

    def __len__(self):
        return len(self.scenes)

    def search_by_day(self, day):
        '''
        get all scenes from a particular day:
            scenes day == day
        '''
        y, m, d = [int(x) for x in day.split('-')]
        day = Timestamp(year=y, month=m, day=d)
        search_results = self.scenes.loc[self.scenes.date == day]
        if len(search_results)>0:
            return Catalog(search_results)
        else:
            raise Exception("No hits!")

# LOOK INTO FUNCTION OVERLOADING
    # def search_by_year_month(self, ym):
    #     '''
    #     get all scenes from a particular day:
    #         scenes day == day
    #     '''
    #     search_results = self.scenes.loc[self.scenes.ym == ym]
    #     if len(search_results)>0:
    #         return Catalog(search_results)
    #     else:
    #         raise Exception("No hits!")

    def search_by_year_month(self, year, month):
        '''
        get all scenes from a particular year/month:
            scenes day == day
        '''
        day = Timestamp(year=year, month=month, day=1)
        ym = day.strftime('%Y-%m')
        search_results = self.scenes.loc[self.scenes.ym == ym]
        if len(search_results)>0:
            return Catalog(search_results)
        else:
            raise Exception("No hits!")

    def search_by_period(self, start_day, end_day):
        '''
        get all scenes that intersect a time period:
            scenes date >= start_day
            scenes date <= end_day
        '''
        y, m, d = [int(x) for x in start_day.split('-')]
        start_day = Timestamp(year=y, month=m, day=d)
        y, m, d = [int(x) for x in end_day.split('-')]
        end_day = Timestamp(year=y, month=m, day=d)
        search_results = self.scenes.loc[(self.scenes.date >= start_day) & (self.scenes.date <= end_day)]
        if len(search_results)>0:
            return Catalog(search_results)
        else:
            raise Exception("No hits!")

    def search_by_bb(self, x, y):
        '''
        get all scenes that intersect a time period:
            scenes date >= start_day
            scenes date <= end_day
        '''
        search_results = self.scenes.loc[(self.scenes.minx < x) & (x < self.scenes.maxx) & (self.scenes.miny < y) & (y < self.scenes.maxy)]
        if len(search_results)>0:
            return Catalog(search_results)
        else:
            raise Exception("No hits!")

    def search_by_intersect(self, aoi, calc_overlap = False):
        '''
        get all scenes that intersect a time period:
            scenes date >= start_day
            scenes date <= end_day
        '''
        if type(self.scenes) == pd.DataFrame:
            self.scenes = gpd.GeoDataFrame(self.scenes, geometry='geometry', crs="EPSG:4326")
        if type(aoi) == Point:
            query = aoi
        elif type(aoi) == gpd.GeoDataFrame:
            if aoi.crs.to_string() != 'EPSG:4326':
                aoi = aoi.to_crs('EPSG:4326')
            query = aoi.geometry.unary_union
        search_results = self.scenes.loc[(self.scenes.intersects(query))]
        if calc_overlap:        
            def calculate_overlap(a1, a2):
                intersection = a1.intersection(a2)
                return(intersection.area/a1.area)                            
            search_results['overlap'] = search_results['geometry'].apply(lambda x: calculate_overlap(aoi.unary_union, x))
        
        if len(search_results)>0:
            return Catalog(search_results)
        else:
            raise Exception("No hits!")

            
class VIIRS_cleaner(object):
    ''' Combine nightly VIIRS images into composite
    '''
    def __init__(self, aws_bucket, scenes, geometry):
        ''' Create nightly VIIRS composites
        
        Input
            aws_bucket (string) - base path to AWS bucket storing nighttinme lights, should be globalnightlight
            catalog (len_tools.Catalog) - nightlights search object
            geometry (shapely polygon) - object used to crop nighttime imagery                       
        '''
        
        self.aws_bucket = aws_bucket
        self.scenes = scenes
        self.geometry = geometry
        
    def viirs_night(self, rade_file):
        ''' Search for the corresponding files matching the provided rad_file
        '''
        aws_bucket = self.aws_bucket
        month = rade_file.split("/")[-2]
        file_name = rade_file.split("/")[-1]
        day     = file_name.split("_")[2]
        time    = file_name.split("_")[3]
        e_thing = file_name.split("_")[4]
        # search through bucket to find other files matching that day
        s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
        sel_files = {}
        truncated = True
        continuation = ''
        sel_month = s3.list_objects_v2(Bucket=aws_bucket, Prefix=month)        
        n_loops = 0
        while truncated:
            #print(n_loops)
            #return(sel_month)
            truncated = sel_month['IsTruncated']        
            for item in sel_month['Contents']:
                if (day in item['Key']) & (time in item['Key']):# & (e_thing in item['Key']):
                    cName = item['Key']
                    cType = cName.split(".")[-3]                    
                    sel_files[cType] = cName
            if truncated:
                sel_month = s3.list_objects_v2(Bucket='globalnightlight', Prefix=month, ContinuationToken=sel_month['NextContinuationToken'])        
            n_loops = n_loops+1
        return(sel_files)
        
    def clean_viirs_data(self, attach_numpy=False, light_file='rade9'):
        ''' combine images found in scenes into a single output raster
        
       Args:
       attach_rasterio [boolean default False] - if True, attach the numpy raster objects to the input scenes geodataframe
        '''
        scenes = self.scenes
        geometry = self.geometry
        n_loops = 0
        numpy_res = {}
        for idx, row in scenes.iterrows():
            tPrint(f'{row.col_id}: {n_loops} of {scenes.shape[0]}')
            n_loops = n_loops + 1
            # Get the input files
            xx = self.viirs_night(row['filename'])
            http_base = os.path.dirname(os.path.dirname(row.href))
            self.http_base = http_base            
            inRad = rasterio.open(os.path.join(http_base, xx[light_file]))
            inFlag = rasterio.open(os.path.join(http_base, xx['vflag']))
            
            ul = inRad.index(*geometry.bounds[0:2])
            lr = inRad.index(*geometry.bounds[2:4])
            window = ((float(lr[0]), float(ul[0]+1)), (float(ul[1]), float(lr[1]+1)))
            
            inRad_data = inRad.read(1, window=window, boundless=True, fill_value=0)
            inFlag_data = inFlag.read(1, window=window, boundless=True, fill_value=0)
            
            good_data = np.isin(inFlag_data, good_viirs_flags).astype(int)
            good_rad = inRad_data * good_data
            
            if attach_numpy:
                numpy_res[idx] = [good_rad, good_data]
            
            if n_loops > 1:            
                final_data  = final_data + good_rad
                final_count = final_count + good_data
            else:
                final_data = good_rad
                final_count = good_data
        self.final_data = final_data
        self.final_count = final_count
        
        if attach_numpy:
            return({'data':final_data, 'count':final_count, 'scenes':numpy_res})
        else:
            return({'data':final_data, 'count':final_count})
        
 
    
    def write_output(self, out_folder, file_base, rad_array=None, count_array=None):
        b = self.geometry.bounds
        new_transform = rasterio.transform.from_bounds(b[0], b[1], b[2], b[3], self.final_data.shape[1], self.final_data.shape[0])

        profile = DefaultGTiffProfile()
        profile.update(width=self.final_data.shape[1], height=self.final_data.shape[0],
                      transform = new_transform, crs="epsg:4326",
                      count=1, dtype='float32')
                      
        if rad_array is None:
            rad_array = np.divide(self.final_data, self.final_count)
        if count_array is None:
            count_array = self.final_count
            
        with rasterio.open(os.path.join(out_folder, f'{file_base}_rad.tif'), 'w', **profile) as outR:
            outR.write_band(1, rad_array)
            
        with rasterio.open(os.path.join(out_folder, f'{file_base}_count.tif'), 'w', **profile) as outR:
            outR.write_band(1, count_array)
            
            
    def write_output_shift(self, cRes, out_folder, nDays=7, width=28):
        ''' Take the raw numpy arrays generated through clean_viirs_data(attach_numpy=True) and 
            create a number of output geotiffs by shifting the monthly summaries by [days] at a time
            
        Args
            cRes [data dictionary of numpy arrays]
            start_date
        '''
        
        scenes = self.scenes.sort_values(['datetime'])
        scenes['datetime'] = pd.to_datetime(scenes['date'])
        min_date = scenes['datetime'].min()
        max_date = scenes['datetime'].max()
        
        sDate = min_date
        eDate = sDate + pd.DateOffset(days=width)
        while eDate < max_date:
            sDate_str = datetime.strftime(sDate, "%Y%m%d")
            eDate_str = datetime.strftime(eDate, "%Y%m%d")
            
            cur_days = scenes.loc[(scenes['datetime'] > sDate) & (scenes['datetime'] < eDate)]
            try:
                del finalRad
                del finalCnt
            except:
                pass
            for idx, row in cur_days.iterrows():
                curRad = cRes[idx][0]
                curCnt = cRes[idx][1]
                try:
                    finalRad = finalRad + curRad
                    finalCnt = finalCnt + curCnt
                except:
                    finalRad = curRad
                    finalCnt = curCnt    
            finalRad = finalRad/finalCnt
            self.write_output(out_folder, f'SHIFTED_{sDate_str}_{eDate_str}', rad_array=finalRad, count_array=finalCnt)
            # reset for next loop
            sDate = sDate + pd.DateOffset(days=nDays)
            eDate = sDate + pd.DateOffset(days=width)
            tPrint(f'completed: {sDate_str} - {eDate_str}')    
    
    