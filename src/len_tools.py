import os, re
from datetime import datetime
from pandas import Timestamp
import pandas as pd
import ast
import geojson
from shapely.geometry import shape, Point
import geopandas as gpd
# try:
#     import eemont
# except:
#     print("eemont not available")
# try:
#     import geemap
# except:
#     print("geemap not available")

pd.set_option('display.max_colwidth', None)
repo_dir = os.path.dirname(os.path.realpath(__file__)) # if Notebooks could also access thorugh ..

class Catalog(object):
    '''
    '''
    def __init__(self, scenes = None):
        def load_scenes():
            scenes = pd.read_csv(os.path.join(repo_dir, "catalog/VIIRS_Catalog_Final.csv"), index_col = 0) # nrows=5000
            scenes['date'] = pd.to_datetime(scenes.date)
            scenes['datetime'] = pd.to_datetime(scenes.datetime)
            scenes['ym'] = scenes.date.dt.strftime('%Y-%m')
            scenes['bbox'] = scenes.bbox.apply(lambda x: ast.literal_eval(x))
            scenes['minx'] = scenes.bbox.apply(lambda x: x[0])
            scenes['miny'] = scenes.bbox.apply(lambda x: x[1])
            scenes['maxx'] = scenes.bbox.apply(lambda x: x[2])
            scenes['maxy'] = scenes.bbox.apply(lambda x: x[3])
            # scenes['geometry_geojson'] = scenes.geometry.apply(lambda x: geojson.loads(x.replace("'", '"')))
            scenes['geometry'] = scenes.geometry.apply(lambda x: shape(geojson.loads(x.replace("'", '"'))))
            return scenes
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

    def search_by_intersect(self, aoi):
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
        if len(search_results)>0:
            return Catalog(search_results)
        else:
            raise Exception("No hits!")
