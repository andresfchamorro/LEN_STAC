# LEN_STAC
Simple python resources to crawl and search the VIIRS NPP catalog from the [**WB Light Every Night dataset**](https://registry.opendata.aws/wb-light-every-night/).
## [VIIRS STAC Catalog](https://globalnightlight.s3.amazonaws.com/VIIRS_npp_catalog.json)
[sat-stac](https://github.com/sat-utils/sat-stac) was used to crawl through the catalog and create a tabular version, saved at *src/catalog/VIIRS_Catalog_Final.csv*. See [Crawl_STAC notebook](notebooks/Crawl_STAC.ipynb).
### Python Class **Catalog()**
Inventory of VIIRS NPP scenes with some functions to search catalog by time and area of interest (shapely point or geopandas gdf).  
See [Test_Catalog notebook](notebooks/Test_Catalog.ipynb) for examples.
```python
from len_tools import Catalog
cat = Catalog()
```
The catalog class contains a *scenes* variable - a pandas Data Frame of every scene in the VIIRS catalog with associated metadata (loaded from the csv table).
```python
cat.scenes
```
*Examples*  
Each search returns a new a catalog of scenes that match the query.
```python
cat_time_filter = cat.search_by_year_month(2020, 5)
cat_time_filter = cat.search_by_day("2020-05-04")
cat_aoi_filter = cat_time_filter.search_by_intersect(shapely_point)
cat_aoi_filter = cat_time_filter.search_by_intersect(gdf)
```
More to come...
