=========================================================
Calculate zonal statistics of regions using WorldPop data
=========================================================

WorldPop release `age and gender disaggregated data at 5 year age gaps <https://www.worldpop.org/project/categories?id=8>`_ globally. This tool accepts a shapefile of polygon regions, and calculates the zonal statistics for each area.


Usage
====================================================

Edit the config file. There are six arguments to set:

- SERVICE_ACCOUNT: Email address for google cloud platform service account.
- JSON_PATH: Path to service account credentials json.
- GCP_PROJECT: Earth engine-enabled google cloud project.
- BUCKET_NAME: Google cloud storage bucket name.
- POLYGONS_PATH: Path to a shapefile containing polygon regions, e.g. administrative boundaries for a country.
- COUNTRY_CODE: A three letter country code, e.g. for Madagascar use 'mdg'.
