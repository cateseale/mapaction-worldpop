import os
import ee
import yaml
import eeUtil
import pandas as pd
import geopandas as gpd
from google.cloud import storage
from pyprojroot import here
from pathlib import Path


def get_worldpop_data_from_gee(constrained=False):

    if constrained:
        return ee.ImageCollection("WorldPop/GP/100m/pop_age_sex_cons_unadj")
    else:
        return ee.ImageCollection("WorldPop/GP/100m/pop_age_sex")


def add_cols_for_totals_by_sex(worldpop_df):

    female_pops = [i for i in list(worldpop_df.columns) if i.startswith('F_')]
    male_pops = [i for i in list(worldpop_df.columns) if i.startswith('M_')]

    worldpop_df['totalF'] = worldpop_df[female_pops].sum(axis=1)
    worldpop_df['totalM'] = worldpop_df[male_pops].sum(axis=1)

    return worldpop_df


def delete_file_from_gcp(filename, bucket_name):

    storage_client = storage.Client()

    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(filename)
        blob.delete()
        print('Cloud file: ', filename, 'deleted.')
    except Exception as e:
        print(e)


def download_file_from_gcp(filename, bucket_name, dst_filename, remove=True):

    storage_client = storage.Client()

    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(filename)
        blob.download_to_filename(dst_filename)
        print('File ', dst_filename, ' downloaded from ', bucket_name, 'successfully')
        if remove:
            blob.delete()
            print('Cloud file: ', filename, 'deleted.')
    except Exception as e:
        print(e)


if __name__ == '__main__':

    config = yaml.safe_load(open("config.yml"))

    credentials = ee.ServiceAccountCredentials(config['SERVICE_ACCOUNT'], config['JSON_PATH'])
    ee.Initialize(credentials)
    eeUtil.init(service_account=credentials,
                project=config['GCP_PROJECT'],
                bucket=config['BUCKET_NAME'],
                credential_path=config['JSON_PATH'])

    # Access worldpop data from Google Earth Engine
    pop_data = get_worldpop_data_from_gee(constrained=False)
    pop_data_img = pop_data.mosaic()

    # Create an asset containing the regions for which you want to run zonal statistics on worldpop data
    polygon_data_dir = os.path.join(here(), 'data', config['COUNTRY_CODE'], 'admin_boundaries')
    gdf = gpd.read_file(os.path.join(polygon_data_dir, config['POLYGONS_PATH']))
    polys_dst_csv_path = os.path.join(polygon_data_dir, config['POLYGONS_PATH'].replace('shp', 'csv'))
    gdf.to_csv(polys_dst_csv_path, index=False)

    asset_name = config['POLYGONS_PATH'].split('.')[0]
    polys_task_id = eeUtil.upload(polys_dst_csv_path, asset_name, clean=False)
    polygons_fc = ee.FeatureCollection(f"projects/{config['GCP_PROJECT']}/assets/{asset_name}")

    # Clean up the csv file from the cloud
    delete_file_from_gcp(filename=config['POLYGONS_PATH'].replace('shp', 'csv'),
                         bucket_name=config['BUCKET_NAME'])

    # Perform zonal statistics calculation
    zonal_statistics = pop_data_img.reduceRegions(reducer=ee.Reducer.sum(), collection=polygons_fc, scale=95)

    # Export results
    task_id, cloud_dst = eeUtil.exportTable(table=zonal_statistics,
                                            blob='zonal_stats_raw',
                                            bucket=config['BUCKET_NAME'],
                                            fileFormat='csv',
                                            overwrite=True,
                                            wait_timeout=1)

    dst_dir = os.path.join(here(), 'data', config['COUNTRY_CODE'], 'zonal_stats')
    Path(dst_dir).mkdir(parents=True, exist_ok=True)
    destination_file_name = os.path.join(dst_dir, f"{config['COUNTRY_CODE']}_worldpop_zonal_stats_raw.csv")
    source_file_name = cloud_dst.split('/')[-1]
    download_file_from_gcp(filename=source_file_name, bucket_name=config['BUCKET_NAME'], dst_filename=destination_file_name)

    # Post-processing to tidy up the csv
    raw_csv = pd.read_csv(destination_file_name)

    columns_to_remove = ['ADM0_EN', 'ADM0_PCODE', 'ADM1_TYPE', 'system:index', '.geo', 'PROV_TYPE', 'PROV_CODE',
                         'SOURCE']
    df = raw_csv.drop(columns_to_remove, axis=1)

    # Calculate totals for female and male population
    df = add_cols_for_totals_by_sex(df)

    # Rearrange table and round to 2 decimal places
    df.insert(2, 'totalM', df.pop('totalM'))
    df.insert(2, 'totalF', df.pop('totalF'))
    df.insert(2, 'totalPop', df.pop('population'))
    df.insert(2, 'OLD_PROVIN', df.pop('OLD_PROVIN'))
    df = df.round(decimals=2)

    # Save results
    df.to_csv(os.path.join(dst_dir, f"{config['COUNTRY_CODE']}_worldpop_zonal_stats.csv"))

    print(f"Zonal statistics calculated for population of {df['totalPop'].sum()} people.")
