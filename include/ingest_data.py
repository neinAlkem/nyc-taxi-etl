import os
import requests
from google.cloud import storage
import tqdm
import argparse

def upload_file_gcs(bucket_name, local_file, object_name):
    """Uploads a file to the bucket."""
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # local_file = "local/path/to/file"
    # object_name = "storage-object-name"

    storage_client = storage.Client.from_service_account_json('/usr/local/airflow/include/credentials/cred_file.json')
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

initial_url_download = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
bucket = 'project_raw_ingest'

def download_data(service, year) :
    """
    The function `download_data` downloads data files for a given year and service, uploads them to
    Google Cloud Storage, and then deletes the local files.
    
    :param year: The `year` parameter in the `download_data` function is used to specify the year for
    which data needs to be downloaded
    :param service: The `service` parameter in the `download_data` function represent the type
    of taxy service being downloaded. 
    """
    for i in range(1,13) :
        month = f'{i:02d}'
        filename = f'{service}_tripdata_{year}-{month}.parquet'
        request_url = f'{initial_url_download}/{filename}'
        
        # r = requests.get(request_url)
        local_file = filename
        # with open(local_file,'wb') as file:
        #         file.write(r.content)
        # print(f'Succesfully Downloaded {local_file}')
        
        print(f'Downloading {local_file}')
        try :
            with open(local_file, 'wb') as f:
                with requests.get(request_url, stream=True) as r:
                    r.raise_for_status()
                    total = int(r.headers.get('content-length', 0))

                    tqdm_params = {
                        'total' : total,
                        'miniters' :1,
                        'unit' : 'B',
                        'unit_scale' : True,
                        'unit_divisor' : True
                    }
                    with tqdm.tqdm(**tqdm_params) as pb:
                        for chunk in r.iter_content(chunk_size=8192):
                            pb.update(len(chunk))
                            f.write(chunk)
            print(f'Succesfully Downloaded {local_file}')
        
            print(f'Uploading {local_file} to GCS Storage')
            upload_file_gcs(bucket, local_file, f'{service}/{year}/{filename}')
            print(f'Succesfully Upload {local_file} to GCS Storage')
        
            os.remove(local_file)

        except :
            break


# if __name__ == '__main__':
#     parser = argparse.ArgumentParser(description="Downloading NYC Dataset from Website, Then Push to GCS")
#     parser.add_argument('year', type=int, help='Set Year of Data')
#     parser.add_argument('service', type=str, help='Set The Type of Services (Yellow / Green)')
    
#     args = parser.parse_args()
    
#     download_data(args.year,args.service)
