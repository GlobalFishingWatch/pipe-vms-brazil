"""
Prepares the data to be uploadede to BQ.

This script will do:
1- Decompress the GZIPs (devices and messages).
2- Per each device ID will list all the messages ID.
3- Add the device info in the message info and save a NEWLINEJSON.
4- Compress again in a separate folder.
"""

from datetime import datetime

from google.cloud import storage

from shutil import rmtree

from pathlib import Path

import argparse, gzip, json, os, re, time


# FORMATS
FORMAT_DT = '%Y-%m-%d'

# FOLDER
LOCAL_MERGER_PATH = "prepare_data"

def copy_blob(gcs_path, blob_name, original_file):
    """
    Copies a blob from one bucket to another with a new name.
    :param gcs_path: The gcs path to the original file.
    :type gcs_path: str
    :param blob_name: The name of the file.
    :type blob_name: str
    :param original_file: The file to download.
    :type original_file: str
    """
    storage_client = storage.Client()

    prefix = ''
    bucket_name = ''
    if gcs_path.startswith("gs:"):
        bucket_name = re.search('(?<=gs://)[^/]*', gcs_path).group(0)
        prefix = re.search('(?<=gs://)[^/]*/(.*)', gcs_path).group(1)

    print(f'bucket {bucket_name}, prefix: {prefix}')

    source_bucket = storage_client.bucket(bucket_name)
    print(f'{prefix}{blob_name}')
    source_blob = source_bucket.blob(f'{prefix}{blob_name}')

    source_blob.download_to_filename(original_file.name)


def create_directory(name):
    """
    Creates a directory in the filesystem.
    :param name: The name of the directory.
    :type name: str
    """
    if not os.path.exists(name):
        os.makedirs(name)

def gcs_transfer(pattern_file, gcs_path):
    """
    Uploads the files from file system to a GCS destination.
    :param pattern_file: The pattern file without wildcard.
    :type pattern_file: str
    :param gcs_path: The absolute path of GCS.
    :type gcs_path: str
    """
    storage_client = storage.Client()
    gcs_search = re.search('gs://([^/]*)/(.*)', gcs_path)
    bucket = storage_client.bucket(gcs_search.group(1))
    pattern_path = Path(pattern_file)
    for filename in pattern_path.parent.glob(pattern_path.name + '*'):
        blob = bucket.blob(gcs_search.group(2) + filename.name)
        blob.upload_from_filename(filename)
        print("File from file system <{}> uploaded to <{}>.".format(filename, gcs_path))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Merges the data comming from'
                                     'devices and messages and generate a new'
                                     'GZIP to compress.')
    parser.add_argument('-d','--query_date', help='The date to be queried. Expects a str in format YYYY-MM-DD',
                        required=True)
    parser.add_argument('-i','--input_directory', help='The GCS directory'
                        'where the data is stored. Expected with slash at'
                        'the end.', required=True)
    parser.add_argument('-o','--output_directory', help='The GCS directory'
                        'where the data will be stored. Expected with slash at'
                        'the end.', required=True)
    args = parser.parse_args()
    query_date = datetime.strptime(args.query_date, FORMAT_DT)
    input_directory= args.input_directory
    output_directory= args.output_directory

    devices_file_path = f'{LOCAL_MERGER_PATH}/devices_{query_date.strftime(FORMAT_DT)}.json.gz'
    messages_file_path = f'{LOCAL_MERGER_PATH}/messages_{query_date.strftime(FORMAT_DT)}.json.gz'
    merged_file_path = f'{LOCAL_MERGER_PATH}/{query_date.strftime(FORMAT_DT)}.json.gz'

    start_time = time.time()

    create_directory(LOCAL_MERGER_PATH)

    # Copies the original GZIP files to local.
    print('Copies the original GZIP files to local.')
    with open(devices_file_path, 'wb') as devices_original_file:
        copy_blob(input_directory, f'devices/{query_date.strftime(FORMAT_DT)}.json.gz', devices_original_file)
    with open(messages_file_path, 'wb') as messages_original_file:
        copy_blob(input_directory, f'messages/{query_date.strftime(FORMAT_DT)}.json.gz', messages_original_file)

    # Decompresses the GZIP.
    print(f'Decompresses the original GZIP files <{devices_file_path},{messages_file_path}>')
    devices=[]
    messages=[]
    with gzip.open(devices_file_path,'rb') as devices_original:
        content = json.loads(devices_original.read().decode())
        for device in content['devices']:
            devices.append(device)

    with gzip.open(messages_file_path,'rb') as messages_original:
        content = json.loads(messages_original.read().decode())
        for message in content['mensagens']:
            messages.append(message)

    # Per each device ID will list all the messages ID.
    merge=[]
    for device in devices:
        merge += [dict(ID=message['ID'], curso=message['curso'],
                       datahora=datetime.strptime(message['datahora'],'%d-%m-%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S'),
                       lat=message['lat'], lon=message['lon'], mID=message['mID'], speed=message['speed'],
                       codMarinha=device['codMarinha'], nome=device['nome'])
                  for message in messages
                  if message['ID'] == device['ID']]
        print(f'== device {device["ID"]} DONE.')
    print(f'Total of devices read {len(devices)}')
    print(f'Total of messages read {len(messages)}')
    print(f'Total of merged results  {len(merge)}')

    # Compress
    print(f'Saves the merged file {merged_file_path} and compress with GZIP.')
    with gzip.open(merged_file_path,'wt', compresslevel=9) as merged:
        for message in merge:
            json.dump(message, merged)
            merged.write("\n")

    # Saves to GCS
    gcs_transfer(merged_file_path, output_directory)

    rmtree(LOCAL_MERGER_PATH)

    ### ALL DONE
    print("All done, you can find the output file here: {0}".format(output_directory))
    print("Execution time {0} minutes".format((time.time()-start_time)/60))
