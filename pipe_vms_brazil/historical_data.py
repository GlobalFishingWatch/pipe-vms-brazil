"""
Loads historical data and upload it to BQ.
Years Files should be in json and GZIP Format.

This script will do:
1- Locate (devices and messages).
2- Per each device ID will list all the messages ID.
3- Add the device info in the message info and save a NEWLINEJSON.
4- Compress again in a separate folder.

Ex.
python -m pipe_vms_brazil.historical_data -d 2012 -i ./downloads/output -o gs://vms-gfw/brazil/historical/v20210321/2012/
"""

from datetime import date, datetime, timedelta

from google.cloud import storage

from shutil import rmtree

from pathlib import Path

import argparse, gzip, json, os, re, time, sys


# FORMATS
FORMAT_DT = '%Y'

# FOLDER
LOCAL_MERGER_PATH = "prepare_data"

PROJECT_ID = "world-fishing-827"


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
    storage_client = storage.Client(PROJECT_ID)
    gcs_search = re.search('gs://([^/]*)/(.*)', gcs_path)
    bucket = storage_client.bucket(gcs_search.group(1))
    pattern_path = Path(pattern_file)
    for filename in pattern_path.parent.glob(pattern_path.name + '*'):
        blob = bucket.blob(gcs_search.group(2) + filename.name)
        blob.upload_from_filename(filename)
        print("File from file system <{}> uploaded to <{}>.".format(filename, gcs_path))

def daterange(start, end):
    for n in range(int((end - start).days)):
        yield start + timedelta(n)


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
    parser.add_argument('-x','--date_stop', help='The date to stop the'
                        'iteration for years. Date inclusive.', default=None, required=False)
    args = parser.parse_args()
    query_date = datetime.strptime(args.query_date, FORMAT_DT)
    input_directory= args.input_directory
    LOCAL_MERGER_PATH= args.input_directory
    DATE_TO_CUT= args.date_stop
    output_directory= args.output_directory

    devices_file_path = f'{LOCAL_MERGER_PATH}/Devices.txt'
    messages_file_path = f'{LOCAL_MERGER_PATH}/{query_date.strftime(FORMAT_DT)}.json'

    start_time = time.time()

    # Decompresses the GZIP.
    print(f'Decompresses the original GZIP files <{devices_file_path},{messages_file_path}>')
    devices=[]
    messages=[]
    with open(devices_file_path,'r') as devices_original:
        content = json.loads(devices_original.read())
        for device in content['devices']:
            devices.append(device)

    with open(messages_file_path,'r') as messages_original:
        content = json.loads(messages_original.read())
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
    print(f'Run the whole year, day by day and compress each day result with GZIP.')
    create_directory(f'{LOCAL_MERGER_PATH}/{query_date.year}')
    acum=0
    for single_day in daterange(date(query_date.year,1,1),date(query_date.year+1,1,1)):
        single_day_formated = single_day.strftime('%Y-%m-%d')
        merged_file_path = f'{LOCAL_MERGER_PATH}/{query_date.year}/{single_day_formated}.json.gz'

        daily_msg = [msg for msg in merge if msg['datahora'].startswith(single_day_formated)]
        acum += len(daily_msg)
        print(f'Day {single_day_formated} amount of messages {len(daily_msg)}, output: {merged_file_path}')
        with gzip.open(merged_file_path,'wt', compresslevel=9) as merged:
            for message in daily_msg:
                json.dump(message, merged)
                merged.write("\n")
        if DATE_TO_CUT != None and single_day_formated == DATE_TO_CUT:
            break

        # Saves to GCS
        gcs_transfer(merged_file_path, output_directory)

    print(f'Total of merged results  {len(merge)} vs daily messages acum  {acum}')
    # rmtree(LOCAL_MERGER_PATH)

    ### ALL DONE
    print("All done, you can find the output file here: {0}".format(output_directory))
    print("Execution time {0} minutes".format((time.time()-start_time)/60))
