import sys
import requests
import json
import pandas as pd
# used to get values from .env file
from decouple import config

"""
Uploads multiple data objects to Zenodo.
Reads a CSV file containing a list of data object filenames, 
creates a Zenodo deposit for each data object and uploads the object.
Output: A CSV file containing DOI and download link for each data object.
Used in TCGADataPipelines and 3CA pipelines.
"""

input_repo = sys.argv[1]
title = sys.argv[2]
description = sys.argv[3]

print(input_repo)
print(title)
print(description)

# in_dir = "/Users/minoru/Documents/pfs/" + input_repo + "/"
# out_dir = "~/Documents/pfs/upload3CA/"
# root_url = 'https://sandbox.zenodo.org'

in_dir = "/pfs/" + input_repo + "/"
out_dir = "/pfs/out/"
ACCESS_TOKEN = config('ZENODO_TOKEN')
root_url = 'https://zenodo.org'

data_list = pd.read_csv(in_dir + 'data_list.csv')

# Create deposit
depo_res = requests.post(root_url + '/api/deposit/depositions',
                        params={'access_token': ACCESS_TOKEN}, json={},
                        headers={"Content-Type": "application/json"})

depo_json = depo_res.json()
print(depo_json)
deposition_id = depo_json['id']

# Add metadata
metadata = {
    'metadata': {
        'title': title,
        'upload_type': 'dataset',
        'description': description,
        'creators': [{'name': 'Haibe-Kains, Benjamin',
                    'affiliation': 'University Health Network'}]
    }
}
metadata_res = requests.put(
    root_url + '/api/deposit/depositions/%s' % deposition_id, 
    params={'access_token': ACCESS_TOKEN}, 
    data=json.dumps(metadata),
    headers={"Content-Type": "application/json"}
)
print(metadata_res.status_code)

# Upload data
bucket_url = depo_json['links']['bucket']
doi = []
download_link = []
for filename in data_list['filename'].tolist():

    doi.append(depo_json['metadata']['prereserve_doi']['doi'])
    download_link.append(root_url + "/record/" + str(deposition_id) + "/files/" + filename + "?download=1")
    
    with open(in_dir + filename, 'rb') as fp:
        upload_res = requests.put(
            bucket_url + '/'+ filename,
            data=fp,
            # No headers included in the request, since it's a raw byte request
            params={'access_token': ACCESS_TOKEN},
        )
    print(filename + ' ' + str(upload_res.status_code))

# # Publish
# publish_res = requests.post(root_url + '/api/deposit/depositions/%s/actions/publish' % deposition_id,
#                     params={'access_token': ACCESS_TOKEN} )
# print(publish_res.status_code)

data_list = data_list.assign(doi = doi, download_link = download_link)
data_list.to_csv(out_dir + "data_list.csv", encoding='utf-8', index=False)
