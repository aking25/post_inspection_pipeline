#!/usr/bin/env python
import json
import sys
from datetime import datetime as dt

meta = sys.argv[1]
bam_folder = sys.argv[2]
instrument_model = sys.argv[3]
first_name = sys.argv[4]        
last_name = sys.argv[5]   
email = sys.argv[6]   
organization = sys.argv[7]    
spuid_namespace = sys.argv[8]   
title = sys.argv[9]   
organism = sys.argv[10]    
package = sys.argv[11]
bioproject = sys.argv[12]
library_layout = sys.argv[13]

def write_ncbi_config():
    """
    Write config file for NCBI upload to out_dir.
    """
    today_date = dt.today().strftime('%Y-%m-%d')
    ncbi_dict = {"project_name": {
        "action_name": "hcov-19_submission",
        "action_type": "bs_sra",
        "submission_type": "Production",
        "batch_submission": "True"},
        "file_download_info": {
            "local_download_dir": bam_folder,
            "credentials_path": "/home/chrissy/ncbi_batch_push/andersen-lab-primary-4009c7fb6054.json",
            "bucket_name": "andersen-lab_hcov-19-genomics",
            "blob_name": "bam_files/illumina",
            "multiprocess": "True",
            "download_files": "False",
            "metadata_location": meta,
            "target_sample_names": "None"
        },

        "submission": {
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "hold": today_date,
            "organization": ' '.join(organization.split('_')),
            "bioproject_id": bioproject,
            "spuid_namespace": spuid_namespace,
            "title": ' '.join(title.split('_')),
            "organism": ' '.join(organism.split('_')),
            "package": package
        },

        "sra": {
            "file_type": "bam",
            "instrument_model": ' '.join(instrument_model.split('_')),
            "library_name": "SEARCH",
            "library_strategy": "AMPLICON",
            "library_source": "VIRAL RNA",
            "library_selection": "RT-PCR",
            "library_layout": library_layout,
            "library_construction_protocol": "A detailed protocol can be found at https://searchcovid.info/protocols/"
        }
    }
    config_fn = 'job_config.json'
    with open(config_fn, 'w') as outfile:
        outfile.write(json.dumps(ncbi_dict, indent=4))
        
write_ncbi_config()

with open('job_config.json', "r") as jfile:   
    json_config = json.load(jfile)
