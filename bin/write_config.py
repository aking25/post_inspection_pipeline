#!/usr/bin/env python
import json
import sys
from datetime import datetime as dt

meta = sys.argv[1]
bam_folder = sys.argv[2]

def write_ncbi_config(meta_loc, bam_fp):
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
            "local_download_dir": bam_fp,
            "credentials_path": "/home/chrissy/ncbi_batch_push/andersen-lab-primary-4009c7fb6054.json",
            "bucket_name": "andersen-lab_hcov-19-genomics",
            "blob_name": "bam_files/illumina",
            "multiprocess": "True",
            "download_files": "False",
            "metadata_location": meta_loc,
            "target_sample_names": "None"
        },

        "submission": {
            "first_name": "Alison",
            "last_name": "King",
            "email": "aking@scripps.edu",
            "hold": today_date,
            "organization": "The Scripps Research Institute",
            "bioproject_id": "PRJNA612578",
            "spuid_namespace": "SEARCH",
            "title": "hcov-19 genomics",
            "organism": "Severe acute respiratory syndrome coronavirus 2",
            "package": "SARS-CoV-2.cl.1.0"
        },

        "sra": {
            "file_type": "bam",
            "instrument_model": "Illumina NovaSeq 6000",
            "library_name": "SEARCH",
            "library_strategy": "AMPLICON",
            "library_source": "VIRAL RNA",
            "library_selection": "RT-PCR",
            "library_layout": "PAIRED",
            "library_construction_protocol": "A detailed protocol can be found at https://searchcovid.info/protocols/"
        }
    }
    config_fn = 'job_config.json'
    with open(config_fn, 'w') as outfile:
        outfile.write(json.dumps(ncbi_dict, indent=4))
        
write_ncbi_config(meta, bam_folder)

with open('job_config.json', "r") as jfile:   
    json_config = json.load(jfile)
