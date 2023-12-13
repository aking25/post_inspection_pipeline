#!/usr/bin/env python

import sys
import pandas as pd
import argparse
import os
import glob
from datetime import datetime

parser = argparse.ArgumentParser()

parser.add_argument(
    "-s",
    "--sra-meta",
    type=str,
    help="Path to SRA metadata for samples."
)

args = parser.parse_args()
sra_fp = args.sra_meta

# merge the info to get status & metadata together
meta = pd.read_csv(sra_fp)

meta['country'] = meta['geo_loc_name'].apply(lambda x: x.split(':')[1])
meta["isolate"] = meta['gisaid_virus_name'].str.replace(
    "hCoV-19","SARS-CoV-2/human"
)
meta["host"] = meta["host"].str.replace("Human", "Homo Sapiens")
meta.rename(
    columns={
        "sample_name": "sequence_ID",
        "collection_date": "collection-date",
        "collection_method": "isolation-source",
    },
    inplace=True,
)
meta.loc[meta["country"] == "MEX", "country"] = "Mexico"
# genbank_meta = genbank_meta[genbank_meta['sequence_ID'].notna()]
meta['organism'] = "Severe acute respiratory syndrome coronavirus 2"
meta['BioProject'] = "PRJNA612578"
meta = meta.rename(columns={'gisaid_accession':'Note'})

print(meta.columns)
meta = meta[
        [
            "sequence_ID",
            "organism",
            "isolate",
            "country",
            "collection-date",
            "host",
            "isolation-source",
            "BioProject",
            "Note"
        ]].fillna('')
meta[['year','mo','day']]=meta['collection-date'].str.split('-',expand=True)
meta['isolate_prefix']=[('/').join(x.split('/')[:-1]) for x in meta['isolate']]
meta['isolate']=meta['isolate_prefix']+'/'+meta['year']

meta.drop(['year','mo','day','isolate_prefix'],inplace=True,axis=1)
meta.to_csv('source.src', sep="\t",index=False)
