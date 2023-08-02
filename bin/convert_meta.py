#!/usr/bin/env python
import pandas as pd
import sys

gisaid_meta_fp = sys.argv[1]
gisaid_meta = pd.read_csv(gisaid_meta_fp)
bioproj = sys.argv[2]
meta_fp = sys.argv[3]
meta = pd.read_csv(meta_fp)

col_rename = {
    'covv_subm_sample_id': 'ID',
    'covv_collection_date': 'collection_date',
    'covv_location': 'geo_loc_name',
    'covv_virus_name': 'isolate',
    'covv_specimen': 'isolation_source',
    'covv_authors': 'collected_by_1',
    'covv_orig_lab': 'collected_by_2'
}

gisaid_meta = gisaid_meta.rename(columns = col_rename)[col_rename.values()]
meta = meta[meta['ID'].isin(gisaid_meta['ID'])]
meta = meta[['ID', 'gisaid_accession', 'gb_accession', 'host']] # add biosample and sra once they are added as columns
meta = meta.merge(gisaid_meta, how="left")
meta = meta[meta['host']!= "Environment"]

# replace geo loc name / with :
meta['geo_loc_name'] = meta['geo_loc_name'].str.replace('/',':')

# add host = Homo Sapiens, host_disease = COVID-19, bioproject
meta['host'] = 'Homo Sapiens'
meta['host_disease'] = 'COVID-19'
meta['bioproject_accession'] = bioproj
# add vaccine_received = not collected
meta['vaccine_received'] = 'not collected'

author_conversions = pd.read_csv('/home/al/code/bjorn_utils/author_conversions.csv')
meta = meta.merge(author_conversions, how="left", left_on="collected_by_1", right_on="authors_original")
meta = meta.drop(columns=["collected_by_1", "authors_original"])
meta = meta.rename(columns={"authors_new": "collected_by_1"})

# split the dataframe by if it has both fields, one, or the other
has_both_fields = meta[(~meta["collected_by_1"].isna()) & (~meta["collected_by_2"].isna())]
has_both_fields["collected_by"] = has_both_fields["collected_by_1"] + " with the help of " + has_both_fields["collected_by_2"]

# has the first field and not the second
has_first_field = meta[(~meta["collected_by_1"].isna()) & (meta["collected_by_2"].isna())]
has_first_field["collected_by"] = has_first_field["collected_by_1"]

# has the second field and not the first
has_second_field = meta[(meta["collected_by_1"].isna()) & (~meta["collected_by_2"].isna())]
has_second_field["collected_by"] = has_second_field["collected_by_2"]

# if neither
has_neither_field = meta[(meta["collected_by_1"].isna()) & (meta["collected_by_2"].isna())]
has_neither_field["collected_by"] = "Unknown"

# combine the 4 dataframes above 
meta_2 = pd.concat([has_both_fields, has_first_field, has_second_field, has_neither_field])

# drop defunct columns
meta_3 = meta_2.drop(columns=["collected_by_1", "collected_by_2"])

meta = meta_3[
        (~meta_3["collected_by"].str.contains("Helix")) & (
        ~meta_3["gisaid_accession"].isna())]
# dump out the dataframe of sequences that need to be uploaded
meta = meta.fillna("not collected")
meta["collection_date"] = pd.to_datetime(meta["collection_date"]).dt.strftime('%Y-%m-%d')
meta = meta[(meta["host"] == "Homo Sapiens") | (meta["host"] == "")]
meta["isolate"] = meta["isolate"].str.replace("N/A", "not collected")
meta["sample_name"] = meta["ID"]
meta.drop(columns=["ID", "gb_accession"], inplace=True)
meta.loc[:,"collection_method"] = meta.loc[:,"isolation_source"]
meta.loc[:,"gisaid_virus_name"] = meta.loc[:,"isolate"]
meta = meta[['sample_name', 'collection_date','geo_loc_name','isolate',
                'isolation_source','collection_method','gisaid_accession',
                'gisaid_virus_name','host','bioproject_accession','host_disease',
                'collected_by','vaccine_received']]
meta.to_csv("ncbi_metadata.csv", index=False)
