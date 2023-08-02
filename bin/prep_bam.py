#!/usr/bin/env python
import pandas as pd
import sys
import os
import glob

ncbi_meta_fp = sys.argv[1]
ncbi_meta = pd.read_csv(ncbi_meta_fp)

bam_folder = sys.argv[2]

bam_files = glob.glob(f"{bam_folder}/*.bam")
for file in bam_files:
    name = file.split('/')[-1].split('_')[0].split('.')[0]
    if name not in list(ncbi_meta["sample_name"]):
        cmd = 'rm %s' %(file)
        os.system(cmd)
    






