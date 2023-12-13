#!/usr/bin/env python

import os
import ast
import sys
import json
import pickle
import ftplib
import typing
import requests
import argparse
import xmlschema
import pandas as pd
import lxml.etree as et
from copy import copy
from math import isnan
from google.cloud import storage
import time
from datetime import datetime

def create_xml(out_dir, zip_file, xml_template):
    cmd = 'touch %s/submission.xml' %(out_dir)
    os.system(cmd)
    submission_template_location = xml_template
    tree = et.parse(submission_template_location)
    root = tree.getroot() #submission level
    today_date = datetime.today().strftime('%Y-%m-%d')
   
    #submission level formatting
    comment = root.find("Description/Comment")
    comment.text = "New test submission, Genbank"    
     
    file_path = root.find("Action/AddFiles/File")
    file_path.attrib['file_path'] = zip_file
    
    namespace = root.find("Action/AddFiles/Identifier/SPUID")
    namespace.text = today_date + '.sarscov2'
    
    tree.write("%s" %(out_dir + '/submission.xml'))
    
def write_fasta(fasta, outdir):    
    # seqs = {}
    
    # with open(fasta, 'r') as f:
    #     for line in f:
    #         if line.startswith('>'):
    #             key = line
    #         else:
    #             seqs[key] = line
    # with open(outdir+'/sequences.fsa', 'w') as f:
    #     for seq in seqs:
    #         f.write('>' + seq + '\n')
    #         f.write(seqs[seq])
    cmd = 'cp %s %s' %(fasta,outdir)
    os.system(cmd)
                
def zip_files(outdir, sbt):
    cmd = 'cp %s %s' %(sbt, out_dir)
    os.system(cmd)

    cmd = 'zip genbank.zip *.fsa *.src *.sbt'
    os.system(cmd)    
    
parser = argparse.ArgumentParser()

parser.add_argument(
    "-o",
    "--out-dir",
    type=str,
    help="Path to output files for ncbi upload."
)

parser.add_argument(
    "-f",
    "--fasta",
    type=str,
    help="Fasta file with all sequences."
)

parser.add_argument(
    '-s',
    '--sbt-template',
    help="Path to sbt template"
)

parser.add_argument(
    '-x',
    '--xml-template',
    help="Path to xml template"
)

args = parser.parse_args()
out_dir = args.out_dir
fasta = args.fasta
sbt_template = args.sbt_template
xml_template = args.xml_template

write_fasta(fasta, out_dir)
zip_files(out_dir, sbt_template)
create_xml(out_dir, 'genbank.zip', xml_template)
