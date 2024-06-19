#!/usr/bin/env python

"""
Script provides utility functions for the following:
* Uploading for BioSampleID
* SRA submission
* Both at the same time.
"""

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

# import credentials

class Sample:
    def __init__(self, sample_name):
        self.sample_name = sample_name
    def file_name(self, filename):
        self.file_name = filename
    def full_filepath(self, full_filepath):
        self.full_filepath = full_filepath
    #refers to if we downloaded it
    def file_download_status(self, file_download_status):
        self.file_download_status = file_download_status
    def action_type(self, action_type):
        self.action_type = action_type
    def biosample_status(self, biosample_status):
        self.biosample_status = biosample_status
    def sra_status(self, sra_status):
        sel.sra_status = sra_status
    def biosample_accession(self, biosample_accession):
        self.biosample_accession = biosample_accession
    def sra_accession(self, sra_accession):
        self.sra_accession = sra_accession

def open_config_file(file_path : str ) -> dict:
    """
    Open and parse a .json config file.
    
    Parameters
    ----------
    file_path : str
        The full filepath to the json file.    

    Returns
    -------
    json_config : dict
        The dictionary stored in the json file.
    """
    with open(file_path, "r") as jfile:   
        json_config = json.load(jfile)
    
    return(json_config)
    
def format_xml(metadata_tsv : str, sample_objs : list, submission_xml_file : str , action_name : str,
    action_type : str, file_type : str, json_config : dict) -> list:

    """
    Takes in a meatdata .tsv file and formats it into .xml
    for BioSample and/or SRA upload, using the base .xml file.    

    Parameters
    ---------
    metadata_tsv : str
        Full filepath to where the metadata tsv is stored.
    submission_xml_file : str
        Full filepath to where the base .xml file is stored.
    action_name : str
        Dir where we're outputting the .xml file.
    action_type : str
        Defines the action block of the .xml file. Can be either bs or bs_sra.
    found_files : list
        List of files that have been sucessfully downloaded locally, used if SRA.
    json_config : dict
        Parameters set by user for formatting.

    Returns
    -------
    succeeded_files : list
        Files we successfully wrote the .xml file.
    """
     
    return_sample_objs = []    

    #load table but skip over the header
    metadata_df = pd.read_csv(metadata_tsv) 
    
    #load base submission template, wither for bs or bs.sra
    tree = et.parse(submission_xml_file)
    root = tree.getroot()

   #part of our action includes biosample submission
    if 'bs' in action_type:
        #remove any excess in the attributes section that come in the bs template
        attributes = root.find("Action/AddData/Data/XmlContent/BioSample/Attributes")
        for a in attributes.iter("Attribute"):
            a.getparent().remove(a)
        
        bs_action = root.find("Action/AddData").getparent()

    if 'sra' in action_type:
        attributes = root.find("Action/AddFiles")
        for a in attributes.iter("Attribute"):
            a.getparent().remove(a)
        files = root.find("Action/AddFiles/File")
        files.getparent().remove(files)
        sra_action = root.find("Action/AddFiles").getparent()
    
    actions = root.findall("Action")
    for a in actions:
        a.getparent().remove(a)
    
    
    #we iterate and append to .xml files for each sample
    for sample in sample_objs:
        #if we are even trying to do sra and don't have a filename, we skip
        if "sra" in action_type:
            if str(sample.file_name) == "Not Found":
                continue
            if str(sample.file_download_status) == 'Absent':
                continue
        
        row = metadata_df.loc[metadata_df['sample_name'] == sample.sample_name]
        row = row.iloc[0].dropna() 
        if 'gisaid_virus_name' not in row:
            continue
        if 'bs' in action_type:
            #deep copy the add data biosample section
            action_copy = copy(bs_action)
            
            #add in proper sample id
            spuid = action_copy.find("AddData/Data/XmlContent/BioSample/SampleId/SPUID")
            spuid.text = str(sample.file_name)
            
            identifier = action_copy.find("AddData/Identifier/SPUID")
            identifier.text = str(sample.file_name)

            #add in the attributes defined in the metadata 
            for k,v in row.items():
                new_elem = et.Element("Attribute")
                new_elem.set("attribute_name", k)
                new_elem.text = str(v)
                action_copy.find("AddData/Data/XmlContent/BioSample/Attributes").insert(-1,new_elem)    
            root.append(action_copy)        

        if 'sra' in action_type:
            action_copy = copy(sra_action)
            
            #add in proper sample id
            spuid = action_copy.find("AddFiles/AttributeRefId/RefId/SPUID")
            spuid.text = str(sample.file_name)
            
            identifier = action_copy.find("AddFiles/Identifier/SPUID")
           
            identifier.text = row['gisaid_virus_name']  

            sra_dict = json_config["sra"]
            filename = action_copy.find("AddFiles/File")
            filename.attrib["file_path"] = str(sample.file_name) + '%s' %file_type
            
            for k,v in sra_dict.items():
                new_elem = et.Element("Attribute")
                new_elem.set("name", k)
                new_elem.text = str(v)
                action_copy.find("AddFiles").insert(2, new_elem) 
            root.append(action_copy)
   
     
    #write the output .xml file
    tree.write("submission.xml")

def google_cloud_bam(local_download_dir, credentials_path, file_type, filename_list, \
    bucket_name, blob_name, multiprocess):
    """
    Downloads contents of a folder from a google cloud bucket to a local directory.    
    Intended use is to retrieve bam files prior to SRA submission.

    Parameters
    ----------
    local_download_dir : str
        Path the directory the bam files should be downloaded to.
    credentials_path : str
        Path to the credentials .json file that is used to connect to 
        the google cloud storage client.
    filename_list : list
        Filenames to try and download, will override file_type is specified.
    file_type : str
        File extension to try and download.
    bucket_name : str
        The name of the bucket to access.
    blob_name : str
        The name of the folder to download.
    multiprocess : bool
        Whether to multiprocess pull files or not.
    """
    #multiprocess download files, use if >1000
    if multiprocess:
        if len(filename_list) > 0:
            pass
        else:
            if file_type:
                pass
            else:
                pass
        with open('filenames.txt','w') as f:
            for name in filename_list:
                f.write('gs://%s/%s/*%s_*.%s' %(bucket_name, blob_name,name,file_type) + '\n')
        cmd = 'cat filenames.txt | gsutil -m cp -n -I %s' %(local_download_dir)        
        os.system(cmd)
    
    #download files one at a time, use if <1000
    else:
        client = storage.Client.from_service_account_json(credentials_path) 
        bucket = client.bucket(bucket_name, user_project='andersen-lab-primary')
        blobs = bucket.list_blobs(prefix=blob_name)
       
        for blob in blobs:
            filename = os.path.basename(blob.name)
            blob.download_to_filename(os.path.join(local_download_dir,filename))
       
def ftp_connection(run_type, run_dir, sample_objs):
    server = 'ftp-private.ncbi.nlm.nih.gov'
    username = credentials.username
    password = credentials.password
        
    ftp = ftplib.FTP(server, username, password)
    ftp.set_debuglevel(1)
    ftp.set_pasv(False)
    #rework this to be more pythonic
    if run_type == 'Test':
        ftp.cwd('submit/Test')
    if run_type == 'Production':
        ftp.cwd('submit/Production')

    if run_dir not in ftp.nlst(): 
        ftp.mkd(run_dir)
    ftp.cwd(run_dir)
    filenames = [str(x.full_filepath) for x in sample_objs if str(x.file_download_status)=='Present']
    filenames.append(os.path.join(run_dir, 'submission.xml')) 
    for i, filename_transfer in enumerate(filenames):
        if i % 100 == 0:
            print("%s of %s transfered" %(i, len(filenames)))
        file_transfer = open(filename_transfer,'rb') 
        filename_stored = os.path.basename(filename_transfer)    
        if filename_stored not in ftp.nlst():
            ftp.storbinary('STOR %s' %filename_stored, file_transfer)
        file_transfer.close()
        time.sleep(10)

        
    
    file_transfer = open("submit.ready", "w").close()
    file_transfer = open("submit.ready",'rb')
    ftp.storbinary('STOR submit.ready', file_transfer)
    
def submission_xml(action_type : str, config_template :str , submission_format_output : str, \
    json_config : dict):
    """
    Formats the overall submission .xml file for a BioSample using general information 
    provided in biosample_submission.json  config. Not dependent on the 
    Biosample type.
    
    Parameters
    ----------
    action_type : str  
        Defines the action block of the .xml file. Can be either bs or bs_sra.
    config_template : str
        Path to the .xml file containing base for action type.
    submission_format_output : str
        The name of the file to output the formatted base .xml file.
    json_config : dict
        User set parameters for submission configuration.
    """
    
    #reconfigure this its dumb
    if 'sra' in action_type:
        format_sra=True
    else:
        format_sra=False
    if 'bs' in action_type:
        format_biosample=True
    else:
        format_biosample=False
    
   
    submission_template_location = os.path.join("/home/alab/data/ncbi_batch_push","xml_package_template", config_template)
    print(submission_template_location) 
    tree = et.parse(submission_template_location)
    root = tree.getroot() #submission level
   
    #submission level formatting
    comment = root.find("Description/Comment")
    comment.text = "New test submission, BioSample and SRA"    

    release_hold = root.find("Description/Hold")
    release_hold.attrib['release_date'] = json_config['submission']['hold']
    
    organization = root.find("Description/Organization/Name")
    organization.text = json_config['submission']["organization"]
    
    contact = root.find("Description/Organization/Contact")
    contact.attrib["email"] = json_config['submission']["email"]
    
    first_name = root.find("Description/Organization/Contact/Name/First")
    first_name.text = json_config['submission']["first_name"]
    
    last_name = root.find("Description/Organization/Contact/Name/Last")
    last_name.text = json_config['submission']["last_name"]
    
    #if we'd like to format a biosample addition block
    if format_biosample:
        identifier = root.find("Action/AddData/Identifier/SPUID")
        identifier.attrib["spuid_namespace"] = json_config['submission']["spuid_namespace"]

        spuid = root.find("Action/AddData/Data/XmlContent/BioSample/SampleId/SPUID")
        spuid.attrib["spuid_namespace"] = json_config['submission']["spuid_namespace"]
                
        title = root.find("Action/AddData/Data/XmlContent/BioSample/Descriptor/Title")
        title.text = json_config['submission']["title"]
                
        organism = root.find("Action/AddData/Data/XmlContent/BioSample/Organism/OrganismName")
        organism.text = json_config['submission']["organism"]
                
        bioproject = root.find("Action/AddData/Data/XmlContent/BioSample/BioProject/PrimaryId")
        bioproject.text = json_config['submission']["bioproject_id"]
               
        package = root.find("Action/AddData/Data/XmlContent/BioSample/Package") 
        package.text = json_config['submission']["package"]

    #if we'd like to format an sra action block
    if format_sra:
        primary_id = root.find("Action/AddFiles/AttributeRefId/RefId/PrimaryId")
        print(primary_id.tag, primary_id.attrib)
        if primary_id.attrib['db'] == "BioProject":
            primary_id.text = json_config['submission']["bioproject_id"]
        spuid = root.find("Action/AddFiles/AttributeRefId/RefId/SPUID")
        spuid.attrib["spuid_namespace"] = json_config["submission"]["spuid_namespace"]
        
        identifier = root.find("Action/AddFiles/Identifier/SPUID")
        identifier.attrib["spuid_namespace"] = json_config["submission"]["spuid_namespace"]         
        

    tree.write("%s" %(submission_format_output))

def validate(xml_path: str, xsd_path: str, verbose : bool) -> bool:
    import xmlschema 
    schema = xmlschema.XMLSchema(xsd_path)

    if verbose:
        schema.validate(xml_path)
    
    results = schema.is_valid(xml_path)
    return(results)

def file_presence(file_dir, sample_objs, file_type, action_name):
    """
    Locates the files from the metadata and returns
    filenames that are found. If sample name mapping is needed
    between the metadata and the sample files will call to a
    different function and createa mapping file.
 
    Parameters
    ----------
    file_dir : str
        Directory where files should be stored.
    metadata_filename : str
        Full filepath to metadata used for upload.
    file_type : str
        The extension of the file used.
    action_name : str
        The name of the project to be used as an output dir.
    
    Returns
    -------
    found_files : list
        List of files in the metadata that have been found locally.
    """
    return_sample_objs = []
    for sample in sample_objs:
        #if we have a filename we check for it 
        if str(sample.file_name) != "Not Found":
            if os.path.exists(os.path.join(file_dir, str(sample.file_name)+file_type)):
                sample.file_download_status = 'Present'
                sample.full_filepath = os.path.join(file_dir,str(sample.file_name)+file_type)
        else:
            sample.file_download_status = 'Absent'
        return_sample_objs.append(sample)
    
    
    return(return_sample_objs)

def internal_filename_mapping(fasta_filenames, sample_objs, file_type):
    """
    Takes two lists of files and tries to map the names to each other.
    
    Parameters
    ----------
    fasta_filenames : list
    local_files : list
    file_typ : str
    
    Returns
    -------
    return_sample_objs : list
        List of sample objects that have been added to.
    """
    fasta_filenames = [x.replace(file_type,"") for x in fasta_filenames]
    fasta_mapping = {}
    
    for filename in fasta_filenames:
        components = filename.split("-")
        components = "_".join(components)
        components = components.split('.')
        components = "_".join(components)
        components = components.split("_")
        for c in components:
            if c.isdigit():
                fasta_mapping[c] = filename
    
    return_sample_objs = []    
    for sample in sample_objs:
        components = sample.sample_name.split("-")
        components = '_'.join(components)
        components = components.split("_")
        for c in components:    
            if c.isdigit():
               if c in fasta_mapping:
                    sample.file_name = fasta_mapping[c]

               else:
                    sample.file_name = "Not Found"
        return_sample_objs.append(sample)
    return(return_sample_objs) 

def get_all_sample_names(metadata_location : str) -> list:
    """
    Takes in a metadata file and returns all the sample names.

    Parameters
    ----------
    metadata_location : str
        Location of metadata file.
    
    Returns
    -------
    all_sample_names : list
        A list of all sample names in the metdata.
    """
    df = pd.read_csv(metadata_location)
    all_sample_names = df['sample_name'].tolist()
    return(all_sample_names)

def define_batches(iterable, n=1):
    """
    Support function to create batches.
    """
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]

def find_sample_names(sample_objs, file_type, download_files, local_download_dir, \
    bucket_name, blob_name):
    """
    Either check google cloud location for filenames or check
    local directory where the files are stored to map the sample names.
    
    Parameters
    ----------
    sample_objs : list
        A list of all the sample objects we'd like to upload.
    file_type : str
        The extension/filetype we're trying to upload.
    download_files : bool
        Whether we have the files locally or if we need to download them.
    local_download_dir : str
        Either the location of the files or the place we will download to.
    bucket_name : str
        The name of the bucket where files would be stored on google cloud.
    blob_name : str
        Name of the blob where files would be located on google cloud.
    """
    if download_files:
        pass
    else:
        all_potential_filenames = os.listdir(local_download_dir) 
        sample_objs = internal_filename_mapping(all_potential_filenames, sample_objs, file_type)
    return(sample_objs)

def dump_objects(out_dir, action_name, sample_objs):
    with open(os.path.join(out_dir, action_name,"file_info.pickle"), 'wb') as pfile:
        for obj in sample_objs:
            pickle.dump(obj, pfile)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('config_filename')
    args = parser.parse_args()
    config_filename = args.config_filename
    
    #parse json config file
    json_config = open_config_file(config_filename)
    original_action_name = json_config['project_name']['action_name']
    action_type = json_config['project_name']['action_type']
    submission_type = json_config['project_name']['submission_type'] 
    batch_submission = ast.literal_eval(json_config['project_name']['batch_submission'])
    target_sample_names = ast.literal_eval(json_config['file_download_info']['target_sample_names'])    

    local_download_dir = json_config['file_download_info']['local_download_dir']
    credentials_path = json_config['file_download_info']['credentials_path']
    bucket_name = json_config['file_download_info']['bucket_name']
    blob_name = json_config['file_download_info']['blob_name']
    multiprocess= ast.literal_eval(json_config['file_download_info']['multiprocess'])
    download_files = ast.literal_eval(json_config['file_download_info']['download_files'])
    file_type = json_config['sra']['file_type'] 

    metadata_location = json_config['file_download_info']['metadata_location']
    
    #hard coded batch size if used
    if batch_submission:
        batch = 8000
    else:
        batch = 1
    if target_sample_names is None:
        all_sample_names = get_all_sample_names(metadata_location)
    else:
        all_sample_names = target_sample_names

    #don't use these, they're nanopore
    reserve = ["SEARCH-103878",\
    "SEARCH-5574-SAN",\
    "SEARCH-5742-SAN",\
    "SEARCH-5743-SAN"]

    all_sample_names = [item for item in all_sample_names if item not in reserve]  
    # print(all_sample_names)
    now = datetime.now()
    dt_string = now.strftime("%Y-%m-%d")

    #large loop to batch out sample
    for count, sample_names in enumerate(define_batches(all_sample_names, batch)):
        # if count <= 12:
        #     continue
        #create list of sample objects
        sample_objs = [Sample(sample_name) for sample_name in sample_names]

        print("Beginning batch %s of %s" %(count, round(len(all_sample_names)/batch)))

        #define the batch start/end samples non-inclusive
        start_batch = str(count*batch)
        end_batch = str((count+1)*batch)
        action_name = original_action_name + '_' + dt_string + '_' + start_batch + '_' + end_batch
        
        #map samples names to filenames, prior to downloading from google cloud or anything else
        sample_objs = find_sample_names(sample_objs, file_type, download_files, local_download_dir, \
            bucket_name, blob_name)
        # print(os.path.join(os.path.dirname(metadata_location), action_name))
        #check is this action name has been used before
        if os.path.isdir(os.path.join(os.path.dirname(metadata_location), action_name)):
            pass 
        else:
            os.system("mkdir %s" %os.path.join(os.path.dirname(metadata_location), action_name))    

        #this whole block can be reworked to accomodate SRA google cloud connection.
        #fetch files if it's an sra submission and they aren't local
        if 'sra' in action_type and download_files:
            #first we get the bam files from google cloud   
            google_cloud_bam(local_download_dir, credentials_path, file_type, all_sample_names,\
                bucket_name, blob_name, multiprocess)

            #if we're doing sra submission check for .bam file presence
        sample_objs = file_presence(local_download_dir, sample_objs, file_type, action_name)

        #checks the file download status
        # sample_objs = file_presence(local_download_dir, sample_objs, file_type, action_name)
        
        one_useful_sample=False
        #make sure the batch has some useable samples
        for sample in sample_objs:
            # print(str(sample.file_name))
            if str(sample.file_download_status) == 'Present':
                one_useful_sample=True
                break
        #     else:
        #         print(sample.__dict__)
        if not one_useful_sample:
            continue            
        #dump objects
        dump_objects(os.path.dirname(metadata_location),action_name, sample_objs)
                
        print("The total # of samples found is %s" %len(sample_objs))

        valid_action_types = {'bs': 'bs.submission.xml', 'sra':'sra.submission.run.xml', 'bs_sra':'sra.submission.bs.run.xml'}
        
        #formats the basics of the submission and action blocks
        submission_xml(action_type, valid_action_types[action_type], "submission_format.xml", json_config)
        
        #format the xml file for submission
        format_xml(metadata_location, sample_objs, "submission_format.xml", action_name, "bs_sra", \
            file_type, json_config)        

        #validates the xml file and returns any problem areas
        results = validate("submission.xml", "/home/alab/data/ncbi_batch_push/xml_package_template/submission_verification.xsd", True)
        
        # sys.exit(0) 
        #connects via ftp to ncbi and uploads the files in the xml 
        # ftp_connection(submission_type, action_name, sample_objs)

if __name__ == "__main__":
    main()


