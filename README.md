# Post Inspection Processing Pipeline
A Nextflow pipeline for uploading sequences to GISAID, Google Cloud, Github, and NCBI SRA after inspection.

## Usage
Set the following parameters in `nextflow.config`:
* `bjorn_folder`: the output folder from running `run_alab_release.sh`
* `ucsd_meta`: metadata from UCSD with zipcodes, leave as "" if not UCSD upload
* `out_dir`: the output directory for NCBI SRA files
* `bioproject`: bioproject for SRA upload
* `metadata`: metadata of samples being uploaded (in `HCoV-19-Genomics` format)
* `aspera`: directory containing ascp 
* `ssh_key`: private ssh key for ascp
* `aspera_folder`: folder to upload files to for SRA (*submit/Production*)
* `bjorn_env`: path to bjorn conda environment

Also set SRA-specific parameters, using `_` to replace any spaces.

Then run the command: 
```
nextflow run main.nf
```

### Notes
* There are a lot of hardcoded parameters currently in `write_config.py` that are not generalizable.
* `submit_ncbi.py` has many functions that aren't used, could reduce file to just essential functions.
* Relies on the docker container `ascp:latest` and conda environment `bjorn` 
* `post_inspection_processing.py` is a symlink to file in `bjorn_utils`
