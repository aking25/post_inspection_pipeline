process PREP_SRA_FILES {
    publishDir "${params.out_dir}", mode: 'link', pattern: 'submission.xml'
    input:
    path meta

    output:
    env bam_folder
    env ascp
    env ssh_key

    shell:
    date = new Date().format("yyyy-MM-dd")
    '''
    bam_folder=!{params.out_dir}/sra_!{params.out_folder}
    ascp=!{params.aspera}
    ssh_key=!{params.ssh_key}
    mkdir -p $bam_folder
    if [ "$(ls -A !{params.bjorn_folder}/bam_inspect/)" ]; then ln !{params.bjorn_folder}/bam_inspect/*.bam ${bam_folder}; fi
    if [ "$(ls -A !{params.bjorn_folder}/bam_white/)" ]; then ln !{params.bjorn_folder}/bam_white/*.bam ${bam_folder}; fi
    prep_bam.py !{meta} ${bam_folder}
    write_config.py !{meta} ${bam_folder} !{params.instrument_model} !{params.first_name} \
        !{params.last_name} !{params.email} !{params.organization} !{params.spuid_namespace} \
        !{params.title} !{params.organism} !{params.organism_package} !{params.bioproject} \
        !{params.library_layout}
    submit_ncbi.py job_config.json
    cp submission.xml $bam_folder
    '''
}

process PREP_SRA_META {
    publishDir "${params.out_dir}", mode: 'link'
    input:
    val start

    output:
    path "ncbi_metadata.csv"

    shell:
    '''
    convert_meta.py !{params.bjorn_folder}/gisaid_metadata.csv !{params.bioproject} !{params.metadata}
    '''
}

process UPLOAD_FILES {
    container 'ascp:latest'
    input:
    path bam_folder
    path aspera
    path ssh_key

    shell: 
    '''
    !{aspera}/ascp -i !{ssh_key} -QT -l100m -k1 -d !{bam_folder} asp-search@upload.ncbi.nlm.nih.gov:!{params.aspera_folder}
    touch !{bam_folder}/submit.ready
    !{aspera}/ascp -i !{ssh_key} -QT -l100m -k1 -d !{bam_folder} asp-search@upload.ncbi.nlm.nih.gov:!{params.aspera_folder}
    '''
}
