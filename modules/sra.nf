process PREP_SRA_FILES {
    publishDir "${params.out_dir}", mode: 'copy', pattern: 'submission.xml'
    input:
    path meta

    output:
    env bam_folder
    env ascp
    env ssh_key

    shell:
    date = new Date().format("yyyy-MM-dd")
    '''
    bam_folder=!{params.out_dir}/ncbi_bam_!{date}
    ascp=!{params.aspera}
    ssh_key=!{params.ssh_key}
    mkdir -p $bam_folder
    cp !{params.bjorn_folder}/bam_inspect/* ${bam_folder}
    cp !{params.bjorn_folder}/bam_white/* ${bam_folder}
    prep_bam.py !{meta} ${bam_folder}
    write_config.py !{meta} ${bam_folder}
    submit_ncbi.py job_config.json
    cp submission.xml $bam_folder
    '''

}

process PREP_SRA_META {
    publishDir "${params.out_dir}", mode: 'copy'
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
