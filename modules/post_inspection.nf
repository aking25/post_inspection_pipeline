process POST_INSPECTION {
    output:
    val 'done'

    conda "${params.bjorn_env}"
    shell:
    '''
    post_inspection_processing.py !{params.bjorn_folder} !{params.ucsd_meta}
    '''
}