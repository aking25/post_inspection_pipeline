#!/usr/bin/env nextflow
nextflow.enable.dsl=2

include { POST_INSPECTION } from './modules/post_inspection.nf'
include { PREP_SRA_META; PREP_SRA_FILES; UPLOAD_FILES } from './modules/sra.nf'
include { CREATE_TABLE; CREATE_FSA; ZIP_FILES; UPLOAD_GENBANK } from './modules/genbank.nf'

workflow {
    PREP_SRA_META('start') | PREP_SRA_FILES | UPLOAD_FILES
    CREATE_FSA()
    CREATE_TABLE(PREP_SRA_META.out)
    ZIP_FILES(CREATE_FSA.out, CREATE_TABLE.out)
    UPLOAD_GENBANK(ZIP_FILES.out, params.aspera, params.ssh_key)
}