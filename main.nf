#!/usr/bin/env nextflow
nextflow.enable.dsl=2

include { POST_INSPECTION } from './modules/post_inspection.nf'
include { PREP_SRA_META; PREP_SRA_FILES; UPLOAD_FILES } from './modules/sra.nf'

workflow {
    POST_INSPECTION | PREP_SRA_META | PREP_SRA_FILES | UPLOAD_FILES
}