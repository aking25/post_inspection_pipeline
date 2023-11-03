#!/usr/bin/env nextflow
nextflow.enable.dsl=2

include { POST_INSPECTION } from './modules/post_inspection.nf'
include { PREP_SRA_META; PREP_SRA_FILES; UPLOAD_FILES } from './modules/sra.nf'

workflow {
    PREP_SRA_META('start') | PREP_SRA_FILES | UPLOAD_FILES
}