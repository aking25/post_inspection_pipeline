process CREATE_TABLE {
    input:
    path sra_meta

    output:
    path("source.src")

    shell:
    '''
    genbank.py -s !{sra_meta}
    '''
}

process CREATE_FSA {
    output:
    path("sequences.reformat.fsa")

    shell:
    '''
    for f in !{params.bjorn_folder}/msa/consensus_sequences/*; do
        cat $f >> sequences.fasta
    done
    convert_fasta_id.py sequences.fasta
    '''
}

process ZIP_FILES {
    input:
    path sequences
    path source

    output:
    env genbank_folder

    shell:
    date = new Date().format("yyyy-MM-dd")
    '''
    submit_genbank.py -o ./ -f !{sequences} -s !{params.sbt_template} -x !{params.xml_template}
    genbank_folder=!{params.out_dir}/genbank_!{params.out_folder}
    mkdir -p $genbank_folder
    cp *.zip $genbank_folder
    cp submission.xml $genbank_folder
    '''
}

process UPLOAD_GENBANK {
    container 'ascp:latest'
    shell '/bin/bash'
    input:
    path genbank_folder
    path aspera
    path ssh_key

    shell: 
    '''
    !{aspera}/ascp -i !{ssh_key} -QT -l100m -k1 -d !{genbank_folder} asp-search@upload.ncbi.nlm.nih.gov:!{params.aspera_folder}
    touch !{genbank_folder}/submit.ready
    !{aspera}/ascp -i !{ssh_key} -QT -l100m -k1 -d !{genbank_folder} asp-search@upload.ncbi.nlm.nih.gov:!{params.aspera_folder}
    '''
}