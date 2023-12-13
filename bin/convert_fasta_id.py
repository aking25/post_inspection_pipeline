#!/usr/bin/env python

import sys

fasta = sys.argv[1]

def process_id(x):
    if not x.startswith("SEARCH"):
        start = x.index("SEARCH")
    return x[start:].split('/')[0]
    # return ''.join(x.split('-')[-2:])

fasta_dict = {}
with open(fasta, 'r') as f:
    for line in f:
        if line.startswith('>'):
            id = process_id(line.strip())
        else:
            fasta_dict[id] = line.strip()

with open('sequences.reformat.fsa', 'w') as f:
    for id, seq in fasta_dict.items():
        f.write(f">{id}\n{seq}\n")