#!/usr/bin/env python

import argparse
import gc
import sys
import os

usage_info = '''usage: %s <options>
at a minimum, specify --infile <file.out>''' % sys.argv[0]


if len(sys.argv) <= 1:
    print usage_info
    exit()

parser = argparse.ArgumentParser(description=usage_info, formatter_class=argparse.RawTextHelpFormatter)

parser.add_argument('--infile',
                type=str,
                nargs='+',
                required=True,
                dest='infile',
                help='input file (required)')

args = parser.parse_args()

l_infiles = args.infile

for infile in l_infiles:
    if infile and not os.path.isfile(infile):
        print "%s isn't a file!" % infile
        exit(1)


def fucking_magic(datafile):
    '''Create a list from the input file, filterd based on arguments'''
    gc.disable()
    input_fn = datafile
    input_fh = open(input_fn, "r")
    l_answers       = []
    for row in input_fh:
        line        = row.split(None) 
        pend_t      = int(line[10])
        run_t       = int(line[12])
        if run_t > 0:
            l_answers.append(pend_t)
    gc.enable()
    return l_answers

print "name,min,max,avg"
for datafile in l_infiles:
    fucking_answers = fucking_magic(datafile)
    dataname        = datafile.replace('.out','')
    print "%s,%s,%s,%s" % (dataname,min(fucking_answers),max(fucking_answers),sum(fucking_answers)/len(fucking_answers))
