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

parser.add_argument('--pend',
                action='store_true',
                dest='pend',
                help='report pend times')

parser.add_argument('--ssusp',
                action='store_true',
                dest='ssusp',
                help='report ssusp times')

parser.add_argument('--percent',
                action='store_true',
                dest='percent',
                help='report as percentage of runtime')



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
    l_percent       = []
    for row in input_fh:
        line        = row.split(None) 
        pend_t      = int(line[10])
        run_t       = int(line[12])
        ssusp_t     = int(line[14])
        if run_t > 0 and args.pend:
            l_answers.append(pend_t)
            l_percent.append(float(pend_t)/(pend_t+run_t)*100)
        elif run_t > 0 and args.ssusp:
            l_answers.append(ssusp_t)
            l_percent.append(float(ssusp_t)/(ssusp_t+run_t)*100)
    gc.enable()
    return l_answers, l_percent

print "name,min,max,avg"
for datafile in l_infiles:
    fucking_answers     = fucking_magic(datafile)[0]
    fucking_percents    = fucking_magic(datafile)[1]
    dataname        = datafile.replace('.out','')
    if not args.percent:
        print "%s,%s,%s,%s" % (dataname,min(fucking_answers),max(fucking_answers),sum(fucking_answers)/len(fucking_answers))
    elif args.percent:
        print "%s,%0.2f,%0.2f,%0.2f" % (dataname,min(fucking_percents),max(fucking_percents),sum(fucking_percents)/len(fucking_percents))
