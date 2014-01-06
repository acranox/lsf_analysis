#!/usr/bin/env python

# Analyze Memory usage on Orchestra
# Peter Doherty, peter_doherty@hms.harvard.edu
# Jan 06 2013

# jobid         = pos[0]
# indexid       = pos[1]
# user          = pos[2]
# num_cpus      = pos[3]
# mem_used      = pos[4]
# mem_reserved  = pos[5]

import argparse
import sys
import os
import csv

usage_info = '''usage: %s <options>
at a minimum, specify --infile <file>''' % sys.argv[0]


if len(sys.argv) <= 1:
    print usage_info
    exit()

parser = argparse.ArgumentParser(description=usage_info, formatter_class=argparse.RawTextHelpFormatter)

parser.add_argument('--infile',
                type=str,
                dest='infile',
                help='input file (required)')

args = parser.parse_args()
if args.infile and not os.path.isfile(args.infile):
    print "%s isn't a file!" % args.infile
    exit(1)

#def build_list(input_file):
#    input_fn = input_file
#    input_fh = open(input_fn, "r")
#    d_jobs  = {}
#    for row in input_fh:
#        line    = row.split(None) 
#        jobid   = int(line[0])
#        indexid = int(line[1])
#        user    = str(line[2])
#        n_cpu   = int(line[3])
#        m_used  = int(line[4])
#        if int(line[5]) == 0:
#            m_rsv   = 2097152
#        else:
#            m_rsv   = int(line[5])
#        print row
#    input_fh.close()

def read_tsv(input_file):
    input_fh    = open(input_file, "r")
    reader      = csv.reader(input_fh, delimiter="\t")
    l_jobs      = list(reader)
    return l_jobs

def make_user_dicts(list_of_jobs):
    d_userjobs  = {}
    for line in list_of_jobs[1:]:
        user    = str(line[2])
        if d_userjobs.has_key(user):
            d_userjobs[user].append(line)
        elif not d_userjobs.has_key(user):
            d_userjobs[user] = [line]
    return d_userjobs

def read_user_jobs(l_userjobs):
    l_badjobs   = []
    for line in l_userjobs:
        jobid   = int(line[0])
        indexid = int(line[1])
        user    = str(line[2])
        n_cpu   = int(line[3])
        m_used  = int(line[4])
        if int(line[5]) == 0:
            m_rsv   = 2097152
        else:
            m_rsv   = int(line[5])
        if (m_rsv*n_cpu)/10 > m_used:
            l_badjobs.append(line)
    return l_badjobs

#build_list(args.infile)
l_jobs      = read_tsv(args.infile)
d_userjobs  = make_user_dicts(l_jobs)
d_user_badjobs  = {}
for user in d_userjobs.keys():
    d_user_badjobs[user]    = read_user_jobs(d_userjobs[user])

for user in d_user_badjobs.keys():
    numjobs = len(d_user_badjobs[user]) 
    if numjobs > 0:
        print "%-4s - %s" % (user, numjobs)
