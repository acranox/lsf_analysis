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
import numpy as np

mem_bins    = [0,0.1,0.25,0.8,1.05,float("inf")]

usage_info = '''usage: %s <options>
at a minimum, specify --infile <file>''' % sys.argv[0]


if len(sys.argv) <= 1:
    print usage_info
    exit()

parser = argparse.ArgumentParser(description=usage_info, formatter_class=argparse.RawTextHelpFormatter)

parser.add_argument('--infile',
                type=str,
                nargs='+',
                dest='infile',
                help='input file (required) (separate multiple files with spaces)')

parser.add_argument('--minjobs',
                type=int,
                dest='minjobs',
                default=10,
                help='minimum number of jobs to display a line (default = 10) (optional)')

parser.add_argument('--minrsv',
                type=int,
                dest='minrsv',
                default=512,
                help='minimum memory reservation to display results ( in MBs ) (optional)')

parser.add_argument('--nodefault',
                action="store_true",
                dest='nodefault',
                help='don\'t display jobs with default memory reservation (optional)')

args = parser.parse_args()

#print args.infile

#l_infiles   = args.infile.split(" ")
l_infiles   = args.infile

for infile in l_infiles:
    if infile and not os.path.isfile(infile):
        print "%s isn't a file!" % infile
        exit(1)

def read_tsv(l_infiles):
    '''read in the data, generate a list'''
    l_jobs      =   []
    for infile in l_infiles:
        input_fh    = open(infile, "r")
        reader      = csv.reader(input_fh, delimiter="\t")
        l_jobs.extend(list(reader)[1:])
#    print len(l_jobs)
    return l_jobs

def make_user_dicts(list_of_jobs):
    '''create up to two keys per user with the calcuated usage ratio.  jobs within a reservation, get put into the user.default key'''
    d_results   = {}
#    for line in list_of_jobs[1:]:
    for line in list_of_jobs:
        jobid   = int(line[0])
        indexid = int(line[1])
        user    = str(line[2])
        n_cpu   = float(line[3])
        m_used  = int(line[4])
        m_rsv   = int(line[5])
        if not args.nodefault and m_rsv == 0:
            jratio  = (m_used/(2097152*n_cpu))
            if d_results.has_key(user+".default"):
                d_results[user+".default"].append(jratio)
            elif not d_results.has_key(user+".default"):
                d_results[user+".default"] = [jratio]
        elif m_rsv > args.minrsv*1024: 
            jratio  = (m_used/(m_rsv*n_cpu))
            if d_results.has_key(user):
                d_results[user].append(jratio)
            elif not d_results.has_key(user):
                d_results[user] = [jratio]
    return d_results

l_jobs      = read_tsv(l_infiles)
d_uresults  = make_user_dicts(l_jobs)

def print_results():
    print "user            -  <10%  - 10-25% - 25-79% - 80-105% - >105%"
    for user in sorted(d_uresults.keys()):
        u_result    = np.histogram(d_uresults[user], bins=mem_bins)
        if sum(u_result[0]) > args.minjobs:
            print "%-15s - %-6d - %-6d - %-6d - %-6d  - %-6d" % (user, u_result[0][0], u_result[0][1], u_result[0][2], u_result[0][3], u_result[0][4])

print_results()
