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
    l_range1   = []
    l_range2   = []
    l_range3   = []
    l_range4   = []
    l_range5   = []
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
        if m_used < int((m_rsv*n_cpu)*.10):
            l_range1.append(line)
        elif m_used >= int((m_rsv*n_cpu)*.10) and m_used < int((m_rsv*n_cpu)*.30): 
            l_range2.append(line)
        elif m_used >= int((m_rsv*n_cpu)*.30) and m_used < int((m_rsv*n_cpu)*.50): 
            l_range3.append(line)
        elif m_used >= int((m_rsv*n_cpu)*.50) and m_used < int((m_rsv*n_cpu)*.80): 
            l_range4.append(line)
        elif m_used >= int((m_rsv*n_cpu)*.80):
            l_range5.append(line)

    return l_range1, l_range2, l_range3, l_range4, l_range5

#build_list(args.infile)
l_jobs      = read_tsv(args.infile)
d_userjobs  = make_user_dicts(l_jobs)
d_user_badjobs  = {}
for user in d_userjobs.keys():
    d_user_badjobs[user]    = read_user_jobs(d_userjobs[user])

print "user         -  <10%  - 10-30% - 30-50% - 50-80% - >80%"
for user in d_user_badjobs.keys():
    n1 = len(d_user_badjobs[user][0])
    n2 = len(d_user_badjobs[user][1])
    n3 = len(d_user_badjobs[user][2])
    n4 = len(d_user_badjobs[user][3])
    n5 = len(d_user_badjobs[user][4])
    if sum([n1,n2,n3,n4,n5]) > 10:
        print "%-12s - %-6d - %-6d - %-6d - %-6d - %-6d" % (user, n1, n2, n3, n4, n5)
