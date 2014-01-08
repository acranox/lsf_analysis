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
    d_results   = {}
    for line in list_of_jobs[1:]:
        jobid   = int(line[0])
        indexid = int(line[1])
        user    = str(line[2])
        n_cpu   = float(line[3])
        m_used  = int(line[4])
        m_rsv   = int(line[5])
        if m_rsv == 0:
            jratio  = (m_used/(2097152*n_cpu))
            if d_results.has_key(user+".default"):
                d_results[user+".default"].append(jratio)
            elif not d_results.has_key(user+".default"):
                d_results[user+".default"] = [jratio]
        elif m_rsv > 524288:
            jratio  = (m_used/(m_rsv*n_cpu))
            if d_results.has_key(user):
                d_results[user].append(jratio)
            elif not d_results.has_key(user):
                d_results[user] = [jratio]
    return d_results

l_jobs      = read_tsv(args.infile)
d_uresults  = make_user_dicts(l_jobs)
mem_bins    = [0,0.1,0.5,0.8,1.0,float("inf")]
print "user            -  <10%  - 10-49% - 50-79% - 80-99% - >100%"
for user in sorted(d_uresults.keys()):
    u_result    = np.histogram(d_uresults[user], bins=mem_bins)
    if sum(u_result[0]) > 10:
        print "%-15s - %-6d - %-6d - %-6d - %-6d - %-6d" % (user, u_result[0][0], u_result[0][1], u_result[0][2], u_result[0][3], u_result[0][4])

#d_user_badjobs  = {}
#for user in d_userjobs.keys():
#    d_user_badjobs[user]    = read_user_jobs(d_userjobs[user])
#
#print "user         -  <10%  - 10-30% - 30-50% - 50-80% - 80-100% - >100%"
#for user in d_user_badjobs.keys():
#    n1 = len(d_user_badjobs[user][0])
#    n2 = len(d_user_badjobs[user][1])
#    n3 = len(d_user_badjobs[user][2])
#    n4 = len(d_user_badjobs[user][3])
#    n5 = len(d_user_badjobs[user][4])
#    n6 = len(d_user_badjobs[user][5])
#    if sum([n1,n2,n3,n4,n5,n6]) > 10:
#        print "%-12s - %-6d - %-6d - %-6d - %-6d - %-6d - %-6d" % (user, n1, n2, n3, n4, n5, n6)
