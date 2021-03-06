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
import matplotlib
matplotlib.use("agg")
import matplotlib.pyplot as plt

mem_bins    = [0,0.1,0.25,0.8,1.05,float("inf")]

usage_info = '''Display histograms of memory utilization.
The --infile option is required.  All others are optional'''



if len(sys.argv) <= 1:
    print usage_info
    exit()

parser = argparse.ArgumentParser(description=usage_info, formatter_class=argparse.RawTextHelpFormatter)

parser.add_argument('--infile',
                type=str,
                nargs='+',
                required=True,
                dest='infile',
                help='input file (separate multiple files with spaces)')

parser.add_argument('--minjobs',
                type=int,
                dest='minjobs',
                default=10,
                help='minimum number of jobs to display a line (default = 10)')

parser.add_argument('--minrsv',
                type=int,
                dest='minrsv',
                default=512,
                help='minimum memory reservation to display results ( in MBs )')

parser.add_argument('--nodefault',
                action="store_true",
                dest='nodefault',
                help='don\'t display jobs with default memory reservation')

parser.add_argument('--graphs',
                action="store_true",
                dest='graphs',
                help='save graphs as png files in ./graphs/')


args = parser.parse_args()

if not os.path.exists("./graphs"):
    os.makedirs("./graphs")

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
    d_users     = {}
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
        if not d_users.has_key(user): 
            d_users[user]      = {user:0,'r':[],'o':[],'y':[],'g':[],'p':[]}
        if d_users.has_key(user) and m_rsv > args.minrsv*1024: 
            d_users[user][user]   += 1
            coords          = "%0.1f,%0.1f" % (((m_rsv*n_cpu)/1048576.0),m_used/1048576.0)
#            print jratio
#            print coords
            if jratio <= 0.1:
                if coords not in d_users[user]['r']:
                    d_users[user]['r'] += [coords]
            elif jratio > 0.1 and jratio <= 0.25:
                if coords not in d_users[user]['o']:
                    d_users[user]['o'] += [coords]
            elif jratio > 0.25 and jratio <= 0.8:
                if coords not in d_users[user]['y']:
                    d_users[user]['y'] += [coords]
            elif jratio > 0.8 and jratio <= 1.05:
                if coords not in d_users[user]['g']:
                    d_users[user]['g'] += [coords]
            elif jratio > 1.05:
                if coords not in d_users[user]['p']:
                    d_users[user]['p'] += [coords]

    return d_users, d_results

l_jobs      = read_tsv(l_infiles)
d_udicts    = make_user_dicts(l_jobs)
d_uusers    = d_udicts[0]
d_uresults  = d_udicts[1]

def print_results():
    print "user            -  <10%  - 10-25% - 25-79% - 80-105% - >105%"
    for user in sorted(d_uresults.keys()):
        u_result    = np.histogram(d_uresults[user], bins=mem_bins)
        if sum(u_result[0]) > args.minjobs:
            print "%-15s - %-6d - %-6d - %-6d - %-6d  - %-6d" % (user, u_result[0][0], u_result[0][1], u_result[0][2], u_result[0][3], u_result[0][4])

#print d_uusers['cp172']['y']

def make_plot():
    for user in sorted(d_uusers.keys()):
        d_colors    = {'r':[],'o':[],'y':[],'g':[],'p':[]}
        d_x    = {'r':[],'o':[],'y':[],'g':[],'p':[]}
        d_y    = {'r':[],'o':[],'y':[],'g':[],'p':[]}
        if d_uusers[user][user] > args.minjobs:
            for color in d_colors.keys():
                for coords in d_uusers[user][color]:
                    d_colors[color] = coords.split(',')
                    d_x[color].append(d_colors[color][0])
                    d_y[color].append(d_colors[color][1])
            plot_file   = "./graphs/"+user+".png"
            plt.figure(0)
            plt.ylabel("Memory used (GB)")
            plt.xlabel("Memory reserved (GB)")
            plt.suptitle("Memory usage for %s \n red=<10%% orange=10-25%% yellow=25-80%% green=80-105%% magenta=>105%%" % user)
            plt.grid(True)
            plt.scatter(d_x['r'],d_y['r'],c='r',marker='o')
            plt.scatter(d_x['o'],d_y['o'],c='#ffa500',marker='o')
            plt.scatter(d_x['y'],d_y['y'],c='y',marker='o')
            plt.scatter(d_x['g'],d_y['g'],c='g',marker='o')
            plt.scatter(d_x['p'],d_y['p'],c='m',marker='o')
            plt.plot([0,100],[0,100])
            plt.xlim(0,100)
            plt.ylim(0,100)
            plt.draw()
            plt.savefig(plot_file, dpi=300)
            plt.close()

print_results()
if args.graphs:
    make_plot()
