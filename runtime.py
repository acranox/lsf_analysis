#!/usr/bin/python

# Do LSF analysis
# Peter Doherty, peter_doherty@hms.harvard.edu
# Oct 3 2013

#        jobid           = pos[0]
#        user            = pos[1]
#        exitstatus      = pos[2]
#        queue           = pos[3]
#        mem_used        = pos[4]
#        mem_reserved    = pos[5]
#        cpu_used        = pos[6]
#        efficiency      = pos[7]
#        num_cpus        = pos[8]
#        pend_time       = pos[9]
#        psusp_time      = pos[10]
#        run_time        = pos[11]
#        ususp_time      = pos[12]
#        ssusp_time      = pos[13]
#        unkwn_time      = pos[14]
#        dependency      = pos[15]

import argparse
import gc
import math
import matplotlib.pyplot as plt
import os
import sys
#import pylab

usage_info = 'usage: %s data_file.out' % sys.argv[0] 

gc.disable()

if len(sys.argv) <= 1:
    print usage_info
    exit()
parser = argparse.ArgumentParser(description=usage_info)

parser.add_argument('--user', '-u',
                type=str,
                dest='u',
                help='The user id to look for')

parser.add_argument('--queue', '-q',
                type=str,
                dest='q',
                help='The queue to look for')

parser.add_argument('--infile', '-i',
                type=str,
                dest='infile',
                help='input file')

parser.add_argument('--minrun', '-m',
                type=int,
                dest='minrun',
                help='minumum runtime')

parser.add_argument('--maxrun', '-x',
                type=int,
                dest='maxrun',
                help='maximum runtime')
                   
args = parser.parse_args()

d_pos = {
    'jobid':    [0,'int'],
    'user':     [1,'str'],
    'exit':     [2,'int'],
    'queue':    [3,'str'],
    'm_used':   [4,'makeintorzero'],
    'm_rsv':    [5,'mungemrsv'],
    'c_used':   [6,'float'],
    'eff':      [7,'float'],
    'n_cpu':    [8,'int'],
    'pend_t':   [9,'int'],
    'psusp_t':  [10,'int'],
    'run_t':    [11,'int'],
    'ususp_t':  [12,'int'],
    'ssusp_t':  [13,'int'],
    'unkwn_t':  [14,'int'],
    'dep':      [15,'str']
}

def makeintorzero(v):
    try:
        n = int(v)
    except:
        n = 0
    return n

def mungemrsv(m):
    if m == 0:
        m = 2097152/1024
    else:
       m = m/1024
    return m

def create_list(datafile):
    output_dir      = datafile.replace('.out','')
    input_fn        = datafile
    input_fh        = open(input_fn, "r")
    l_bin           = []
    for row in input_fh:
        pos             = row.split(None)
        l_bin.append(pos)

    input_fh.close()

    return l_bin

def filter_generic(data_bin,f_opt,compare,f_arg):
    databin     = []
    result      = data_bin
    pos_p       = d_pos[f_opt][0]
    pos_t       = d_pos[f_opt][1]
    for line in result:
        if pos_t == 'int':
            opt = int(line[pos_p])
        elif pos_t == 'str':
            opt = str(line[pos_p])
        if compare == 'min':
            if opt >= f_arg:
                databin.append(line)
        elif compare == 'max':
            if opt <= f_arg:
                databin.append(line)
        if compare == 'eq':
            if opt == f_arg:
                databin.append(line)
    return databin

def filter_q(data_bin,q):
    q_databin   = []
    result      = data_bin
    q_in        = q
    for line in result:
        q       = line[3]
        if q == q_in:
            q_databin.append(line)
    return q_databin

def filter_u(data_bin,u):
    u_databin   = []
    result      = data_bin
    u_in        = u
    for line in result:
        u       = line[1]
        if u == u_in:
            u_databin.append(line)
    return u_databin

def filter_list(data_bin,arglist):
    u   = arglist.u
    q   = arglist.q
    if u and q:
        u_result    = filter_u(data_bin,u) 
        uq_result    = filter_q(u_result,q) 
        l_filter_result = uq_result
    elif u and not q:
        u_result    = filter_u(data_bin,u) 
        l_filter_result = u_result
    elif not u and q:
        q_result    = filter_q(data_bin,q) 
        l_filter_result = q_result
    else:
        l_filter_result = data_bin 
    return l_filter_result

def calc(data_bin):
    result      = data_bin
    l_cpu       = []
    l_r         = []
    l_mrsv      = []
    l_mused     = []
    l_ncpu      = []
    for line in result:
#        jid     = line[0]
        u       = line[1]
#        exit    = line[2]
#        q       = line[3]
        m_used  = makeintorzero(line[4])
        m_rsv   = mungemrsv(int(line[5]))
        c_used  = float(line[6])
#        eff     = line[7]
        n_cpu   = int(line[8])
#        pend_t  = line[9]
#        psusp_t = line[10]
        run_t   = int(line[11])
#        run_t   = line[11]
#        ususp_t = line[12]
#        ssusp_t = line[13]
#        unk_t   = line[14]
#        dep     = line[15]
    
        l_cpu.append(c_used)
        l_r.append(run_t)
        l_mrsv.append(m_rsv)
        l_mused.append(m_used/1024)
        l_ncpu.append(n_cpu)
    return l_cpu, l_r, l_mrsv, l_mused, l_ncpu

l_parsed    = create_list(args.infile)
#l_filter    = filter_list(l_parsed,args)

def loop_args(l_input,d_args):
    l_filtered  = l_input 
    for opt,arg in d_args.iteritems():
        if arg and opt == "u":
            l_filtered  = filter_generic(l_filtered,'user','eq',arg)
            print len(l_filtered)
        elif arg and opt == "q":
            l_filtered  = filter_generic(l_filtered,'queue','eq',arg)
            print len(l_filtered)
        elif arg and opt == "minrun":
            l_filtered  = filter_generic(l_filtered,'run_t','min',arg)
            print len(l_filtered)
        elif arg and opt == "maxrun":
            l_filtered  = filter_generic(l_filtered,'run_t','max',arg)
            print len(l_filtered)
    return l_filtered

d_args = args.__dict__
print len(l_parsed)
l_filtered = loop_args(l_parsed,d_args)
print len(l_filtered)

l_result    = calc(l_filtered)
n_jobs      = len(l_result[0])
c_total     = sum(l_result[0])/3600.0 

if args.u and args.q:
    print "%s queue, user %s: %0.1f cpu hours and %d jobs" % (args.q,args.u,c_total,n_jobs)
elif args.u and not args.q:
    print "user %s: %0.1f cpu hours and %d jobs" % (args.u,c_total,n_jobs)
elif not args.u and args.q:
    print "%s queue: %0.1f cpu hours and %d jobs" % (args.q,c_total,n_jobs)

def hist_cused(l_input):
    plt.figure(0)
    plt.ylabel('number of jobs')
    plt.xlabel('cpu usage (sec)')
    plt.grid(True)
    plt.suptitle(args.infile)
    plt.hist(l_input, bins=[0,60,3600,14400,43200,86400,604800,2592000])
#    plt.hist(l_input)
    plt.draw()

def hist_ncpu(l_input):
    plt.figure(1)
    plt.ylabel('number of jobs')
    plt.xlabel('number of cores reserved')
    plt.grid(True)
    plt.suptitle(args.infile)
    plt.hist(l_input, bins=[1,2,4,8,12,50])
    plt.draw()

def hist_runt(l_input):
    plt.figure(2)
    plt.ylabel('number of jobs')
    plt.xlabel('run time (sec)')
    plt.grid(True)
    plt.suptitle(args.infile)
    plt.hist(l_input)
    plt.draw()

def hist_mrsv(l_input):
    plt.figure(3)
    plt.ylabel('number of jobs')
    plt.xlabel('memory reservation (mb)')
    plt.grid(True)
    plt.suptitle(args.infile)
#    plt.hist(l_input, bins=[2048,4096,8192,32768,65536])
    plt.hist(l_input, bins=len(set(l_input)))
    plt.draw()

def hist_mused(l_input):
    plt.figure(4)
    plt.ylabel('number of jobs')
    plt.xlabel('memory used (mb)')
    plt.grid(True)
    plt.suptitle(args.infile)
    plt.hist(l_input, bins=[512,2048,4096,8192,32768,65536])
#    plt.hist(l_input, bins=len(set(l_input)))
    plt.draw()



hist_cused(l_result[0])
hist_runt(l_result[1])
hist_mrsv(l_result[2])
hist_mused(l_result[3])
hist_ncpu(l_result[4])
plt.show()
