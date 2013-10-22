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
import datetime
import csv
import gc
import math
import numpy as np
import os
import sys
import time

usage_info = '''usage: %s <options>
        at a minimum, specify --infile <file.out>''' % sys.argv[0] 


if len(sys.argv) <= 1:
    print usage_info
    exit()

parser = argparse.ArgumentParser(description=usage_info, formatter_class=argparse.RawTextHelpFormatter)

parser.add_argument('--infile',
                type=str,
                dest='infile',
                help='input file (required)')

parser.add_argument('--users',
                type=str,
                dest='u',
                help='list of users (comma separated)')

parser.add_argument('--queues',
                type=str,
                dest='q',
                help='''Either: a list of queues (comma separated)
or one(1) of: contrib,shared,all
or a queue name with a wildcard as the last character, ie: sysbio*''')

parser.add_argument('--minrun',
                type=int,
                dest='minrun',
                help='minumum runtime (seconds)')

parser.add_argument('--maxrun',
                type=int,
                dest='maxrun',
                help='maximum runtime (seconds)')

parser.add_argument('--exitzero',
                action="store_true",
                dest='exitzero',
                help='only include jobs that had an exit status of 0 (zero)')

parser.add_argument('--nojobdepend',
                action="store_true",
                dest='nojobdepend',
                help='exclude jobs with dependencies')

parser.add_argument('--nosumusers',
                action="store_true",
                dest='nosumusers',
                help='''generate totals for each user, instead of processing them collectively (includes graphs and csv output)''')

parser.add_argument('--graphs',
                type=str,
                dest='graphs',
                help='''one or more of: all,cpu,memory,suspend''')

parser.add_argument('--savegraphs',
                action="store_true",
                dest='savegraphs',
                help='''save graphs as PNG files''')

parser.add_argument('--showgraphs',
                action="store_true",
                dest='showgraphs',
                help='''display graphs interactively''')

parser.add_argument('--csv',
                action="store_true",
                dest='csv',
                help='output to csv files')

parser.add_argument('--quiet',
                action="store_true",
                dest='quiet',
                help='do not print results to stdout.  for use with --csv, or --savegraphs')

parser.add_argument('--outdir',
                type=str,
                dest='outdir',
                help='''the output directory for saving csv and png files''')

parser.add_argument('--prefix',
                type=str,
                dest='prefix',
                help='''a optional file name prefix for csv and png files''')

parser.add_argument('--debug',
                action="store_true",
                dest='debug',
                help='print timing info for debugging purposes')




args = parser.parse_args()

if args.infile and not os.path.isfile(args.infile):
    print "%s isn't a file!" % args.infile
    exit(1)

# Load MatPlotLib if we're going to use it, and set the backend appropriately
if args.savegraphs and not args.showgraphs:
    import matplotlib
    matplotlib.use("agg")
    import matplotlib.pyplot as plt
elif not args.savegraphs and args.showgraphs or args.graphs:
    import matplotlib.pyplot as plt

# Queue definitions
d_queues   = {
    'contrib': ['bpf_12h','bpf_15m','bpf_1d','bpf_2h','bpf_7d','bpf_int_12h','bpf_int_2h','bpf_unlimited','cbdm_12h','cbdm_2h','cbdm_int_12h','cbdm_unlimited','cbi_12h','cbi_1d','cbi_7d','cbi_int_12h','cbi_unlimited','cbmi_12h','cbmi_15m','cbmi_2h','cbmi_int_12h','cbmi_int_15m','cbmi_int_2h','cbmi_unlimited','church_12h','church_15m','church_1d','church_2h','church_7d','church_int_12h','church_int_15m','church_int_2h','church_unlimited','danuser_12h','danuser_1d','danuser_2h','danuser_7d','danuser_int_12h','danuser_int_15m','danuser_int_2h','danuser_int_7d','danuser_unlimited','deisboeck_unlimited','erdogmus_12h','erdogmus_1d','erdogmus_unlimited','freedberg_12h','freedberg_1d','freedberg_2h','freedberg_7d','freedberg_unlimited','hcra_7d','hcra_unlimited','i2b2_12h','i2b2_15m','i2b2_1d','i2b2_2h','i2b2_7d','i2b2_int_12h','i2b2_int_15m','i2b2_int_2h','i2b2_unlimited','kreiman_12h','kreiman_15m','kreiman_1d','kreiman_2h','kreiman_7d','kreiman_int_12h','kreiman_int_15m','kreiman_int_2h','kreiman_unlimited','kung_12h','kung_int_12h','kung_unlimited','megason_12h','megason_15m','megason_1d','megason_2h','megason_7d','megason_int_12h','megason_int_2h','megason_unlimited','merfeld_12h','merfeld_int_12h','merfeld_unlimited','mgh-ita_unlimited','mini','nezafat_12h','nezafat_15m','nezafat_2h','nezafat_int_12h','nezafat_int_2h','nezafat_unlimited','nowak_12h','nowak_1d','nowak_2h','nowak_7d','nowak_unlimited','park_12h','park_15m','park_1d','park_2h','park_7d','park_int_12h','park_int_15m','park_int_2h','park_unlimited','reich','reich_12h','reich_15m','reich_1d','reich_2h','reich_7d','reich_int_12h','reich_int_15m','reich_int_2h','reich_unlimited','rodeo_12h','rodeo_15m','rodeo_unlimited','seidman','sorger_12h','sorger_15m','sorger_1d','sorger_2h','sorger_7d','sorger_int_2d','sorger_int_2h','sorger_par_1d','sorger_par_unlimited','sorger_unlimited','sysbio_12h','sysbio_15m','sysbio_1d','sysbio_2h','sysbio_7d','sysbio_int_15m','sysbio_int_2d','sysbio_int_2h','sysbio_par_1d','sysbio_unlimited','tonellato','usheva_15m','usheva_1d','usheva_unlimited'],
    'shared': ['all_12h','all_15m','all_1d','all_2h','all_7d','all_unlimited','experimental','gpu_unlimited','highmem_unlimited','interactive','long','lost_and_found','mini','mpi','no_suspend','parallel','priority','shared_12h','shared_15m','shared_1d','shared_2h','shared_7d','shared_int_12h','shared_int_15m','shared_int_2h','shared_lenny','shared_mini','shared_par_unlimited','shared_unlimited','short','smp','testing','tmp_unlimited','training']
}


# Dictionary that defines the structure of the data file
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

# Dictionary with the parameters for the various graphs.
# figure number, graph type, name, y-axis, x-axis, title, bins, normalized
d_figs = {
    'cpu_usage': [0,'hist','cpu_usage','Number of Jobs','CPU Usage (hours per job)','Histogram of CPU Usage',range(0,720,1),False],
    'runtime': [1,'hist','runtime','Number of Jobs','Wall Clock Time (sec per job)','Histogram of Job Run Times',range(0,2592000,60),False],
    'mem_reserved': [2,'hist','mem_reserved','Number of Jobs','Memory Reserved (GB per core)','Histogram of Memory Reservations',range(0,96,2),False],
    'mem_used': [3,'hist','mem_used','Number of Jobs','Memory Used (GB per job)','Histogram of Memory Usage',range(0,64,1),False],
    'ncpu': [4,'bar','number_cores','Number of Jobs','Number of Cores Reserved','Histogram of Core Reservation',range(1,128,1),False],
    'eff': [5,'hist','efficiency','Number of Jobs','Job Efficiency ((CPU Usage*Cores)/RunTime)','Histogram of Job Efficiency',range(10,500,20),False],
    'memdelta': [6,'hist','mem_delta','Number of Jobs','(Mem. Reserved) - (Mem. Used)','Histogram of Memory Efficiency',range(-8,64,2),False],
    'memscat': [7,'scatter','mem_scat','Mem. Used (MB)','Mem. Reserved (MB)','Scatter Plot of Memory Efficiency',False,False],
    'runpct': [8,'hist','runpct','Number of Jobs','Run / Total','Histogram of Percent of time spent in RUN',range(0,500,10),False],
    'susppct': [9,'hist','susppct','Number of Jobs','Susp+Run / Run ','Histogram of Percent of time spent in SSUSP',range(0,500,10),False]
    }

# Dictionary for groups of graphs, to simplify argument handling.
d_fig_groups = {
    'cpu': ['cpu_usage','runtime','ncpu','eff'],
    'memory': ['mem_reserved','mem_used','memdelta','memscat'],
    'suspend': ['runpct','susppct']
    }

def makeintorzero(v):
    '''Convert a string into an int if possible, or set it to 0'''
    try:
        n = int(v)
    except:
        n = 0
    return n

def mungemrsv(m):
    '''If memory reservation was zero, set it to 2 (GB), otherwise convert the value from KB to GB'''
    if m == 0:
        m = 2
    else:
       m = m/1048576
    return m

def make_out_fn(out_fn):
    '''Set the output file path and file name based on the outdir and prefix arguments'''
    if args.prefix:
        out_fn      = args.prefix+"_"+out_fn
    if args.outdir:
        if not os.path.exists(args.outdir):
            os.makedirs(args.outdir)
        out_fn      = args.outdir+"/"+out_fn
    return out_fn

def create_filtered_list(datafile,d_args):
    '''Create a list from the input file, filterd based on arguments'''
    gc.disable()
    input_fn        = datafile
    input_fh        = open(input_fn, "r")
    d_filt_result   = {'jobs':[]}
    u_list          = []
    q_list          = []
    q_regex         = False
    # create default args, and parse the rest
    if d_args['u']:
        unames = d_args['u'].split(",")
    else:
        unames = []
    if d_args['q'] and d_args['q'] == "contrib":
        qnames   = d_queues['contrib']
    elif d_args['q'] and d_args['q'] == "shared":
        qnames   = d_queues['shared']
    elif d_args['q'] and d_args['q'][-1] == '*':
        q_regex = True
        q_pattern = d_args['q'][:-1] 
        qnames = d_args['q'].split(",")
    elif d_args['q']:
        qnames = d_args['q'].split(",")
    else:
        qnames = []
    if d_args['minrun']:
        minrun = d_args['minrun']
    else:
        minrun = 0
    if d_args['maxrun']:
        maxrun = d_args['maxrun']
    else:
        maxrun = sys.maxint
      
    for row in input_fh:
        line            = row.split(None) 
        user            = str(line[1])
        exitstatus      = int(line[2])
        queue           = str(line[3])
        run_t           = int(line[11])
        depend          = str(line[15])
        u_list.append(user)
        q_list.append(queue)
        if user in unames or not unames:
            ufilt = True
        else:  
            ufilt = False
        if queue in qnames or not qnames:
            qfilt = True
        elif q_regex and queue.startswith(q_pattern):
            qfilt = True
        else:
            qfilt = False
        if d_args['exitzero'] and exitstatus != 0:
            exfilt = False
        else:
            exfilt = True
        if d_args['nojobdepend'] and depend != 'nojobdepend':
            depfilt = False
        else:
            depfilt = True

        if ufilt and qfilt and exfilt and depfilt and run_t >= minrun and run_t <= maxrun:
                d_filt_result['jobs'].append(line)
    d_filt_result['u'] = list(set(u_list))
    d_filt_result['q'] = list(set(q_list))
 
    input_fh.close()
    gc.enable()
    return d_filt_result

def calc(data_bin):
    ''' Build lists of the key data using the filtered input'''
    gc.disable()
    result      = data_bin
    d_calc      = dict((key,[]) for key in d_figs.keys())
    for line in result:
#        jid     = line[0]
        u       = line[1]
#        exit    = line[2]
#        q       = line[3]
        m_used  = makeintorzero(line[4])
        m_rsv   = mungemrsv(int(line[5]))
        c_used  = float(line[6])
        eff     = int(float(line[7]))
        n_cpu   = int(line[8])
        pend_t  = int(line[9])
        psusp_t = int(line[10])
        run_t   = int(line[11])
        ususp_t = int(line[12])
        ssusp_t = int(line[13])
#        unk_t   = line[14]
#        dep     = line[15]
        d_calc['cpu_usage'].append(c_used/3600.0)
        d_calc['runtime'].append(run_t)
        d_calc['mem_reserved'].append(m_rsv*n_cpu)
        d_calc['mem_used'].append(m_used/1048576.0)
        d_calc['ncpu'].append(n_cpu)
        d_calc['eff'].append(eff)
        d_calc['memdelta'].append(m_rsv-m_used)
        d_calc['memscat'].append([m_rsv,m_used/1048576.0])
        if run_t > 0:
            d_calc['runpct'].append(float(run_t)/float(pend_t+psusp_t+ususp_t+ssusp_t+run_t)*100)
            d_calc['susppct'].append(float(ssusp_t)/(ssusp_t+run_t)*100)
    gc.enable()
    return d_calc

def filter_string(argdict):
    '''Based on the arguments passed in, build a human-readable string of which filters were used'''
    l_args = []
    if argdict['q']:
        l_args.append('Queues: %s' % argdict['q'])
    if argdict['u']:
        l_args.append('Users: %s' % argdict['u'])
    if argdict['minrun']:
        l_args.append('Min. Runtime: %s' % argdict['minrun'])
    if argdict['maxrun']:
        l_args.append('Max. Runtime: %s' % argdict['maxrun'])
    if argdict['exitzero']:
        l_args.append('Exclude jobs w/ non-zero exit values')
    if argdict['nojobdepend']:
        l_args.append('Exclude jobs w/ dependencies')
     if len(l_args) == 0:
        s_filter = "No data filters applied"
    else:
        s_filter = ", ".join(l_args)
    return s_filter 


def create_dict(l_parsed):
    q_dict  = {}
    u_dict  = {}
    for line in l_parsed:
        u       = str(line[1])
        q       = str(line[3])
        if q_dict.has_key(q):
            q_dict[q].append(line)
        elif not q_dict.has_key(q):
            q_dict[q] = [line]
        if u_dict.has_key(u):
            u_dict[u].append(line)
        elif not u_dict.has_key(u):
            u_dict[u] = [line]
    return q_dict, u_dict

def new_print_results(user,d_results):
    if not args.csv:
        n_jobs      = len(d_results['cpu_usage'])
        c_total     = sum(d_results['cpu_usage'])
        print "users: %s\n%0.1f cpu hours and %d jobs" % (user,c_total,n_jobs)
    elif args.csv:
        n_jobs      = len(d_results['cpu_usage'])
        c_total     = sum(d_results['cpu_usage'])
        print "%s,%0.1f,%d" % (user,c_total,n_jobs)

def make_csv(user,d_results):
    for metric in d_results.keys():
        out_fn      = make_out_fn(user+"_"+metric+".csv")
        out_fh      = open(out_fn, 'w')
        out_csv     = csv.writer(out_fh, delimiter='\n')
        out_fh.write("%s,%s\n" % (user,metric))
        out_csv.writerow(sorted(d_results[metric]))
        out_fh.close()

# Functions for creating a histogram
def draw_hist(l_input,user,l_result,save):
    gc.disable()
    fignum = l_input[0]
    figname = l_input[2]
    figfile = make_out_fn('%s_%s.png' % (user,l_input[2]))
    figylab = l_input[3]
    figxlab = l_input[4]
    figtit = '%s - (source: %s)\n%s' % (l_input[5],args.infile, filter_names)
    figdata = l_result
    figbins = l_input[6]
    fignorm = l_input[7]
    plt.figure(fignum)
    plt.ylabel(figylab)
    plt.xlabel(figxlab)
    plt.grid(True)
    plt.suptitle(figtit)
    plt.hist(figdata, bins=figbins, normed=fignorm)
    plt.draw()
    if save:
        plt.savefig(figfile, dpi=300)
        plt.close()
    gc.enable()

# Functions for creating a bar graph
def draw_bar(l_input,user,l_result,save):
    gc.disable()
    fignum  = l_input[0]
    figfile = make_out_fn('%s_%s.png' % (user,l_input[2]))
    figylab = l_input[3]
    figxlab = l_input[4]
    figtit  = '%s - (source: %s)\n%s' % (l_input[5],args.infile, filter_names)
    figdata = l_result
    figbins = l_input[6]
    fignorm = l_input[7]
    if fignorm:
        figdata_y, figdata_x = np.histogram(figdata, bins=figbins, density=True)
    elif not fignorm:
        figdata_y, figdata_x = np.histogram(figdata, bins=figbins)
    figdata_x = figdata_x[:-1]
    if fignorm:
        figdata_y *= 100
    plt.figure(fignum)
    plt.ylabel(figylab)
    plt.xlabel(figxlab)
    plt.grid(True)
    plt.suptitle(figtit)
#    plt.xlim(figdata_x[0],figdata_x[-1])
#    barwidth = float(((figdata_x[-1]-figdata_x[0])/len(figdata_x))/2)
    plt.bar(figdata_x,figdata_y)
    plt.draw()
    if save:
        plt.savefig(figfile, dpi=300)
    gc.enable()

# Function for drawing a Scatter plot
def draw_scatter(l_input,user,l_result,save):
    gc.disable()
    fignum  = l_input[0]
    figfile = make_out_fn('%s_%s.png' % (user,l_input[2]))
    figylab = l_input[3]
    figxlab = l_input[4]
    figtit  = '%s - (source: %s)\n%s' % (l_input[5],args.infile, filter_names)
    figdata_x,figdata_y = zip(*l_result)
    plt.figure(fignum)
    plt.ylabel(figylab)
    plt.xlabel(figxlab)
#    plt.xlim(0,max(figdata_x+figdata_y))
#    plt.ylim(0,max(figdata_x+figdata_y))
    plt.xlim(0,max(figdata_y))
    plt.ylim(0,max(figdata_y))
    plt.grid(True)
    plt.suptitle(figtit)
#    plt.xlim(figdata_x[0],figdata_x[-1])
#    plt.axes().set_aspect('equal', adjustable='datalim', anchor='SW')
    plt.scatter(figdata_x,figdata_y,c='b',marker='.')
    plt.plot([0,100],[0,100])
    plt.draw()
    if save:
        plt.savefig(figfile, dpi=300)
        plt.close()
    gc.enable()

def make_graphs(user,d_uresults):
    l_graph = []
    if args.debug:
        t_start = time.time()
    if args.graphs == "all" or not args.graphs:
        l_graph = d_figs.keys()
    else:
        ggrp = args.graphs.split(",")
        for gname in ggrp:
            if gname in d_fig_groups.keys():
                l_graph.extend(d_fig_groups[gname])             
    for graph in l_graph:
        if d_figs[graph][1] == "scatter":
            draw_scatter(d_figs[graph],user,d_uresults[graph],args.savegraphs)
        elif d_figs[graph][1] == "hist":
            draw_hist(d_figs[graph],user,d_uresults[graph],args.savegraphs)
        elif d_figs[graph][1] == "bar":
            draw_bar(d_figs[graph],user,d_uresults[graph],args.savegraphs)
    if args.debug:
        t_fin = time.time()
        print "finished making graphs in: %8.2f seconds." % (t_fin-t_start)
    if args.showgraphs or args.graphs:
        plt.show()

u_merged    = []
d_args      = args.__dict__

# Run the filter_string function to get a human readable string of the filters that were applied
filter_names = filter_string(d_args)

if args.debug:
    t_start = time.time()
    d_filtered  = create_filtered_list(args.infile,d_args)
    t_fin   = time.time()
    print "create_filtered_list() completed in: %8.2f seconds." % (t_fin-t_start)
    t_start = time.time()
    d_result    = create_dict(d_filtered['jobs'])
    t_fin   = time.time()
    print "create_dict() completed in: %8.2f seconds." % (t_fin-t_start)
elif not args.debug:
    d_filtered  = create_filtered_list(args.infile,d_args)
    d_result    = create_dict(d_filtered['jobs'])

q_dict      = d_result[0]
u_dict      = d_result[1]

if args.debug:
    t_start = time.time()

for user in u_dict.keys():
    u_merged.extend(u_dict[user])

if args.debug:
    t_fin = time.time()
    print "new_print_results() and make_csv() completed in: %8.2f seconds." % (t_fin-t_start)

if args.nosumusers:
    d_umerged   = calc(u_merged)
    if args.csv:
        print "user,cpu_hours,numjobs"
    for user in u_dict.keys():
        d_uresult   = calc(u_dict[user])
        if not args.csv and not args.quiet:
            new_print_results(user,d_uresult)
        elif args.csv:
            new_print_results(user,d_uresult)
            make_csv(user,d_uresult)
            make_csv('total',d_umerged)
        if args.showgraphs or args.savegraphs or args.graphs:
            make_graphs(user,d_uresult)
    if not args.quiet:
        new_print_results('total',d_umerged)
elif not args.nosumusers:
    d_umerged = calc(u_merged)
    if not args.csv and not args.quiet:
        new_print_results('total',d_umerged)
    elif args.csv:
        print "user,cpu_hours,numjobs"
        new_print_results('total',d_umerged)
        make_csv('total',d_umerged)
    if args.showgraphs or args.savegraphs or args.graphs:
        make_graphs('total',d_umerged)

