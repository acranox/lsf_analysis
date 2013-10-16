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
import os
import sys
import csv
import numpy as np

usage_info = '''usage: %s <options>
        at a minimum, specify --infile <file.out>''' % sys.argv[0] 

#gc.disable()

if len(sys.argv) <= 1:
    print usage_info
    exit()

parser = argparse.ArgumentParser(description=usage_info)

parser.add_argument('--users', '-u',
                type=str,
                dest='u',
                help='The user ids to look for(comma separated list)')

parser.add_argument('--queues', '-q',
                type=str,
                dest='q',
                help='The queue to look for(comma separated list)')

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

parser.add_argument('--exitzero', '-z',
                action="store_true",
                dest='exitzero',
                help='exclude jobs with non-zero exit codes')

parser.add_argument('--sumusers',
                action="store_true",
                dest='sumusers',
                help='if set, a sum total is calculated for the list of users, instead of processing them individually')

parser.add_argument('--graphs', '-g',
                type=str,
                dest='makegraphs',
                help='''Is one of: all, runtime, ncpu, mem_reserved, cpu_usage, memdelta, eff, mem_used''')

parser.add_argument('--savegraphs',
                action="store_true",
                dest='savegraphs',
                help='save graphs')

parser.add_argument('--showgraphs',
                action="store_true",
                dest='showgraphs',
                help='show graphs')

parser.add_argument('--csv', '-c',
                action="store_true",
                dest='csv',
                help='output in csv format')

parser.add_argument('--quiet',
                action="store_true",
                dest='quiet',
                help='supress certain output')


args = parser.parse_args()

if args.infile and not os.path.isfile(args.infile):
    print "%s isn't a file!" % args.infile
    exit(1)

# Queue definitions
q_contrib   = ['church_int_15m','danuser_int_15m','freedberg_int_15m','i2b2_int_15m','megason_int_15m','merfeld_int_15m','nezafat_int_15m','nowak_int_15m','park_int_15m','sorger_int_15m','sysbio_int_15m','usheva_int_15m','church_int_2h','danuser_int_2h','freedberg_int_2h','i2b2_int_2h','kreiman_int_2h','megason_int_2h','merfeld_int_2h','nezafat_int_2h','nowak_int_2h','park_int_2h','sorger_int_2h','sysbio_int_2h','usheva_int_2h','bpf_int_12h','cbi_int_12h','church_int_12h','danuser_int_7d','danuser_int_12h','freedberg_int_12h','i2b2_int_12h','kreiman_int_12h','megason_int_12h','merfeld_int_12h','nezafat_int_12h','nowak_int_12h','park_int_12h','sorger_int_2d','sysbio_int_2d','usheva_int_12h','bpf_15m','church_15m','danuser_15m','freedberg_15m','i2b2_15m','kreiman_15m','megason_15m','merfeld_15m','nezafat_15m','nowak_15m','park_15m','sorger_15m','sysbio_15m','usheva_15m','church_2h','danuser_2h','freedberg_2h','i2b2_2h','kreiman_2h','megason_2h','merfeld_2h','nezafat_2h','nowak_2h','park_2h','sorger_2h','sysbio_2h','usheva_2h','bpf_12h','cbi_12h','church_12h','danuser_12h','freedberg_12h','i2b2_12h','kreiman_12h','megason_12h','merfeld_12h','nezafat_12h','nowak_12h','park_12h','sorger_12h','sysbio_12h','usheva_12h','church_1d','danuser_1d','freedberg_1d','i2b2_1d','kreiman_1d','megason_1d','merfeld_1d','nezafat_1d','nowak_1d','park_1d','sorger_1d','sysbio_1d','usheva_1d','church_7d','danuser_7d','freedberg_7d','i2b2_7d','megason_7d','merfeld_7d','nezafat_7d','nowak_7d','park_7d','sorger_7d','sysbio_7d','usheva_7d','bpf_unlimited','cbi_unlimited','church_unlimited','danuser_unlimited','freedberg_unlimited','i2b2_unlimited','kreiman_unlimited','megason_unlimited','merfeld_unlimited','nezafat_unlimited','nowak_unlimited','park_unlimited','sorger_unlimited','sorger_par_unlimited','sorger_par_1d','sysbio_unlimited','sysbio_par_1d','usheva_unlimited','rodeo_15m','rodeo_12h','rodeo_unlimited','reich','seidman']
q_shared    = ['priority','interactive','mpi','mcore','parallel','short','long','mini']
q_all       = q_contrib+q_shared 

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
        m = 2
    else:
       m = m/1048576
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

def filter_generic(data_bin,d_args):
    d_filt_result   = {'jobs':[]}
    if d_args['u']:
        u_list = d_args['u'].split(",")
        # list comprehension!
        newu_list = [ user for user in u_list if user in d_uniq['u']]
    else:
        newu_list = d_uniq['u']
    if d_args['q'] and d_args['q'] == "contrib":
        qnames   = q_contrib
    elif d_args['q'] and d_args['q'] == "shared":
        qnames   = q_shared
    elif d_args['q'] and d_args['q'] == "all":
        qnames   = d_uniq['q']
    elif d_args['q']:
        qnames = d_args['q'].split(",")
    else:
        qnames = d_uniq['q']
    if d_args['minrun']:
        minrun = d_args['minrun']
    else:
        minrun = 0
    if d_args['maxrun']:
        maxrun = d_args['maxrun']
    else:
        maxrun = sys.maxint
      
    result      = data_bin
    for line in result:
        user            = str(line[1])
        exitstatus      = int(line[2])
        queue           = str(line[3])
        run_t           = int(line[11])
        if d_args['exitzero']:
            if user in newu_list and queue in qnames and run_t >= minrun and run_t <= maxrun and exitstatus == 0:
                d_filt_result['jobs'].append(line)
        elif not d_args['exitzero']:
            if user in newu_list and queue in qnames and run_t >= minrun and run_t <= maxrun:
                d_filt_result['jobs'].append(line)
    return d_filt_result

def find_uniques(data_bin):
    ''' A function for getting all the user names and queues present in the file
        and creating a dictionary with the values, using the keys "u" and "q" '''
    uniques         = {}
    u_list          = []
    q_list          = []
    for row in data_bin:
        username    = str(row[1])
        queue       = str(row[3])

        u_list.append(username)
        q_list.append(queue)

    uniques['u'] = list(set(u_list))
    uniques['q'] = list(set(q_list))

    return uniques


def calc(data_bin):
    result      = data_bin
    d_calc      = {'l_cpu': [], 'l_r':[], 'l_mrsv': [], 'l_mused': [], 'l_ncpu': [], 'l_eff': [], 'l_memdelta': [] }
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
#        pend_t  = line[9]
#        psusp_t = line[10]
        run_t   = int(line[11])
#        run_t   = line[11]
#        ususp_t = line[12]
#        ssusp_t = line[13]
#        unk_t   = line[14]
#        dep     = line[15]
        d_calc['l_cpu'].append(c_used/3600.0)
        d_calc['l_r'].append(run_t)
        d_calc['l_mrsv'].append(m_rsv*n_cpu)
        d_calc['l_mused'].append(m_used/1048576.0)
        d_calc['l_ncpu'].append(n_cpu)
        d_calc['l_eff'].append(eff)
        d_calc['l_memdelta'].append(m_rsv-m_used)
    return d_calc

def filter_string(argdict):
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
        l_args.append('Exclude Non-Zero exit values')
    if len(l_args) == 0:
        s_filter = "None"
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

def new_print_results(d_results,lu_merged):
    users_total = []
    if not args.csv:
        for user in d_results.keys():
            l_result    = calc(d_results[user])
            n_jobs      = len(l_result['l_cpu'])
            c_total     = sum(l_result['l_cpu'])
            print "users: %s\n%0.1f cpu hours and %d jobs" % (user,c_total,n_jobs)
    elif args.csv:
        print "user,cpu_hours,numjobs"
        for user in d_results.keys():
            l_result    = calc(d_results[user])
            n_jobs      = len(l_result['l_cpu'])
            c_total     = sum(l_result['l_cpu'])
            print "%s,%0.1f,%d" % (user,c_total,n_jobs)
    l_result    = lu_merged
    n_jobs      = len(l_result['l_cpu'])
    c_total     = sum(l_result['l_cpu'])
    print "total,%0.1f,%d" % (c_total,n_jobs)

def make_csv(d_results,lu_merged):
    for user in d_results.keys():
        l_result    = calc(d_results[user])
        for metric in l_result.keys():
            out_fn      = user+"_"+metric.replace("l_","")+".csv"
            out_fh      = open(out_fn, 'w')
#            out_csv     = csv.writer(out_fh, delimiter=',',quoting=csv.QUOTE_ALL)
            out_csv     = csv.writer(out_fh, delimiter='\n')
            out_fh.write("%s,%s\n" % (user,metric.replace("l_","")))
            out_csv.writerow(sorted(l_result[metric]))
            out_fh.close()
    for metric in lu_merged.keys():
        tot_fn      = "total_"+metric.replace("l_","")+".csv"
        tot_fh      = open(tot_fn, 'w')
        tot_csv     = csv.writer(tot_fh, delimiter='\n')
        tot_fh.write("total,%s\n" % (metric.replace("l_","")))
        tot_csv.writerow(sorted(lu_merged[metric]))
        tot_fh.close()


    

u_merged    = []
l_parsed    = create_list(args.infile)
d_uniq      = find_uniques(l_parsed)
d_args      = args.__dict__
d_filtered  = filter_generic(l_parsed,d_args)
d_result    = create_dict(d_filtered['jobs'])
q_dict      = d_result[0]
u_dict      = d_result[1]
for user in u_dict.keys():
    u_merged.extend(u_dict[user])
mrg_u_result    = calc(u_merged)
if not args.quiet:
    new_print_results(u_dict,mrg_u_result)
if args.csv:
    make_csv(u_dict,mrg_u_result)

def draw_hist(l_input,save):
    fignum = l_input[0]
    figname = l_input[2]
    figfile = '%s.png' % l_input[2]
    figylab = l_input[3]
    figxlab = l_input[4]
    figtit = '%s - (source: %s)\nFilters: %s' % (l_input[5],args.infile, filter_names)
    figdata = l_input[6]
    figbins = l_input[7]
#    figxticks = l_input[8]
    plt.figure(fignum)
    plt.ylabel(figylab)
    plt.xlabel(figxlab)
    plt.grid(True)
    plt.suptitle(figtit)
#    if figxticks:
#        plt.xticks(figxticks)
#    if figbins == "auto":
#        plt.hist(figdata, bins=len(set(figdata)))
    if figbins and figbins != "auto":
        plt.hist(figdata, bins=figbins)
    elif not figbins:
        plt.hist(figdata)
    plt.draw()
    if save:
        plt.savefig(figfile, dpi=300)


def draw_bar(l_input,save):
    fignum  = l_input[0]
    figfile = '%s.png' % l_input[1]
    figylab = l_input[2]
    figxlab = l_input[3]
    figtit  = '%s - (source: %s)\nFilters: %s' % (l_input[4],args.infile, filter_names)
    figbins = l_input[6]
    if figbins and figbins != "auto":    
        figdata_y, figdata_x = np.histogram(l_input[5], bins=figbins)
    elif not figbins or figbins == "auto":    
        figdata_y, figdata_x = np.histogram(l_input[5])
    figdata_x = figdata_x[:-1]
    plt.figure(fignum)
    plt.ylabel(figylab)
    plt.xlabel(figxlab)
    plt.grid(True)
    plt.suptitle(figtit)
    binlen = len(figdata_x)
    binwidth = 1.0/binlen
#    plt.xticks(range(int(figdata_x[0]),int(figdata_x[-1]),int((figdata_x[-1]-figdata_x[0])/len(figdata_x))),figdata_x)
#    plt.xticks(range(int(figdata_x[0]),int(figdata_x[-1]),int((figdata_x[-1]-figdata_x[0])/len(figdata_x))))
    plt.xticks(figdata_x)
    plt.xlim(figdata_x[0],figdata_x[-1])
    barwidth = float(((figdata_x[-1]-figdata_x[0])/len(figdata_x))/2)
    plt.bar(figdata_x,figdata_y,width=barwidth,align='edge')
#    plt.bar(figdata_x,figdata_y,width=1.0,align='center')
#    plt.bar(range(len(figdata_x)),figdata_y,width=1.0,align='center')
    plt.draw()

def draw_scatter(l_input,save):
    fignum  = l_input[0]
    figfile = '%s.png' % l_input[2]
    figylab = l_input[3]
    figxlab = l_input[4]
    figtit  = '%s - (source: %s)\nFilters: %s' % (l_input[5],args.infile, filter_names)
    figdata_y = l_input[6]
    figdata_x = l_input[7]
    plt.figure(fignum)
    plt.ylabel(figylab)
    plt.xlabel(figxlab)
    plt.grid(True)
    plt.suptitle(figtit)
#    plt.xlim(figdata_x[0],figdata_x[-1])
    plt.scatter(figdata_x,figdata_y,c='b',marker='.')
    plt.draw()



d_figs = {
    'cpu_usage': [0,'hist','cpu_usage','Number of Jobs','CPU Usage (hours per job)','Histogram of CPU Usage',mrg_u_result['l_cpu'],range(0,2592000,60)],
    'runtime': [1,'hist','runtime','Number of Jobs','Wall Clock Time (sec per job)','Histogram of Job Run Times',mrg_u_result['l_r'],range(0,2592000,60)],
    'mem_reserved': [2,'hist','mem_reserved','Number of Jobs','Memory Reserved (GB per core)','Histogram of Memory Reservations',mrg_u_result['l_mrsv'],range(0,65536,2048)],
    'mem_used': [3,'hist','mem_used','Number of Jobs','Memory Used (GB per job)','Histogram of Memory Usage',mrg_u_result['l_mused'],range(0,65536,2048)],
    'ncpu': [4,'hist','number_cores','Number of Jobs','Number of Cores Reserved','Histogram of Core Reservation',mrg_u_result['l_ncpu'],range(1,128,1)],
    'eff': [5,'hist','efficiency','Number of Jobs','Job Efficiency ((CPU Usage*Cores)/RunTime)','Histogram of Job Efficiency',mrg_u_result['l_eff'],range(10,500,20)],
    'memdelta': [6,'hist','mem_delta','Number of Jobs','(Mem. Reserved) - (Mem. Used)','Histogram of Memory Efficiency',mrg_u_result['l_memdelta'],range(-8192,65536,2048)],
    'memscat': [7,'scatter','mem_scat','Mem. Used (MB)','Mem. Reserved (MB)','Scatter Plot of Memory Efficiency',mrg_u_result['l_mused'],mrg_u_result['l_mrsv']]
    }

filter_names = filter_string(d_args)

if args.showgraphs or args.savegraphs or args.makegraphs:
    if args.savegraphs and not args.showgraphs:
        import matplotlib
        matplotlib.use("agg")
    import matplotlib.pyplot as plt
    if args.makegraphs == "all" or not args.makegraphs:
        l_graph = d_figs.keys()
    else:
        l_graph = args.makegraphs.split(",")
    for graph in l_graph:
        if d_figs[graph][1] == "scatter":
            draw_scatter(d_figs[graph],args.savegraphs)
        elif d_figs[graph][1] == "hist":
            draw_hist(d_figs[graph],args.savegraphs)
        elif d_figs[graph][1] == "bar":
            draw_bar(d_figs[graph],args.savegraphs)
    if args.showgraphs or args.makegraphs:
        plt.show()
