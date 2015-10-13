#!/usr/bin/env python
# -*- coding: utf-8 -*-

''' 
 get gpfs stats from a gpfs client and insert them to influxdb using the line protocol
 https://influxdb.com/docs/v0.9/write_protocols/line.html

 for details in these values check  "Monitoring GPFS I/O performance with the mmpmon command" 
 in the GPFS: Advanced Administration Guide.

 Pablo Escobar - pablo.escobarlopez [{AT]} unibas.ch - http://scicore.unibas.ch
'''

import os
import commands
import time
import socket
import sys
import urllib2

# path to mmpmon binary which is used to query the metrics
mmpmon_path = '/usr/lpp/mmfs/bin/mmpmon'

# InfluxDB server connection details
# influxdb 0.9.3 or higher required (int values followed by a trailing i are not supported in influxdb <= 0.9.2)
# details here: https://influxdb.com/docs/v0.9/write_protocols/write_syntax.html
INFLUXDB_SERVER = 'sysmon01'
INFLUXDB_PORT = 8086
INFLUXDB_DBNAME = 'test2'
INFLUXDB_USER = 'root'
INFLUXDB_PASSWD = 'root'


def main():

    global_stats = get_gpfs_global_stats()
    stats_by_fs = get_gpfs_stats_by_fs()
    now = int(time.time())
    lines = []

    hostname = global_stats['gpfs_node_hostname']
    #print global_stats

    # global perf stats. All filesystems
    # for global stats filesystem name is hardcoded to "all_fs" and gpfs_cluster is hardcoded to "all"
    for key, value in global_stats.iteritems():
        if key is not 'gpfs_node_hostname':
            values = (str(key), "hostname="+hostname, "fs=all_fs", "gpfs_cluster=all", "value_int="+str(value)+"i", str(now))
            values = '{0},{1},{2},{3} {4} {5}'.format(*values)
            #print values
            lines.append((values))

    # by filesystem perf stats
    for fs in stats_by_fs:
        for key, value in fs.iteritems():
            if key is not 'gpfs_node_hostname' and key is not 'gpfs_cluster' and key is not 'fs_name':
                gpfs_cluster = fs['gpfs_cluster']
                #gpfs_cluster = gpfs_cluster.replace(".","_")
                fs_name = fs['fs_name']
                values = (str(key), "hostname="+hostname, "fs="+fs_name, "gpfs_cluster="+gpfs_cluster, "value_int="+str(value)+"i", str(now))
                values = '{0},{1},{2},{3} {4} {5}'.format(*values)
                #print values
                lines.append((values))

    message = '\n'.join(lines) + '\n'

    # comment out this print statement for debugging what will be sent to influxdb
    #print message  
    send_to_influxdb(message)

    # reset counters provided by mmpmon so next execution of the script we get values
    # just for the latest period
    reset_gpfs_counters()


def get_gpfs_global_stats():
    """ returns a dictionary with the global gpfs statistics (for all filesystems) """

    cmd = 'echo io_s | %s -s -p' % mmpmon_path
    gpfs_stats = commands.getoutput(cmd).split()
    #print gpfs_stats

    gpfs_node_hostname = gpfs_stats[4]

    # _br_ Total number of bytes read, from both disk and cache
    bytes_read = gpfs_stats[12]
    # convert bytest to megabytes and remove decimals
    megabytes_read = int((float(bytes_read)/float(1024))/float(1024))

    # _bw_ Total number of bytes written, to both disk and cache.
    bytes_written = gpfs_stats[14]
    # convert bytes to megabytes and remove decimals
    megabytes_written = int((float(bytes_written)/float(1024))/float(1024))

    # Count of open() call requests serviced by GPFS. The open count also includes creat() call counts.
    open_call_requests = gpfs_stats[16]

    # _cc_ Number of close() call requests serviced by GPFS.
    close_call_requests = gpfs_stats[18]

    #_rdc_ Number of application read requests serviced by GPFS.
    app_read_requests = gpfs_stats[20]

    #_wc_ Number of application write requests serviced by GPFS.
    app_write_requests = gpfs_stats[22]

    # _dir_ Number of readdir() call requests serviced by GPFS.
    readdir_call_requests = gpfs_stats[24]

    # _iu_ Number of inode updates to disk. This includes inodes flushed to disk because of access time updates.
    inodes_updates = gpfs_stats[26]

    return {'gpfs_node_hostname': gpfs_node_hostname,
            'megabytes_read': megabytes_read, 
            'megabytes_written': megabytes_written, 
            'open_call_requests': open_call_requests,
            'close_call_requests': close_call_requests,
            'app_read_requests': app_read_requests,
            'app_write_requests': app_write_requests,
            'readdir_call_requests': readdir_call_requests,
            'inodes_updates': inodes_updates,
            }

def get_gpfs_stats_by_fs():
    """ returns a list of dictionaries.
    Each dictionary contains the stats for one filesytem """

    cmd = 'echo fs_io_s | %s -s -p' % mmpmon_path
    gpfs_stats_by_fs = commands.getoutput(cmd).split('\n')
    stats_by_fs = [] 

    for fs in gpfs_stats_by_fs:
        fs_stats = fs.split()
        #print fs_stats

    
        gpfs_node_hostname = fs_stats[4]
        #print gpfs_node_hostname

        # _cl_ Name of the cluster that owns the file system.
        gpfs_cluster = fs_stats[12]
        #print gpfs_cluster

        # _fs_ The name of the file system for which data are being presented.
        fs_name = fs_stats[14]
        #print fs_name

        # _br_ Total number of bytes read, from both disk and cache.
        bytes_read = fs_stats[18]
        #print bytes_read
        megabytes_read = int((float(bytes_read)/float(1024))/float(1024))
        
        # _bw_ Total number of bytes written, to both disk and cache.
        bytes_written = fs_stats[20]
        #print bytes_written
        megabytes_written = int((float(bytes_written)/float(1024))/float(1024))

        # _oc_ Count of open() call requests serviced by GPFS. This also includes creat() call counts
        open_call_requests = fs_stats[22]
        #print open_call_requests

        # _cc_ Number of close() call requests serviced by GPFS.
        close_call_requests = fs_stats[24]
        #print close_call_requests


        # _rdc_ Number of application read requests serviced by GPFS.
        app_read_requests = fs_stats[26]
        #print app_read_requests

        # _wc_ Number of application write requests serviced by GPFS.
        app_write_requests = fs_stats[28]
        #print app_write_requests

        # _dir_ Number of readdir() call requests serviced by GPFS.
        readdir_call_requests = fs_stats[30]
        #print readdir_call_requests

        # _iu_ Number of inode updates to disk. This includes inodes flushed to disk because of access time updates.
        inodes_updates = fs_stats[32]
        #print inodes_updates

        fs_stats_dict = {'gpfs_node_hostname': gpfs_node_hostname,
                        'gpfs_cluster': gpfs_cluster, 
                        'fs_name': fs_name, 
                        'megabytes_read': megabytes_read, 
                        'megabytes_written': megabytes_written, 
                        'open_call_requests': open_call_requests,
                        'close_call_requests': close_call_requests,
                        'app_read_requests': app_read_requests,
                        'app_write_requests': app_write_requests,
                        'readdir_call_requests': readdir_call_requests,
                        'inodes_updates': inodes_updates,
                        }
        
        stats_by_fs.append(fs_stats_dict)

        #print fs_stats_dict
    return stats_by_fs
 

def reset_gpfs_counters():
    cmd = 'echo reset | %s -s -p &> /dev/null' % mmpmon_path
    os.system(cmd) 


def send_to_influxdb(message):
    """ send metrics to influxdb """
    try:

        req = urllib2.Request('http://%s:%s/write?db=%s&u=%s&p=%s&precision=s' % (INFLUXDB_SERVER, INFLUXDB_PORT, INFLUXDB_DBNAME, INFLUXDB_USER, INFLUXDB_PASSWD))
        req.add_data(message)
        res=urllib2.urlopen(req)

    except (urllib2.HTTPError,urllib2.URLError) as e:
        print 'error connecting to influxdb'
        print e
 
if __name__ == "__main__":
    main()
