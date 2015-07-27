#!/usr/bin/python

#      Copyright 2015, Schuberg Philis BV
#
#      Licensed to the Apache Software Foundation (ASF) under one
#      or more contributor license agreements.  See the NOTICE file
#      distributed with this work for additional information
#      regarding copyright ownership.  The ASF licenses this file
#      to you under the Apache License, Version 2.0 (the
#      "License"); you may not use this file except in compliance
#      with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#      Unless required by applicable law or agreed to in writing,
#      software distributed under the License is distributed on an
#      "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#      KIND, either express or implied.  See the License for the
#      specific language governing permissions and limitations
#      under the License.

# Script to search primary storage pools for 'detached disks' and remove
# them to free up space

import sys
import getopt
import math
import os.path
import random
import argparse

from cloudstackops import cloudstackops
from cloudstackops import cloudstackopsssh
from cloudstackops.cloudstackstorage import StorageHelper

from prettytable import PrettyTable

__version__ = "0.2"


def get_volume_filesize(file_uuid_in_cloudstack, *filelist):
    filelist, = filelist
    size = None
    for filepath in filelist.keys():
        file_uuid_on_storagepool = filepath.split('/')[-1].split('.')[:1][0]

        if file_uuid_in_cloudstack == file_uuid_on_storagepool:
            size = int(filelist[filepath])
    return size

# per script custom arguments


def add_custom_arguments(parser, required):
    # 'required' and 'parser' must be argparse ArgumentParser objects

    required.add_argument("-z", "--zone", help="Zone Name", required=True)

    parser.add_argument("-t", "--cluster", help="Cluster Name")
    parser.add_argument("-V", "--version", action='version',
                        version="%(prog)s (version " + __version__ + ")")

    # return parser object so we can parse_args()
    return parser


## MAIN ##

# Parse arguments
if __name__ == "__main__":

    description = "List Detached disks based on Volume Cloudstack api vs Storaaepools"

    # Init our classes
    c = cloudstackops.CloudStackOps()
    cs = cloudstackopsssh.CloudStackOpsSSH()

    parser, required = c.add_generic_arguments(description)
    try:
        parser = add_custom_arguments(parser, required)

    except Exception:
        pass

    finally:
        args = c.parse_arguments(parser)

    # assign custom options to globals
    zone = args.zone

    if args.cluster:
        clusterarg = args.cluster

    c.configProfileName = args.configprofile


# Init the CloudStack API
c.initCloudStackAPI()

if c.DEBUG == 1:
    print "DEBUG: API address: " + c.apiurl
    print "DEBUG: ApiKey: " + c.apikey
    print "DEBUG: SecretKey: " + c.secretkey

# Check cloudstack IDs
if c.DEBUG == 1:
    print "DEBUG: Checking CloudStack IDs of provided input.."

zoneid = c.getZoneId(zone)


if zoneid is None:
    print "Cannot find zone " + zone
    exit(1)

# get all clusters in zone if no cluster is given as input
if 'clusterarg' not in locals():
    clusters = c.listClusters({'zoneid': zoneid, 'listall': 'true'})
else:
    clusters = c.listClusters(
        {'zoneid': zoneid, 'name': clusterarg, 'listall': 'true'})

# die if there are no clusters found (unlikely)
if clusters is None:
    print "DEBUG: No clusters found in zone"
    exit(1)


# get a list of storage pools for each cluster
t_storagepool = PrettyTable(
    ["Cluster", "Storage Pool", "Number of detached disks", "Real Space used (GB)"])

for cluster in clusters:
    storagepools = []
    storagepools.append(c.getStoragePool(cluster.id))
    random_hypervisor = random.choice(c.getHostsFromCluster(cluster.id))

    # flatten storagepool list
    storagepools = [y for x in storagepools for y in x]

    # # if there are storage pools (should be)
    if len(storagepools) > 0:

        storagehelper = StorageHelper(debug=c.DEBUG)

        for storagepool in storagepools:
            used_space = 0

            # Get list of detached_disks from cloudstack for storagepool
            print "[INFO]: Retrieving list of detached disks for storage pool", storagepool.name
            detached_disks = c.getDetachedVolumes(storagepool.id)

            storagepool_devicepath = storagepool.ipaddress + \
                ":" + str(storagepool.path)

            # get filelist for storagepool via a 'random' hypervisor from
            # cluster
            primary_mountpoint = storagehelper.get_mountpoint(
                random_hypervisor.ipaddress, storagepool_devicepath)

            if primary_mountpoint is None:
                if c.DEBUG == 1:
                    print "[DEBUG]: no physical volume list retrieved for " + storagepool.name + " skipping"
                storagepool_filelist = None

            else:
                storagepool_filelist = storagehelper.list_files(
                    random_hypervisor.ipaddress, primary_mountpoint)

            t = PrettyTable(["Domain", "Account", "Name", "Cluster", "Storagepool", "Path",
                             "Allocated Size (GB)", "Real Size (GB)", "Disk Found"])

            for detached_disk in detached_disks:
                diskfound = ''

                disk_allocated_sizeGB = (
                    detached_disk.size / math.pow(1024, 3))

                if storagepool_filelist is None:
                    disk_real_sizeGB = 'n/a'
                    diskfound = '?'

                else:
                    disk_real_sizeGB = get_volume_filesize(
                        detached_disk.path, storagepool_filelist)

                    if disk_real_sizeGB is not None:
                        used_space += (disk_real_sizeGB / 1024)
                        disk_real_sizeGB = format(
                            (disk_real_sizeGB / 1024), '.2f')
                        diskfound = 'Y'

                    else:
                        disk_real_sizeGB = 0
                        diskfound = 'N'

                # add a row with disk details
                t.add_row([detached_disk.domain, detached_disk.account, detached_disk.name, cluster.name, storagepool.name, detached_disk.path,
                           disk_allocated_sizeGB, disk_real_sizeGB, diskfound])

            # Print disk table
            print t.get_string()
            t_storagepool.add_row(
                [cluster.name, storagepool.name, len(detached_disks), format(used_space, '.2f')])

print "Storagepool Totals"
print t_storagepool.get_string()
