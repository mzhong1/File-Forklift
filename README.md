# Filesystem Forklift

Filesystem Migration Tool

-------------------------

Filesystem Forklift is an open source tool for migrating NFS and CIFS shares.  The goal is to quickly move large shares over the network through multiple Virtual Machines to a destination Gluster quickly and with little error.  Large shares these days may be considered impossible to move due to fact that it may take months to move the share.  Filesystem Forklift is intended to radically decrease the time needed to move the share, so that even seemingly impossibly large shares can be migrated to new clusters.

-------------------------

## To Start Using Filesystem Forklift

### Configuration:
1. Create your configuration file, forklift.json. The tool takes json config information.  The database_url, lifetime, and workgroup fields are optional.  Database_url will allow Filesystem Forklift to send log messages and updates to the specified Postgres database server. TimescaleDB is the preferred Postgres server type. Lifetime changes the timeout time of a node from the default of 5 seconds.  Workgroup is optional in that it is not needed for an NFS share, and can therefore be omitted.  
Fields for this file are:
```
{
    "nodes": [
        "yourip:port",
        "clusterip:port",
        ...
    ],
    "lifetime": some positive non-zero number,
    "src_server": "shareserver",
    "dest_server": "destinationserver",
    "src_share": "/src_share",
    "dest_share": "/destination_share",
    "system": "Nfs or Samba",
    "debug_level": "OFF, FATAL, ERROR, WARN, INFO, DEBUG, or ALL",
    "num_threads": number from [0-some reasonable number],
    "workgroup": "WORKGROUP",
    "src_path": "/ starting directory of src share",
    "dest_path": "/ starting directory of destination share",
    "database_url": "postgresql://postgres:meow@127.0.0.1:8080"
}
```
### Dependencies
1. libnanomsg-dev
2. libsmbclient-dev
3. libnfs-dev
4. nanomsg (libnanomsg.so.5), libnanomsg#

## Quick Start Guide
### NFS
1. Download and build any dependencies for the forklift (see above).  Nanomsg-1.1.4 can be found here:
- https://github.com/nanomsg/nanomsg
2. Configure your forklift.json file on every node in your cluster
Example:
```
{
    "nodes": [
        "127.0.0.1:8888",
        "clusterip:port",
        ...
    ],
    "src_server": "10.0.0.24",
    "dest_server": "192.88.88.88",
    "src_share": "/src_share",
    "dest_share": "/destination_share",
    "system": "Nfs",
    "debug_level": "OFF",
    "num_threads": 20,
    "src_path": "/",
    "dest_path": "/",
    "database_url": "postgresql://postgres:meow@127.0.0.1:8080"
}
```
Note: 
- src_path and dest_path should be "/" unless you are starting from a subdirectory in either of the shares. 
- leave workgroup out, as it is not needed for NFS
- lifetime can be adjusted, default is 5 seconds
- database_url is optional, only include it if you want to log data to a database, it will slow down the processes.
- if you are configuring a file for adding a node to a cluster, only include two socket addresses in the nodes section,the socket address of the node to be added, and the socket address of some node in the running cluster
3. Initialize the forklift.  On each node in your cluster, type ./filesystem_forklift -u "" -p "" (if you configured the forklift.json in /etc/forklift).  Otherwise, type ./filesystem_forklift -c path_to_directory_containing_config_file -u "" -p ""
Note: the reason why we leave the username and password flag as "" is because they are required, but are not actually used in the forklift since this for an NFS share and not a Samba share.  Since a Samba context is still initialized but unused, they are necessary, but non-"" entries may result in an error when Samba attempts to authenticate.  

