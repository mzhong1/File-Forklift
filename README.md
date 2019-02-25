# Filesystem Forklift

Filesystem Migration Tool

-------------------------

Filesystem Forklift is an open source tool for migrating NFS and CIFS shares.  The goal is to quickly move large shares over the network through multiple Virtual Machines to a destination Gluster quickly and with little error.  Large shares these days may be considered impossible to move due to fact that it may take months to move the share.  Filesystem Forklift is intended to radically decrease the time needed to move the share, so that even seemingly impossibly large shares can be migrated to new clusters.

-------------------------

## To Start Using Filesystem Forklift

### Configuration:
1. Create your configuration file. The tool takes json config information.  The database_url, lifetime, and workgroup fields are optional.  Database_url will allow Filesystem Forklift to send log messages and updates to the specified Postgres database server. TimescaleDB is the preferred Postgres server type. Lifetime changes the timeout time of a node from the default of 5 seconds.  Workgroup is optional in that it is not needed for an NFS share, and can therefore be omitted.  
Fields for this file are:
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
    "debug_level": number from [0-10],
    "num_threads": number from [0-10],
    "workgroup": "WORKGROUP",
    "src_path": "/ starting directory of src share",
    "dest_path": "/ starting directory of destination share",
    "database_url": "postgresql://postgres:meow@127.0.0.1:8080
}

### Dependencies"
1. libnanomsg-dev
2. libsmbclient-dev
3. libsmbclient (smbclient, depends on your operating system)
4. flatbuffers?