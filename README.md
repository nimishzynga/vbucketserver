Vbucketserver
=============

#### How to build ?

To build a rpm of Vbucketserver, use:

    $ ./build-rpm.sh

### How to start Vbucketserver after RPM installation

Create server parameter config files as follows:

Example Config (Server settings): /etc/sysconfig/vbucketserver

For Active vbucketserver:
{
"State":"Active",
"ConfigMap" : {"cluster1": {"Capacity": 300, "SecondaryIps": [" --- "], "Servers": ["---"], "Replica":1 , "Vbuckets": 32, "Port": 11114}}
}

For Replica vbucketserver:
{
"State":"Replica",
"ActiveIp":"----:14000",
"ConfigMap" : {"cluster1": {"Capacity": 300, "SecondaryIps": [" --- "], "Servers": [" --- "], "Replica":1 , "Vbuckets": 32, "Port": 11114}}
}

ActiveIp is the ip of the machine, where Active vbuckerserver is running.
SecondarysIps and Servers are the array of the machine ips.If machine has only single ip, only put ips in Servers.


Server can be started using following command:

    # /etc/init.d/vbucketserver start

VBA should be started before starting the vbucketserver server on the machines, which are mentioned in vbucketconfig. 


