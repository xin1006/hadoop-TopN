Hadoop map reduce example - Top N active users
------------------------------------

Calculate top N the most active users.

Access log format:

    [13/Jul/2015:07:54:18 +0200] GET /nmo/ 10.187.98.36 10.156.15.25 bsentnohead=0 qry=?frameLayout=false stat=302 sess=BCE7E9AE09201750653E802A4001090A thr=http-bio-9443-exec-4 usr=- scenId=- time=1

Example file may be found `src/test/resources/accesslog_input`

This project assumes that `hadoop` is already installed and accessible as 

    $ hadoop ...
    
command line.

## How to run it

### In hadoop's signle mode

    $ ./gradlew runLocally
    
### In hadoop's pseudo distributed mode

Hadoop data nodes, name nodes have to be started.
Hadoop configuration should point to mapreduce (not YARN, but YARN is easy to configure).

    $ ./gradlew run