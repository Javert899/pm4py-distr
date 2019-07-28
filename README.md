# pm4py-distr
Support for distributed logs and computations in PM4Py.

See the currently supported API in api.txt

To execute locally an example (one master running on 5001 and two slaves running on 5002 and 5003)
just run "python main.py"

The example contains the partitioned version of the logs "receipt" and "roadtraffic".

## Example of usage of the distributed services (default keyphrase is "hello"):

1) ALLOCATION OF THE PARTITIONS BETWEEN THE SLAVES

http://localhost:5001/doLogAssignment?keyphrase=hello

2) CALCULATION OF THE DFG

http://localhost:5001/calculateDfg?keyphrase=hello&process=receipt

## Configuration of the keyphrase

The keyphrase that is shared between the slaves and the master is contained in the pm4pydistr.configuration file.

## Custom executions of master/slave

If instead of running the default example, you want to execute the master/slaves in custom configuration,
follow the process:

1) REMOVE THE EXAMPLE FILES

Remove the "master.db" database and the "master" folder

2) CREATE A "master" FOLDER CONTAINING THE PARTITIONED DATASETS THAT YOU WANT TO USE

See the PARTITIONING.txt file for instructions on how to partition a log file into a partitioned dataset!

3) LAUNCH THE MASTER

Launch the master with the command: python launch.py --type master --conf master --port 5001
(replace possibly the port, it is by default listening on 0.0.0.0)

4) LAUNCH THE SLAVES

Launch a slave (that are master-aware) with the command:

python launch.py --type slave --conf slave1 --port 5002 --master-host 127.0.0.1 --master-port 5001
(replace possibly the port used by the slave, and the host/port that points to the master).

## Demo on a custom server

It may work, or it may not work :)

Distributed DFG calculation:
 
http://212.237.8.106:5001/calculateDfg?keyphrase=hello&process=roadtraffic
 
http://212.237.8.106:5001/calculateDfg?keyphrase=hello&process=bpic2018
 
http://212.237.8.106:5001/calculateDfg?keyphrase=hello&process=bpic2019
 
Retrieval of the start activities of the log:
 
http://212.237.8.106:5001/getStartActivities?keyphrase=hello&process=roadtraffic
 
http://212.237.8.106:5001/getStartActivities?keyphrase=hello&process=bpic2018
 
http://212.237.8.106:5001/getStartActivities?keyphrase=hello&process=bpic2019
 
Retrieval of the end activities of the log:
 
http://212.237.8.106:5001/getEndActivities?keyphrase=hello&process=roadtraffic
 
http://212.237.8.106:5001/getEndActivities?keyphrase=hello&process=bpic2018
 
http://212.237.8.106:5001/getEndActivities?keyphrase=hello&process=bpic2019
