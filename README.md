# A Python RAFT leader election implementation using sockets
This repository contains the code used for the second assignment of Distributed Data Processing Systems and cannot be used for any other purpose. RAFT is a consensus algorithm that elects a leader to enforce consensus. The specific part of leader election is implemented for the assignment. There are two main files, one can be used to run the election on a local system (which can be slow but good for testing purposes) and a file used for running the code on the DAS-5 system.

# How to run the code
## Local
Running the code on a local system is quite easy, the _local_raft.py_ file can be run as standalone. All settings should be changed in the file when you want to run for different numbers of nodes.

## DAS-5
Make sure that you can run the code on all nodes! After reservering your nodes, you should ssh into them on separate terminals and call the script as following
```console
foo@bar:~$ python das_5_raft.py "node111 node112 node113"
```
Where you change the nodes mentioned in the string with the nodes you reserved. Once all nodes are running the file, it will have opened all the receiving channels. Only after you run the file on all nodes can you continue (by pressing enter in each terminal) and open the sending channels. Once all nodes have established connection, leader election automatically begins.

# Dependencies
The local code requires a Python version of 3.7 or greater, whereas DAS-5 only runs on the Python 2.7.5 version installed on DAS-5. Both version require an installation of NumPy.
