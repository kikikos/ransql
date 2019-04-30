import subprocess

p = subprocess.Popen("ps ax | grep FlinkAccumulator.jar | awk '{print $1}' ", stdout=subprocess.PIPE, shell=True)
 
## Talk with date command i.e. read data from stdout and stderr. Store this info in tuple ##
## Interact with process: Send data to stdin. Read data from stdout and stderr, until end-of-file is reached.  ##
## Wait for process to terminate. The optional input argument should be a string to be sent to the child process, ##
## or None, if no data should be sent to the child.
(output, err) = p.communicate()
 
## Wait for date to terminate. Get return returncode ##
p_status = p.wait()
print("out", output)
pids=[]

for pid in output.decode("utf-8").replace(" ","").split("\n"):
    if not pid == "":
        pids.append(pid)

for p in pids:
    print ("p : ", p)


print ("Command exit status/return code : ", p_status)