import time
import docker
import re

import datetime
import concurrent.futures
client = docker.from_env()
containerid = ""
for container in client.containers.list():
    containerid = container.id
horizon = client.containers.get(containerid)
fulltime1 = time.perf_counter()
totalthreads = 30 #total threads
rangesetstart = 6100000 #change to start ledger #
numledgers = 32491392 #total ledgers
rangesetend = rangesetstart + numledgers
ledgersperthread = 25000 #number of ledgers to process per thread
jobranges = []
#current ledger = 38,491,391
print(f'processing ledgers {rangesetstart} through {rangesetend}')
def getrangeset(start):
    for this in range(start,(rangesetend + 1),ledgersperthread):
        if this==rangesetstart:
            continue
        rangestring = (str(start) + " " + str(this))
        #print(rangestring)
        jobranges.append(rangestring)
        #print(jobranges)
        start = this+1

getrangeset(rangesetstart)

def startjob(range):
    t1 = time.perf_counter()
    cmd="/opt/stellar/horizon/bin/horizon --captive-core-storage-path=/opt/stellar/temp/" + str(range.split(" ")[0]) +  " db reingest range " + range
    print(cmd)
    #def injestrange(cmd, start,end):
    #res, stream = horizon.exec_run(cmd, stream=True, demux=False)
    #for data in stream:
        #tempdata = data
    process = horizon.exec_run(cmd)
    
    t2 = time.perf_counter()
    t3 = (t2 - t1) / 25
    print(f'Range {range} Took:{t2 - t1} seconds, avg per 100 ledgers: {t3}')
    return True

with concurrent.futures.ThreadPoolExecutor(totalthreads) as executor:
    executor.map(startjob, jobranges)

fulltime2 = time.perf_counter()
ledgerspersec =  (rangesetend - rangesetstart) / (fulltime2 - fulltime1)
print(f'total ledgers processed: {numledgers} with {ledgersperthread} per thread')
print(f'total threads: {totalthreads}')
print(f'total time {fulltime2 - fulltime1}')
print(f'ledgers per sec: {ledgerspersec}')