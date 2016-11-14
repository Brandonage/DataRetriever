import re


def build_dict_environment(doc):
    def parse_mb_exec(string):
        m = re.search('(\d+)([a-zA-Z]{1})',string)
        unit = m.group(2) ## Are they megabytes or gigabytes
        if (unit=='m'): # if they are megabytes just leave the number as it is
            number = int(m.group(1))
        if (unit=='g'): # if they are gigabytes multiply by 1024
            number = int(m.group(1))*1024
        return number
    d = {}
    array_spark = dict(doc.get('spark')) ## We take the Spark array and convert it into a dictionary to have better access
    d.update({'appId': doc.get('appId'),
              'spark.shuffle.spill.compress' : array_spark.get('spark.shuffle.spill.compress','true'),
              'spark.GC' : array_spark.get('spark.executor.extraJavaOptions','Default'),
              'spark.io.compression.codec' : array_spark.get('spark.io.compression.codec','lz4'),
              'spark.broadcast.compress': array_spark.get('spark.broadcast.compress','true'),
              'spark.memory.storageFraction' : array_spark.get('spark.memory.storageFraction','0.5'),
              'spark.shuffle.compress': array_spark.get('spark.shuffle.compress','true'),
              'spark.app.name' : array_spark.get('spark.app.name'),
              'spark.shuffle.file.buffer' : array_spark.get('spark.shuffle.file.buffer','32k'),
              'spark.reducer.maxSizeInFlight' : array_spark.get('spark.reducer.maxSizeInFlight','48m'),
              'spark.executor.cores' : array_spark.get('spark.executor.cores','1'),
              'spark.memory.fraction' : array_spark.get('spark.memory.fraction','0.75'),
              'spark.executor.memory' : array_spark.get('spark.executor.memory','1g'),
              'spark.locality.wait' : array_spark.get('spark.locality.wait','3s'),
              'spark.executor.instances' : array_spark.get('spark.executor.instances'),
              'spark.shuffle.io.preferDirectBufs': array_spark.get('spark.shuffle.io.preferDirectBufs','true'),
              #'spark.speculation': array_spark.get('spark.speculation','false'),
              #'spark.speculation.multiplier': array_spark.get('spark.speculation.multiplier','1.5'),
              #'spark.speculation.quantile': array_spark.get('spark.speculation.quantile','0.75'),
              'spark.shuffle.manager': array_spark.get('spark.shuffle.manager','sort'),
              'spark.task.cpus' : array_spark.get('spark.task.cpus','1'),
              'spark.serializer': array_spark.get('spark.serializer','org.apache.spark.serializer.JavaSerializer'),
              'spark.rdd.compress': array_spark.get('spark.rdd.compress','false')
              })
    d.update({'conf': d.get('spark.executor.cores') + '/' + d.get('spark.executor.memory')  + '/' +   d.get('spark.memory.fraction')  + '/' +  d.get('spark.reducer.maxSizeInFlight')})
    d.update({'parallelism': d.get('spark.executor.cores') + '/' + d.get('spark.executor.memory')})
    d.update({'spark.executor.bytes' : parse_mb_exec(d.get('spark.executor.memory'))})
    ## d.update({'memoryPerTask': d.get('spark.executor.bytes')}) maybe add it at a later stage
    return d


def build_dict_readings(doc): ## Creates a dictionary of GMONE reading values from a Mongo's JSON document
    d = {}
    d.update({'host':doc.get('host'),
                 'info':doc.get('info'),
                 'parameter':doc.get('parameter'),
                 'time':doc.get('time'),
                 'units':doc.get('units'),
                 'value':doc.get('value')
                })

    return d


def build_dict_task_attempts(doc):  ## Creates a dictionary of task attempts of Meteor from a Mongo's JSON document
    d = {}
    d.update({'appId':doc.get('appId'),
                 'stageId':doc.get('stageId'),
                 'stageAttemptId':doc.get('stageAttemptId'),
                 'duration':doc.get('duration'),
                 'endReason':doc.get('end',{}).get('Reason'),
                 'ClassName':doc.get('end',{}).get('ClassName'),
                 'Description':doc.get('end',{}).get('Description'), ## there is also an attribute called StackTrace
                 'id':doc.get('id'),
                 'index':doc.get('index'),
                 'locality':doc.get('locality'),
                 'start':doc.get('time',{}).get('start'),
                 'end':doc.get('time',{}).get('start') + doc.get('duration'), ## Fixed a bug: Not using end but start + duration. This values will always be in the mongodb
                 'hostname':doc.get('metrics',{}).get('HostName'),
                 'ExecutorDeserializeTime':doc.get('metrics',{}).get('ExecutorDeserializeTime'),
                 'ExecutorRunTime':doc.get('metrics',{}).get('ExecutorRunTime'),
                 'ResultSize':doc.get('metrics',{}).get('ResultSize'),
                 'SchedulerDelayTime':doc.get('metrics',{}).get('SchedulerDelayTime'),
                 'JVMGCTime':doc.get('metrics',{}).get('JVMGCTime'),
                 'MemoryBytesSpilled':doc.get('metrics',{}).get('MemoryBytesSpilled'),
                 'DiskBytesSpilled':doc.get('metrics',{}).get('DiskBytesSpilled'),
                 'ResultSerializationTime':doc.get('metrics',{}).get('ResultSerializationTime'),
                 'status':doc.get('status'),
                 'BytesReadDisk':doc.get('metrics',{}).get('InputMetrics',{}).get('BytesRead'),
                 'DataReadMethod':doc.get('metrics',{}).get('InputMetrics',{}).get('DataReadMethod'),
                 'BytesWrittenDisk':doc.get('metrics',{}).get('OutputMetrics',{}).get('BytesWritten'),
                 'ShuffleBytesWritten':doc.get('metrics',{}).get('ShuffleWriteMetrics',{}).get('ShuffleBytesWritten'),
                 'ShuffleWriteTime':doc.get('metrics',{}).get('ShuffleWriteMetrics',{}).get('ShuffleWriteTime'),
                 'ShuffleBytesRead':doc.get('metrics',{}).get('ShuffleReadMetrics',{}).get('TotalBytesRead'),
                 'ShuffleReadTime':doc.get('metrics',{}).get('ShuffleReadMetrics',{}).get('FetchWaitTime')
                 })
    return d

def build_dict_stage_attempts(doc):  ## Creates a dictionary from a document of stage attempts
    d = {}
    d.update({'appId':doc.get('appId'),
                 'id':doc.get('id'),
                 'stageId':doc.get('stageId'),
                 'jobId':doc.get('jobId'),
                 'start':doc.get('time',{}).get('start'),
                 'end':doc.get('time',{}).get('end'),
                 'name':doc.get('name'),
                 'taskCountsNum':doc.get('taskCounts',{}).get('num'),
                 'taskCountsRunning':doc.get('taskCounts',{}).get('running'),
                 'taskCountsSucceeded':doc.get('taskCounts',{}).get('succeeded'),
                 'taskCountsFailed':doc.get('taskCounts',{}).get('failed'),
                 'duration':doc.get('duration'),
                 'status':doc.get('status'),
                 'totalTaskDuration':doc.get('totalTaskDuration'),
                 'ExecutorDeserializeTime':doc.get('metrics',{}).get('ExecutorDeserializeTime'),
                 'ExecutorRunTime':doc.get('metrics',{}).get('ExecutorRunTime'),
                 'SchedulerDelayTime':doc.get('metrics',{}).get('SchedulerDelayTime'),#
                 'JVMGCTime':doc.get('metrics',{}).get('JVMGCTime'),#
                 'ResultSerializationTime':doc.get('metrics',{}).get('ResultSerializationTime'),#
                 'ResultSize':doc.get('metrics',{}).get('ResultSize'),#
                 'MemoryBytesSpilled':doc.get('metrics',{}).get('MemoryBytesSpilled'),
                 'DiskBytesSpilled':doc.get('metrics',{}).get('DiskBytesSpilled'),
                 'BytesReadDisk':doc.get('metrics',{}).get('InputMetrics',{}).get('BytesRead'),
                 'BytesWrittenDisk':doc.get('metrics',{}).get('OutputMetrics',{}).get('BytesWritten'),
                 'ShuffleBytesWritten':doc.get('metrics',{}).get('ShuffleWriteMetrics',{}).get('ShuffleBytesWritten'),
                 'ShuffleWriteTime':doc.get('metrics',{}).get('ShuffleWriteMetrics',{}).get('ShuffleWriteTime'),
                 'ShuffleBytesRead':doc.get('metrics',{}).get('ShuffleReadMetrics',{}).get('TotalBytesRead'),
                 'ShuffleReadTime':doc.get('metrics',{}).get('ShuffleReadMetrics',{}).get('FetchWaitTime')
                 })
    return d

def build_dict_jobs(doc):
    d = {}
    d.update({'appId':doc.get('appId'),
                 'id':doc.get('id'),
                 'start':doc.get('time',{}).get('start'),
                 'end':doc.get('time',{}).get('end'),
                 'stageIDs':doc.get('stageIDs'),
                 'stageNames':doc.get('stageNames'),
                 'status':doc.get('status'),
                 'name':doc.get('name'),
                 'taskCountsNum':doc.get('taskCounts',{}).get('num'),
                 'taskCountsRunning':doc.get('taskCounts',{}).get('running'),
                 'taskCountsSucceeded':doc.get('taskCounts',{}).get('succeeded'),
                 'stageCountsNum':doc.get('taskCounts',{}).get('num'),
                 'stageCountsRunning':doc.get('taskCounts',{}).get('running'),
                 'stageCountsSucceeded':doc.get('taskCounts',{}).get('succeeded'),
                 'duration':doc.get('duration'),
                 'ExecutorDeserializeTime':doc.get('metrics',{}).get('ExecutorDeserializeTime'),
                 'ExecutorRuntime':doc.get('metrics',{}).get('ExecutorRuntime'),
                 'SchedulerDelayTime':doc.get('metrics',{}).get('SchedulerDelayTime'),#
                 'JVMGCTime':doc.get('metrics',{}).get('JVMGCTime'),#
                 'ResultSerializationTime':doc.get('metrics',{}).get('ResultSerializationTime'),#
                 'ResultSize':doc.get('metrics',{}).get('ResultSize'),#
                 'BytesReadDisk':doc.get('metrics',{}).get('InputMetrics',{}).get('BytesRead'),
                 'BytesWrittenDisk':doc.get('metrics',{}).get('OutputMetrics',{}).get('BytesWritten'),
                 'ShuffleBytesWritten':doc.get('metrics',{}).get('ShuffleWriteMetrics',{}).get('ShuffleBytesWritten'),
                 'ShuffleWriteTime':doc.get('metrics',{}).get('ShuffleWriteMetrics',{}).get('ShuffleWriteTime'),
                 'ShuffleBytesRead':doc.get('metrics',{}).get('ShuffleReadMetrics',{}).get('TotalBytesRead'),
                 'ShuffleReadTime':doc.get('metrics',{}).get('ShuffleReadMetrics',{}).get('FetchWaitTime'),
                 'totalTaskDuration':doc.get('totalTaskDuration'),
                 'succeeded':doc.get('succeeded')
                 })
    return d

def build_dict_stage_executors(doc):
    d = {}
    d.update({'appId':doc.get('appId'),
                 'execId':doc.get('execId'),
                 'stageId':doc.get('stageId'),
                 'stageAttemptId':doc.get('stageAttemptId'),
                 'host':doc.get('host'),
                 'taskCountsnum':doc.get('taskCounts',{}).get('num'),
                 'taskCountssucceeded':doc.get('taskCounts',{}).get('succeeded'),
                 'totalTaskDuration':doc.get('totalTaskDuration'),
                 'ExecutorDeserializeTime':doc.get('metrics',{}).get('ExecutorDeserializeTime'),
                 'ExecutorRunTime':doc.get('metrics',{}).get('ExecutorRunTime'),
                 'ResultSize':doc.get('metrics',{}).get('ResultSize'),
                 'JVMGCTime':doc.get('metrics',{}).get('JVMGCTime'),
                 'ResultSerializationTime':doc.get('metrics',{}).get('ResultSerializationTime'),
                 'BytesReadDisk':doc.get('metrics',{}).get('InputMetrics',{}).get('BytesRead'),
                 'BytesWrittenDisk':doc.get('metrics',{}).get('OutputMetrics',{}).get('BytesWritten'),
                 'SchedulerDelayTime':doc.get('metrics',{}).get('SchedulerDelayTime')
                })
    return d


def build_dict_apps(doc):
    d = {}
    d.update({'appId':doc.get('id'),
                 'start':doc.get('time',{}).get('start'),
                 'end':doc.get('time',{}).get('end'),
                 'maxMem':doc.get('maxMem'),
                 'name':doc.get('name'),
                 'duration':doc.get('duration'),
                 'status':doc.get('status'),
                 'executorCounts':doc.get('executorCounts',{}).get('num'),
                 'totalJobDuration':doc.get('totalJobDuration'),
                 'totalTaskDuration' : doc.get('totalTaskDuration'),
                 'JVMGCTime':doc.get('metrics',{}).get('JVMGCTime'),
                 'ExecutorDeserializeTime':doc.get('metrics',{}).get('ExecutorDeserializeTime'),
                 'ExecutorRunTime':doc.get('metrics',{}).get('ExecutorRunTime'),
                 'ResultSerializationTime':doc.get('metrics',{}).get('ResultSerializationTime'),
                 'MemoryBytesSpilled':doc.get('metrics',{}).get('MemoryBytesSpilled'),
                 'DiskBytesSpilled':doc.get('metrics',{}).get('DiskBytesSpilled'),
                 'SchedulerDelayTime':doc.get('metrics',{}).get('SchedulerDelayTime'),
                 'ShuffleWriteTime':doc.get('metrics',{}).get('ShuffleWriteMetrics',{}).get('ShuffleWriteTime'),
                 'ShuffleBytesWritten':doc.get('metrics',{}).get('ShuffleWriteMetrics',{}).get('ShuffleBytesWritten'),
                 'ShuffleBytesRead':doc.get('metrics',{}).get('ShuffleReadMetrics',{}).get('TotalBytesRead'),
                 'ShuffleReadTime':doc.get('metrics',{}).get('ShuffleReadMetrics',{}).get('FetchWaitTime'),
                 'BytesReadDisk':doc.get('metrics',{}).get('InputMetrics',{}).get('BytesRead'),
                 'BytesWrittenDisk':doc.get('metrics',{}).get('OutputMetrics',{}).get('BytesWritten')
              })
    return d
