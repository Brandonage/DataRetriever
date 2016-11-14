
## We are aiming to construct a dataframe with all the information that will allow us to do machine learning. The dataset shouldn't be too big since it will consist of mainly tasks and its execution
## Maybe stages as well.



import pandas as pd
import matplotlib.colors as colors
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import matplotlib as mpl
import seaborn as sns
from sklearn.cross_validation import train_test_split
from sklearn import svm, linear_model
from sklearn import preprocessing
from sklearn.preprocessing import OneHotEncoder
import numpy as np
import colorsys
import statsmodels.formula.api as sm

client = MongoClient()
dbm = client.meteor
dbg = client.gmone


### parameters banned to be included in the dataframe for machine learning
ban_parameters = ["cpu_siq","cpu_hiq","mem_buffer","proc","procs_new","procs_blk","sysv_ipc_sem","sysv_ipc_shm","sysv_ipc_shm","sockets_tcp","sockets_udp","sockets_frq","sockets_raw",
                      "disk_percentage","rpc_client_retr","rpc_server_call_erc1","rpc_server_call_xdrc","rpc_server_erau","virtual_mem_majpf","virtual_mem_minpf","virtual_mem_alloc","sysv_ipc_msg","virtual_mem_free",
                  "rpc_server_call" ,"rpc_server_erca"]

#### SEVERAL FUNCTIONS TO CREATE DICTIONARIES OUT OF Mongo Documents. This dictionaries are flattened
#### to be inserted into a Pandas Data Frame


def build_dict_environment(doc):
    d = {}
    array_spark = dict(doc.get('spark')) ## We take the Spark array and convert it into a dictionary to have better access
    d.update({'appId': doc.get('appId'),
              'spark.shuffle.spill.compress' : array_spark.get('spark.shuffle.spill.compress','true'),
              'spark.GC' : array_spark.get('spark.executor.extraJavaOptions','Default'),
              'spark.io.compression.codec' : array_spark.get('spark.io.compression.codec','snappy'),
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
              'spark.speculation': array_spark.get('spark.speculation','false'),
              'spark.speculation.multiplier': array_spark.get('spark.speculation.multiplier','1.5'),
              'spark.speculation.quantile': array_spark.get('spark.speculation.quantile','0.75'),
              'spark.shuffle.manager': array_spark.get('spark.shuffle.manager','sort'),
              'spark.task.cpus' : array_spark.get('spark.task.cpus','1'),
              })
    d.update({'conf': d.get('spark.executor.cores') + '/' + d.get('spark.executor.memory')  + '/' +   d.get('spark.memory.fraction')  + '/' +  d.get('spark.reducer.maxSizeInFlight')})
    d.update({'parallelism': d.get('spark.executor.cores') + '/' + d.get('spark.executor.memory')  + '/' +   d.get('spark.task.cpus')})
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
                 'duration':doc.get('duration'),
                 'status':doc.get('status'),
                 'totalTaskDuration':doc.get('totalTaskDuration'),
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


### END OF BUILD DICT FUNCTIONS



#### BEGIN THE BUILD DATAFRAME GROUP OF FUNCTIONS



def mongo_stats_avg_host(dbg,start,end,hostname): ## calculate the mean of the different metrics inside the gmone database for a given HOST, START AND END
    interval=510 ## we test this interval so we don't miss tasks that last less than 1 second ( the monitor period we set in GMone )
    cursor = dbg.readings.aggregate([{"$match":{"time":{ "$gt": long(start-interval), "$lt": long(end+interval)},"host":hostname}},
                       {"$group":{"_id":"$parameter","average":{"$avg":"$value"}}}])
    return cursor

def build_task_attempts_dataframe(db,dbg):  ## Builds the task attempts dataframe out of the meteor database and GMone DATAFRAME as a parameter
    cursor = db.task_attempts.find()
    rows_list = []
    for doc in cursor:
        d = {}
        try:
            d = build_dict_task_attempts(doc)
            ## now we extend the dictionary with the summary statistics of the GMone dataframe for that tasks's [start,end,host]
            stats_cursor = mongo_stats_avg_host(dbg,start=d.get('start'),end=d.get('end'),hostname=d.get('hostname'))
            for stats_doc in stats_cursor:
                col_name=stats_doc.get('_id')
                if col_name not in ban_parameters:
                    value=stats_doc.get('average')
                    d.update({col_name:value})
            ## And also for the entry point in time
            stats_cursor = mongo_stats_avg_host(dbg,start=d.get('start')-500,end=d.get('start')+500,hostname=d.get('hostname'))
            for stats_doc in stats_cursor:
                col_name=stats_doc.get('_id')
                if col_name not in ban_parameters:
                    value=stats_doc.get('average')
                    d.update({col_name + "_entry_point":value})
            stage_cursor = db.stages.find({'appId':d.get('appId'),'id':d.get('stageId')},{'name':1})
            for stages in stage_cursor:
                sta = stages.get('name')
                d.update({"stageName":sta})
        except:
            d.update({'appId':doc.get('_id')}) ## we just insert an _id in case it fails to be able to debug
        rows_list.append(d)
    df = pd.DataFrame(rows_list)
    return df


def build_readings_dataframe(db):
    cursor = db.readings.find({"parameter": { '$nin' : ban_parameters  }  })
    rows_list = []
    for doc in cursor:
        d = {}
        try:
            d = build_dict_readings(doc)
        except:
            d.update({'host':doc.get('_id')}) ## we just insert an _id in case it fails
        rows_list.append(d)
    df = pd.DataFrame(rows_list)
    #df.set_index('time',drop=True,inplace=True)
    return df

def build_stage_attempts_dataframe(db):
    cursor = db.stage_attempts.find()
    rows_list = []
    for doc in cursor:
        d = {}
        try:
            d = build_dict_stage_attempts(doc)
        except:
            d.update({'appId':doc.get('_id')}) ## we just insert an _id in case it fails
        rows_list.append(d)
    df = pd.DataFrame(rows_list)
    return df


def build_apps_dataframe(db):
    cursor = db.apps.find()
    rows_list = []
    for doc in cursor:
        d = {}
        try:
            d = build_dict_apps(doc)
        except:
            d.update({'appId':doc.get('_id')}) ## we just insert an _id in case it fails
        rows_list.append(d)
    df = pd.DataFrame(rows_list)
    return df


def build_environment_dataframe(db):
    cursor = db.environment.find({},{'spark':1,'appId':1})
    rows_list = []
    for doc in cursor:
        d = {}
        try:
            d = build_dict_environment(doc)
        except:
            d.update({'appId':doc.get('_id')})
        rows_list.append(d)
    df = pd.DataFrame(rows_list)
    return df

def build_jobs_dataframe(db):
    cursor = db.jobs.find()
    rows_list = []
    for doc in cursor:
        d = {}
        try:
            d = build_dict_jobs(doc)
        except:
            d.update({'appId':doc.get('_id')}) ## we just insert an _id in case it fails
        rows_list.append(d)
    df = pd.DataFrame(rows_list)
    return df

def build_stage_executors_dataframe(db):
    cursor = db.stage_executors.find()
    rows_list = []
    for doc in cursor:
        d = {}
        try:
            d = build_dict_stage_executors(doc)
        except:
            d.update({'appId':doc.get('_id')}) ## we just insert an _id in case it fails
        rows_list.append(d)
    df = pd.DataFrame(rows_list)
    return df


###### END THE BUILD DATAFRAME GROUP OF FUNCTIONS


######## AUXILIARY FUNCTIONS ##########




def entry_point_task_load(df,tuple,parameter): ## return values from gmone monitoring for a given task
    ## You can feed it the previous tuple and a paramater from the readings collection and  it will return the values
    return df.loc[(df["host"]==tuple[2]) & (df["time"]>=long(tuple[0]-5000)) & (df["time"]<long(tuple[1]+5000)) & (df["parameter"]==parameter)]






def time_interval_for_app(dfm, application,stageId, hostname):
    d = dfm.loc[(dfm['stageId']==stageId) & (dfm['appId']==application) & (dfm['hostname']==hostname),['start','end']]
    return[d.start.min()-1000,d.end.max()+1000]


def create_time_series_host(df,host,parameters,timestart, timeend):  ## Create a timeseries dataframe with [time,value] if a given parameter
    return df.loc[(df["host"]==host) & (df["parameter"].isin(parameters) ) & (df["time"]<timeend) & (df["time"]>timestart),["time","value","parameter"]]

def plot_stage_in_host(dfm,dfg,appId,stageId,parameters,features,host,normalise):
    def plotCollection(subplot,xs,ys, *args, **kwargs):
        subplot.plot(xs,ys, *args, **kwargs)
        if "label" in kwargs.keys():
            handles,labels = plt.gca().get_legend_handles_labels()
            newLabels, newHandles = [], []
            for handle, label in zip(handles,labels):
                if label not in newLabels:
                    newLabels.append(label)
                    newHandles.append(handle)
            plt.legend(newHandles, newLabels,loc='lower right')

    [s,e] = time_interval_for_app(dfm, appId,stageId, host)
    time_series = create_time_series_host(dfg, host, parameters, s,e)
    fig,p1 = plt.subplots()
    p2 = p1.twinx()
    for para in parameters:
        p1.plot(time_series.loc[time_series['parameter']==para].time,time_series.loc[time_series['parameter']==para].value,'-o',label=para)
    p1.legend()
    p1.set_xlabel("Time")
    p1.set_ylabel(ylabel='%')
    mpl.rcParams['axes.formatter.useoffset'] = False
    if (normalise == True):
        p1.set(ylim=(-1,1))
    p2.set_ylabel("TASK INDEX")
    tasks = dfm.loc[(dfm["hostname"]==host) & (dfm["start"]>s) & (dfm["end"]<e) & (dfm["end"]!=0)].groupby(['appId']) #& (dfm["appId"]==appId) & (dfm["stageId"]==stageId)]
    apps = tasks.grouper.levels[0]
    norm = colors.Normalize(0,len(apps))
    scalar_map = cm.ScalarMappable(norm=norm, cmap='hsv')
    for name, group in tasks:
        color = scalar_map.to_rgba(apps.get_loc(name))
        plotCollection(p2,[group.start,group.end],[group.index,group.index],c=color,lw=4,label=name)
    #
    # apps = tasks.appId.unique()
    # norm = colors.Normalize(0,len(apps))
    # scalar_map = cm.ScalarMappable(norm=norm, cmap='hsv')
    # for _,row in tasks.iterrows():
    #     color = scalar_map.to_rgba(np.where(apps == row['appId'])[0][0])
    #     p2.plot([row['start'],row['end']],[row['index'],row['index']],lw=4 ,c=color)
    ## Now we print information about the tasks based on the features that we pass. This will help us with the data analysis part. Want to find out
    ## What is going on
    for name, group in tasks:
        print name
        print group[features]



def train_SVM(dfm):
    train, test = train_test_split(dfm, test_size = 0.2)
    collist = train.columns.drop('duration')
    X = train[collist]
    Y = train['duration']
    clf = svm.SVR()
    clf.fit(X, Y)
    Xtest = test[collist]
    Ytest = test['duration']
    Result = clf.predict(Xtest)


def preprocess_tasksdf(dfm):
    dfm.fillna(value=0, inplace = True)
    labEncod = preprocessing.OneHotEncoder()
    one_hot = pd.get_dummies(dfm['locality'])
    dfm = dfm.join(one_hot)
    one_hot = pd.get_dummies(dfm['stageName'])
    dfm = dfm.join(one_hot)
    # one_hot = pd.get_dummies(dfm['hostname'])
    # dfm = dfm.join(one_hot)
    return dfm

def normalise_task_df(dfm):
    cols_to_norm = dfm.columns.drop(['index','status','locality','nlocality','appId','hostname','stageId','start','end','duration',])
    dfm[cols_to_norm] = dfm[cols_to_norm].apply(lambda x: (x - x.mean()) / (x.max() - x.min()))

def normalise_gmone_df(dfg):
    dfg['value'] = dfg.groupby('parameter')['value'].transform(lambda x:  (x - x.mean()) / (x.max() - x.min()))

def plot_correlations(df):
    ## print a function for regression for the data d and attributes x and y
    def correlation_regression(d,x,y): ## wrapper to easily call sns.pairplot with regression
        sns.pairplot(data=d, x_vars=x,y_vars=y,kind='reg')
    ## print a function for the data d and categories
    def correlation_categories(d,x,y): ## wrapper to easily call sns.stripplot with categories
        sns.stripplot(data=d,x=x,y=y)
    ## the attributes we will use for the different functions
    attributes = [(['cpu_wait_entry_point','cpu_usr_entry_point','cpu_sys_entry_point'],['duration']),
                  (['paging_in_entry_point','procs_run_entry_point'],['duration']),
                  (['disk_read_entry_point','disk_write_entry_point'],['duration']),
                  (['mem_used_entry_point','mem_free_entry_point'],['duration']),
                  (['sys_contswitch_entry_point','sys_interrupts_entry_point'],['duration']),
                  (['net_recv_entry_point','sockets_tot_entry_point'],['duration'])
                  ]
    for att in attributes:
        g = sns.FacetGrid(df,col='stageName',col_wrap=4,size=5,sharey=False)
        g.map(sns.regplot,att,"duration")
        ## we print the correlation between attributes defined
        try:
            input("Press enter to continue")
        except SyntaxError:
            pass
        sns.stripplot(data=df.loc[dfm['stageName']==stage],x='locality',y='duration') ## we also print data locality related attributes




if __name__ == '__main__':


    dfg = build_readings_dataframe(dbg)
    dfm = build_task_attempts_dataframe(dbm,dbg)
    dfj = build_jobs_dataframe(dbm)
    dfs = build_stage_attempts_dataframe(dbm)
    dfe = build_stage_executors_dataframe(dbm)
    dfenv = build_environment_dataframe(dbm)
    dfapps = build_apps_dataframe(dbm)
    dfapps = dfapps.merge(dfenv,on='appId').sort('start')
    dfs = dfs.merge(dfenv,on='appId').sort('start')
    dfg_norm = dfg
    normalise_gmone_df(dfg_norm)

    application_1467023706568_0051 ## TAKE OUT THESE APPS ( THey were killed )
    application_1467023706568_0050
    application_1467023706568_0049
    dfapps = dfapps[dfapps['appId'].isin(['application_1467023706568_0051','application_1467023706568_0050','application_1467023706568_0049','application_1467023706568_0051','application_1467970325842_0002'])]
    dfm = dfm[~dfm['appId'].isin(['application_1467023706568_0051','application_1467023706568_0050','application_1467023706568_0049','application_1467023706568_0051'])]
    dfapps = dfapps[dfapps['name']!='BigDataBench PageRank'] ## Take out PageRank because it doesn't fit



    ## The features of the plot_stage function
    f = ['start','end','stageName','duration','status','JVMGCTime','SchedulerDelayTime','ExecutorDeserializeTime','endReason','ClassName','Description','ExecutorRunTime','MemoryBytesSpilled','DiskBytesSpilled']
    ## parameters are (dfm,dfg,appId,stageId,parameters,features,host,normalise)
    plot_stage_in_host(dfm,dfg_norm,'application_1465290821806_0097',2,['sys_contswitch','cpu_usr','cpu_wait','paging_in','disk_write'],features=f,host='granduc-18.luxembourg.grid5000.fr',normalise=True)

    ## train an SVM

    dfm_ml = preprocess_tasksdf(dfm)
    train_col = ['duration','cpu_idl_entry_point','cpu_sys_entry_point','cpu_usr_entry_point','cpu_wait_entry_point','disk_read_entry_point','disk_write_entry_point','io_total_read_entry_point','io_total_write_entry_point',
                 'mem_cached_entry_point','mem_free_entry_point','mem_used_entry_point','net_recv_entry_point','net_send_entry_point','paging_in_entry_point','paging_out_entry_point','pkt_total_recv_entry_point',
                 'pkt_total_send_entry_point','sockets_tot_entry_point','sys_contswitch_entry_point','sys_interrupts_entry_point','nlocality','nstagename','nhostname']

    train_drop = ['BytesReadDisk','BytesWrittenDisk','ClassName','Description','DiskBytesSpilled','ExecutorDeserializeTime',
                 'ExecutorRunTime','JVMGCTime','MemoryBytesSpilled','ResultSerializationTime','ResultSize','SchedulerDelayTime','ShuffleBytesRead','ShuffleBytesWritten','ShuffleReadTime',
                  'ShuffleWriteTime','appId','cpu_idl','cpu_sys','cpu_usr','cpu_wait','disk_read','disk_write','end','endReason','hostname','id','index','io_total_read','io_total_write','locality',
                  'mem_cached','mem_free','mem_used','net_recv','net_send','paging_in','paging_out','pkt_total_recv','pkt_total_send','procs_run','rpc_client_call','rpc_client_refr','sockets_tot',
                  'stageAttemptId','stageId','stageName','start','status','sys_contswitch','sys_interrupts']




    train, test = train_test_split(dfm_ml1.loc[dfm_ml1['nstagename']==9], test_size = 0.2)







def plot_bars_duration(x,y,e):
    fig = plt.figure()
    ax = plt.subplot(111)
#    y = [data.ExecutorRunTime.values[0],data.ExecutorDeserializeTime.values[0],data.JVMGCTime.values[0],data.SchedulerDelayTime.values[0],data.duration.values[0],data.totalTaskDuration.values[0]]
#    x = ['ExecutorRunTime','ExecutorDeserializeTime','JVMGCTime','SchedulerDelayTime','duration','totalTaskDuration']
    ind = np.arange(len(x))
    width = 1
    HSV_tuples = [(c*1.0/len(x), 0.5, 0.5) for c in range(len(x))]
    RGB_tuples = map(lambda x: colorsys.hsv_to_rgb(*x), HSV_tuples)
    ax.bar(ind,y/1000,color=RGB_tuples)
    ax.set_ylabel('duration')
    ax.set_xlabel('configuration')
    ax.set_title(e)
    ax.set_xticks(ind + (width-0.6))
    ax.set_xticklabels(tuple(x))

def plot_duration_of_apps(df):
    elements = ['duration']#,'totalTaskDuration','ExecutorRunTime','JVMGCTime']
    for e in elements:
        plot_bars_duration(df['conf'],df[e],e)



#### GRAFICAS DE CONFIGURACION Y DURACION

g = sns.FacetGrid(dfs_kmeans,col='name',col_wrap=4,size=5)
g.map(sns.barplot,"conf","duration")


#### PLOTS EN LA MISMA VENTANA DIFERENTES SUBPLOTS

f, (ax1, ax2) = plt.subplots(2)
sns.kdeplot(dfm['cpu_usr'].loc[dfm["appId"]=="application_1468937997200_0001"],ax=ax1)
sns.kdeplot(dfm['cpu_wait'].loc[dfm["appId"]=="application_1468937997200_0001"],ax=ax2)
sns.kdeplot(dfm['mem_used'].query("appId==application_1468923739444_0002"),ax=ax2)
sns.kdeplot(dfm['disk_write'].query("appId==application_1468923739444_0002"),ax=ax4)

g = sns.FacetGrid(dfm['cpu_idl_entry_point'].query("appId==application_1468923739444_0002"))
g.map_diag(sns.kdeplot)
g.map_offdiag(plt.scatter)

#### FOR GMONE
f, (ax1, ax2, ax3) = plt.subplots(3)
sns.kdeplot(dfg.query("parameter=='cpu_usr'").value,ax=ax1,legend=False).set_title('cpu_usr')
sns.kdeplot(dfg.query("parameter=='mem_used'").value,ax=ax3,legend=False).set_title('mem_used')
sns.kdeplot(dfg.query("parameter=='disk_write'").value,ax=ax2,legend=False).set_title('disk_write')



### ENTRENAR MODELO LINEAR PARA PREDECIR DURACION DE APP

dfapps['spark.executor.memory'] = dfapps['spark.executor.memory'].replace(to_replace=['5g','2g','3g','512m','4g','1g'],value=[5,2,3,0.512,4,1])
dfapps['spark.executor.cores'] = dfapps['spark.executor.cores'].astype(float)
dfapps['spark.executor.instances'] = dfapps['spark.executor.instances'].astype(float)
dfapps['spark.memory.fraction'] = dfapps['spark.memory.fraction'].astype(float)
dfapps['spark.locality.wait'] = dfapps['spark.locality.wait'].astype(float)
dfapps['spark.memory.storageFraction'] = dfapps['spark.memory.storageFraction'].astype(float)
dfapps['spark.reducer.maxSizeInFlight'] = dfapps['spark.reducer.maxSizeInFlight'].replace(to_replace=['24m','48m','128m','256m'],value=[24,48,128,256])
dfapps['spark.shuffle.file.buffer'] = dfapps['spark.shuffle.file.buffer'].replace(to_replace=['32k','128k','512k'],value=[32,128,512])

dumm_cols = ['spark.broadcast.compress', 'spark.io.compression.codec' , 'spark.GC' , 'spark.shuffle.compress' , 'spark.shuffle.spill.compress',
             'spark.speculation','spark.shuffle.io.preferDirectBufs']

for col in dumm_cols:
    one_hot = pd.get_dummies(dfapps[col],prefix=col)
    dfapps = dfapps.join(one_hot)


col_for_train = [
       'spark.executor.cores',
       'spark.executor.memory',
       'spark.locality.wait', 'spark.memory.fraction',
       'spark.memory.storageFraction', 'spark.reducer.maxSizeInFlight',
       'spark.shuffle.file.buffer',
       'executorCounts','duration',
       'spark.broadcast.compress_false','spark.broadcast.compress_true','spark.io.compression.codec_lz4',
       'spark.io.compression.codec_lzf',  'spark.io.compression.codec_snappy',  'spark.GC_-XX:+UseConcMarkSweepGC',
       'spark.GC_-XX:+UseG1GC',  'spark.GC_-XX:+UseParallelGC',  'spark.GC_-XX:+UseSerialGC',  'spark.shuffle.compress_false',
       'spark.shuffle.compress_true',  'spark.shuffle.spill.compress_false',  'spark.shuffle.spill.compress_true',
       'spark.speculation_true','spark.speculation_false']

def train_linear_model_app(dfapps):
    train, test = train_test_split(dfapps, test_size = 0.2)
    collist = train.columns.drop('duration')
    X = train[collist]
    Y = train['duration']
    regr = linear_model.LinearRegression()
    regr.fit(X, Y)
    Xtest = test[collist]
    Ytest = test['duration']
    Xtest.join(Ytest)
    Result = regr.predict(Xtest)



#### Analyse the configuration of the different applications
application_1467023706568_0051 ## TAKE OUT THESE APPS ( THey were killed )
application_1467023706568_0050
application_1467023706568_0049
dfapps = dfapps[dfapps['appId']!='application_1467023706568_0051']

dfapps = dfapps[dfapps['name']!='BigDataBench PageRank']


dfapps.groupby("name").apply(lambda x: x[x['duration']==x.duration.min()])
dfmins = dfapps.groupby("name").apply(lambda x: x[x['duration']==x.duration.min()])
dfmaxs= dfapps.groupby("name").apply(lambda x: x[x['duration']==x.duration.max()])
dfdefaults = dfapps.loc[(dfapps['spark.executor.cores']=='1') & (dfapps['spark.executor.memory']=='1g') & (dfapps['spark.memory.fraction']=='0.75')]


dftotal = dfmins.append(dfmaxs)
dftotal = dftotal.append(dfdefaults)

sns.set_context("talk")

g = sns.FacetGrid(dftotal,col='name',col_wrap=3,sharey=False,sharex=False,palette="GnBu_d")
g = g.map(sns.barplot,"conf","duration",color="#338844")

#### We want to compare task completion time with the total duration of the apps

df_min_task = dfapps.groupby("name").apply(lambda x: x[x['totalTaskDuration']==x.totalTaskDuration.min()])

dftotal = df_min_task.append(dfmins)

dffinal = pd.melt(dftotal[['totalTaskDuration','duration','conf','name']],value_vars=['totalTaskDuration','duration'],id_vars=['conf','name'])



g = sns.FacetGrid(dffinal,col='name',col_wrap=3,sharey=False,sharex=False,legend_out=True)
g = g.map_dataframe(sns.barplot,x="conf",y="stand_value",hue="variable")


g = sns.barplot(data=dffinal.query('name=="Grep"'),y="value",x="conf",hue="variable")





### Trade off


#### FOR GMONE



g = sns.barplot(data=dffinal.query('name=="KMeans"'),y="value",x="conf",hue="variable").set_title("KMeans")



#### Correlation between stages and metrics


g = sns.FacetGrid(data=dfm,sharey=False,col='stageName',col_wrap=3)
g.map(sns.regplot,x='cpu_wait',y='duration')





### ENTRENAR MODELO LINEAR PARA DETECTAR PARAMETROS MAS IMPORTANTES DENTRO DE LA APLICACION

dfapps = dfapps.query("status==2 and appId!='application_1467970325842_0001'")

#dfapps['spark.executor.memory'] = dfapps['spark.executor.memory'].replace(to_replace=['5g','2g','3g','512m','4g','1g'],value=[5,2,3,0.512,4,1])
#dfapps['spark.executor.cores'] = dfapps['spark.executor.cores'].astype(float)
#dfapps['spark.executor.instances'] = dfapps['spark.executor.instances'].astype(float)
dfapps['spark.memory.fraction'] = dfapps['spark.memory.fraction'].astype(float)
dfapps['spark.locality.wait'] = dfapps['spark.locality.wait'].astype(float)
dfapps['spark.memory.storageFraction'] = dfapps['spark.memory.storageFraction'].astype(float)
dfapps['spark.reducer.maxSizeInFlight'] = dfapps['spark.reducer.maxSizeInFlight'].replace(to_replace=['24m','48m','128m','256m'],value=[24,48,128,256])
dfapps['spark.shuffle.file.buffer'] = dfapps['spark.shuffle.file.buffer'].replace(to_replace=['32k','128k','512k'],value=[32,128,512])
dfapps['spark.speculation.multiplier'] = dfapps['spark.speculation.multiplier'].astype(float)
dfapps['spark.speculation.quantile'] = dfapps['spark.speculation.quantile'].astype(float)
dfapps['spark.locality.wait'] = dfapps['spark.locality.wait'].astype(float)

dumm_cols = ['spark.broadcast.compress', 'spark.io.compression.codec' , 'spark.GC' , 'spark.shuffle.compress' , 'spark.shuffle.spill.compress','spark.shuffle.manager','spark.speculation']

for col in dumm_cols:
    one_hot = pd.get_dummies(dfapps[col],prefix=col)
    dfapps = dfapps.join(one_hot)


col_for_train = [
       #'spark.executor.cores',
       #'spark.executor.memory',
       'spark.locality.wait', 'spark.memory.fraction',
       'spark.memory.storageFraction', 'spark.reducer.maxSizeInFlight',
       'spark.shuffle.file.buffer',
       #'executorCounts',
       'duration',
       'spark.broadcast.compress_false','spark.broadcast.compress_true','spark.io.compression.codec_lz4',
       'spark.io.compression.codec_lzf',  'spark.io.compression.codec_snappy',  'spark.GC_-XX:+UseConcMarkSweepGC',
       'spark.GC_-XX:+UseG1GC',  'spark.GC_-XX:+UseParallelGC',  'spark.GC_-XX:+UseSerialGC',  'spark.shuffle.compress_false',
       'spark.shuffle.compress_true',  'spark.shuffle.spill.compress_false',  'spark.shuffle.spill.compress_true','spark.shuffle.manager_sort']# didnt change]
dfapps_ml = dfapps[col_for_train]

def train_linear_model_app(dfapps):
    collist = dfapps_ml.columns.drop('duration')
    X = dfapps_ml[collist]
    Y = dfapps_ml['duration']
    mod = sm.OLS(Y,X)
    results = mod.fit()
    print(results.summary())


#### VAMOS A EXPORTAR PARA ANALIZAR EN R

col_for_train = [
       #'spark.executor.cores',
       #'spark.executor.memory',
       'spark.locality.wait', 'spark.memory.fraction',
       'spark.memory.storageFraction', 'spark.reducer.maxSizeInFlight',
       'spark.shuffle.file.buffer',
       #'executorCounts',
       'duration',
       'spark.broadcast.compress','spark.io.compression.codec',
       'spark.GC', 'spark.shuffle.compress',
       'spark.shuffle.spill.compress','spark.shuffle.io.preferDirectBufs','spark.shuffle.manager']# didnt change]


### For the paper

sns.set_style("whitegrid")
sns.set_context("paper")

### THE WORD COUNT BENCHMARK OF 13th of JULY

figure = dfapps.query("parallelism=='1/1g/1' or  duration==54860 or duration==142923")
g = sns.barplot("parallelism","duration",data=figure)
g.set(xticklabels=["Default","1/6g/1","6/4g/1"])
g.set(title="WordCount")
g.set(ylabel="duration in ms")

figure3 = dfapps.query("duration==54860 or totalTaskDuration==2005756")
dffinal = pd.melt(figure3[['totalTaskDuration','duration','parallelism']],value_vars=['totalTaskDuration','duration'],id_vars=['parallelism'])
g = sns.barplot(x="parallelism",y="value",hue="variable",data=dffinal)
g.set(ylabel="duration in ms")
g.set_title("WordCount")


## THE SORT AND INTERFERENCE BENCHMARK OF 14 JULY

## parallelisation of the sort benchmark
dfapps_1= dfapps.loc[~(dfapps['appId'].isin(['application_1468488054401_0086','application_1468488054401_0087','application_1468488054401_0088','application_1468488054401_0089','application_1468488054401_0090'])) &
           (dfapps['name']=='BigDataBench Sort')]
figure2 = dfapps_1.query("parallelism=='1/1g/1' or duration==168037 or duration==348309")
g = sns.barplot("parallelism","duration",data=figure2)
g.set(xticklabels=["Default","1/6g/1","5/4g/1"])
g.set(title="Sort")
g.set(ylabel="duration in ms")

## Interference of I/O based workloads

figure4 = dfapps[dfapps.appId.isin(['application_1468488054401_0086','application_1468488054401_0087','application_1468488054401_0088','application_1468488054401_0089','application_1468488054401_0090'])]

x = figure4.duration
y = figure4.appId

colors = ['grey' if _y in ['application_1468488054401_0087','application_1468488054401_0088','application_1468488054401_0089'] else 'red' for _y in y]



plt.style.use('seaborn-paper')
fig,p1 = plt.subplots()
p1.set_ylabel("Application")
p1.yaxis.set_visible(False)
norm = colors.Normalize(0,3)
scalar_map = cm.ScalarMappable(norm=norm, cmap='hsv')
p1.set(ylim=(0,6))
color = scalar_map.to_rgba(1)
p1.plot([1468513543341,1468513924529],[5,5],'-',label='application_1468488054401_0086',lw=4,c=color)
p1.plot([1468514244423,1468514857022],[4,4],'--',label='application_1468488054401_0087',lw=4,c=scalar_map.to_rgba(2))
p1.plot([1468514244423,1468514860240],[3,3],'--',label='application_1468488054401_0088',lw=4,c=scalar_map.to_rgba(2))
p1.plot([1468514244433,1468514861432],[2,2],'--',label='application_1468488054401_0089',lw=4,c=scalar_map.to_rgba(2))
p1.plot([1468514254438,1468514863743],[1,1],'--',label='application_1468488054401_0090',lw=4,c=scalar_map.to_rgba(3))
plt.legend()
p1.set_xlabel("Epoch Time")


### P values for SVM

SVM = dfapps[~dfapps.appId.isin(['application_1468826928991_0001','application_1468826928991_0091'])]
SVM.to_csv('/Users/alvarobrandon/Desktop/dfapps_SVM.csv',index=False)

