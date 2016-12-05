from pymongo import MongoClient
import mongodictbuilder as mongdict
import pandas as pd
from math import ceil
from math import isnan
import math
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import LeaveOneGroupOut
from sklearn.pipeline import make_pipeline
from sklearn import preprocessing
from sklearn.svm import SVC
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.neighbors import NearestNeighbors
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import StandardScaler
import requests
from time import sleep
import seaborn as sns
from sklearn.externals import joblib
from sklearn.neighbors import KNeighborsRegressor
model_path = '/Users/alvarobrandon/Experiments/memory_and_cores/BigBenchmark/pickle/clf.pickle'
cluster_path = '/Users/alvarobrandon/Experiments/memory_and_cores/BigBenchmark/pickle/cluster.pickle'
normaliser_path = '/Users/alvarobrandon/Experiments/memory_and_cores/BigBenchmark/pickle/normaliser.pickle'


class MongoDFBuilder:

    def __init__(self):
        client = MongoClient()
        self.dbm = client.meteor
        self.dbg = client.gmone


    def mongo_stats_avg_host(self,start,end,hostname): ## calculate the mean of the different metrics inside the gmone database for a given HOST, START AND END
        interval=510 ## we test this interval so we don't miss tasks that last less than 1 second ( the monitor period we set in GMone )
        cursor = self.dbg.readings.aggregate([{"$match":{"time":{ "$gt": long(start-interval), "$lt": long(end+interval)},"host":hostname}},
                           {"$group":{"_id":"$parameter","average":{"$avg":"$value"}}}])
        return cursor

    def build_task_attempts_dataframe(self):  ## Builds the task attempts dataframe out of the meteor database and GMone DATAFRAME as a parameter
        ban_parameters = ["cpu_siq","cpu_hiq","mem_buffer","proc","procs_new","procs_blk","sysv_ipc_sem","sysv_ipc_shm","sysv_ipc_shm","sockets_tcp","sockets_udp","sockets_frq","sockets_raw",
                          "disk_percentage","rpc_client_retr","rpc_server_call_erc1","rpc_server_call_xdrc","rpc_server_erau","virtual_mem_majpf","virtual_mem_minpf","virtual_mem_alloc","sysv_ipc_msg","virtual_mem_free",
                          "rpc_server_call" ,"rpc_server_erca"]
        cursor = self.dbm.task_attempts.find()
        rows_list = []
        for doc in cursor:
            d = {}
            try:
                d = mongdict.build_dict_task_attempts(doc)
                ## now we extend the dictionary with the summary statistics of the GMone dataframe for that tasks's [start,end,host]
                stats_cursor = self.mongo_stats_avg_host(start=d.get('start'),end=d.get('end'),hostname=d.get('hostname'))
                for stats_doc in stats_cursor:
                    col_name=stats_doc.get('_id')
                    if col_name not in ban_parameters:
                        value=stats_doc.get('average')
                        d.update({col_name:value})
                ## And also for the entry point in time
                stats_cursor = self.mongo_stats_avg_host(start=d.get('start')-500,end=d.get('start')+500,hostname=d.get('hostname'))
                for stats_doc in stats_cursor:
                    col_name=stats_doc.get('_id')
                    if col_name not in ban_parameters:
                        value=stats_doc.get('average')
                        d.update({col_name + "_entry_point":value})
                stage_cursor = self.dbm.stages.find({'appId':d.get('appId'),'id':d.get('stageId')},{'name':1})
                for stages in stage_cursor:
                    sta = stages.get('name')
                    d.update({"stageName":sta})
            except:
                d.update({'appId':doc.get('_id')}) ## we just insert an _id in case it fails to be able to debug
            rows_list.append(d)
        df = pd.DataFrame(rows_list)
        return df


    def build_readings_dataframe(self):
        ban_parameters = ["cpu_siq","cpu_hiq","mem_buffer","proc","procs_new","procs_blk","sysv_ipc_sem","sysv_ipc_shm","sysv_ipc_shm","sockets_tcp","sockets_udp","sockets_frq","sockets_raw",
                          "disk_percentage","rpc_client_retr","rpc_server_call_erc1","rpc_server_call_xdrc","rpc_server_erau","virtual_mem_majpf","virtual_mem_minpf","virtual_mem_alloc","sysv_ipc_msg","virtual_mem_free",
                          "rpc_server_call" ,"rpc_server_erca"]
        cursor = self.dbg.readings.find({"parameter": { '$nin' : ban_parameters  }  })
        rows_list = []
        for doc in cursor:
            d = {}
            try:
                d = mongdict.build_dict_readings(doc)
            except:
                d.update({'host':doc.get('_id')}) ## we just insert an _id in case it fails
            rows_list.append(d)
        df = pd.DataFrame(rows_list)
        #df.set_index('time',drop=True,inplace=True)
        return df

    def build_stage_attempts_dataframe(self):
        cursor = self.dbm.stage_attempts.find()
        rows_list = []
        for doc in cursor:
            d = {}
            try:
                d = mongdict.build_dict_stage_attempts(doc)
            except:
                d.update({'appId':doc.get('_id')}) ## we just insert an _id in case it fails
            rows_list.append(d)
        df = pd.DataFrame(rows_list)
        return df

    def calculate_task_per_host(self,yarnam,yarnminalloc,execmem,execcores,nNodes,available_mb,ntasks):
        def calculate_allocation(containermem,alloc):
            return float(containermem+max([384,containermem*0.10]))/alloc
        driver_memory=ceil(calculate_allocation(yarnam,yarnminalloc))*yarnminalloc
        exec_memory=ceil(calculate_allocation(execmem,yarnminalloc))*yarnminalloc
        nexec=int(available_mb/exec_memory)
        nslots_for_node=nexec*execcores
        nslots_for_cluster = nslots_for_node * nNodes
        if ntasks < nslots_for_cluster:
            ntasks_for_node=ceil(ntasks/float(nNodes)) # we need to do the float thing to get an int number
            ntasks_for_cluster=ntasks
        else:
            ntasks_for_node = nslots_for_node
            ntasks_for_cluster = nslots_for_cluster
        memoryPerTask = exec_memory/execcores
        return pd.Series([ntasks_for_node,nexec,ntasks_for_cluster,memoryPerTask,nslots_for_cluster],index=['taskspernode','nExecutorsPerNode','tasksincluster','memoryPerTask','slotsInCluster'])

    def calculate_stage_average_osmetrics(self,stageId,attemptId,appId,dfm):
        metrics = ['cpu_wait','cpu_usr','cpu_idl','paging_in','paging_out','disk_read','disk_write','net_recv','net_send','io_total_read','io_total_write','sys_contswitch','sys_interrupts']
        res = dfm[metrics].loc[(dfm['stageId']==stageId) & (dfm['appId']==appId) & (dfm['stageAttemptId']==attemptId) & (dfm['duration']>0)].mean()
        res['tasksThatRunned'] = len(dfm[metrics].loc[(dfm['stageId']==stageId) & (dfm['appId']==appId) & (dfm['stageAttemptId']==attemptId) & (dfm['duration']>0)].index)
        return res


    def calculate_read_method(self,stageId,attemptId,appId,dfm):
        try:
            totales = dfm['DataReadMethod'].loc[(dfm['stageId']==stageId) & (dfm['appId']==appId) & (dfm['stageAttemptId']==attemptId)].value_counts()
            res = totales/totales.sum()
        except:
            res = pd.Series([0.0,0.0,0.0,0.0],index=['Memory','Disk','Hadoop','Network'])
        return res





    def build_signature_stages_dataframe(self,dfm,dfs): ## We will pass the dataframe of tasks and the stages dataframe, we want to keep the original stages just in case
        to_drop = ['end','start','conf', 'parallelism', 'spark.GC', 'spark.broadcast.compress',### We will drop this columns from the stage dataframe since we won't need it for the kernels
                   'spark.executor.instances','spark.io.compression.codec','spark.locality.wait', 'spark.memory.fraction', 'spark.memory.storageFraction', 'spark.rdd.compress', 'spark.reducer.maxSizeInFlight',
                   'spark.serializer', 'spark.shuffle.compress', 'spark.shuffle.file.buffer', 'spark.shuffle.io.preferDirectBufs', 'spark.shuffle.manager', 'spark.shuffle.spill.compress', 'spark.task.cpus']
        norm = ['BytesReadDisk','BytesWrittenDisk','DiskBytesSpilled','ExecutorDeserializeTime','ExecutorRunTime','JVMGCTime','MemoryBytesSpilled','ResultSerializationTime',
                'ResultSize','SchedulerDelayTime','ShuffleBytesRead','ShuffleBytesWritten','ShuffleReadTime','ShuffleWriteTime','totalTaskDuration','taskCountsFailed']
        kernels = dfs.drop(to_drop,axis=1)
        ## calculate the number of tasks per node, the number of executors and the conccurent tasks running in the cluster for that particular stage
        res = kernels.apply(lambda row: self.calculate_task_per_host(512,1024,row['spark.executor.bytes'],int(row['spark.executor.cores']),2,21504,row['taskCountsNum']),axis=1)
        ### concat the results to the previous dataframe
        kernels = pd.concat([kernels,res],axis=1)
        ## calculate the OS average counter metrics for that stage and concat them into the dataframe
        res = kernels.apply(lambda row: self.calculate_stage_average_osmetrics(row['stageId'],row['id'],row['appId'],dfm),axis=1)
        kernels = pd.concat([kernels,res],axis=1)
        ## normalize the metrics of Spark on the number of tasks spawned for that stage
        kernels[norm]=kernels[norm].astype('float').div(kernels.tasksThatRunned.astype('float'),axis='index')
        ## last we decide if the stage read from memory or from disk
        res = kernels.apply(lambda row: self.calculate_read_method(row['stageId'],row['id'],row['appId'],dfm),axis=1)
        kernels = pd.concat([kernels,res],axis=1)
        kernels['nWaves'] = kernels.taskCountsNum/kernels.slotsInCluster
        #kernels.loc[kernels['nWaves']<1,'nWaves']=1
        return kernels

    def build_ml_dataframe(self,dfk,gigas,cores):
        drop_this_for_training = ['appId','id','jobId','name','stageId','spark.app.name','spark.executor.memory', 'taskCountsRunning' ,
                                  'taskCountsSucceeded','slotsInCluster','nExecutorsPerNode', 'tasksincluster'#'totalTaskDuration',
                                  ,'ExecutorRunTime', 'ResultSize','ResultSerializationTime','disk_read','disk_write','net_recv','net_send',
                                  'SchedulerDelayTime','ExecutorDeserializeTime','SchedulerDelayTime','status']#,'io_total_read','io_total_write','paging_in','paging_out']#,'cpu_usr','cpu_wait','cpu_idl','sys_contswitch','sys_interrupts'] #,'spark.executor.bytes','tasksincluster'
        def feature_importances(clf,columns): #feature_importances(clf.named_steps["gradientboostingregressor"],database.columns.drop(drop_this_for_training).drop('duration'))
            importances  = zip(clf.feature_importances_,columns)
            return importances

        def prepare_database(dfk,gigas,cores): ## prepare a dataframe with all the apps from dfk that has {signature of App metrics and system metrics for 1g/1core, configuration, parallelism metrics,
            reference = dfk.loc[(dfk['spark.executor.memory']==gigas) & (dfk['spark.executor.cores']==cores) & (dfk['status']!=4)] ### This are the stage executions with the default configuration for gigas and cores
            ## we only want to take the configuration parameters of the executions that are not default and attach the signatures of 1G's and 1Core contained in reference dataframe
            to_add = dfk[['appId','memoryPerTask','spark.executor.bytes','spark.executor.memory','spark.executor.cores','taskspernode','nWaves','nExecutorsPerNode',
                          'tasksincluster','spark.app.name','stageId',# we took away status from the features
                         'duration','slotsInCluster','taskCountsNum']].loc[~((dfk['spark.executor.memory']==gigas) & (dfk['spark.executor.cores']==cores)) & (dfk['status']!=4)] ## status 4 are stages that are skipped. We don't want those
            database = reference.drop(['appId','memoryPerTask','spark.executor.bytes','spark.executor.memory','spark.executor.cores','nExecutorsPerNode',
                         'duration','slotsInCluster','tasksincluster','taskspernode','nWaves','taskCountsNum'],axis=1).merge(to_add,on=['spark.app.name','stageId']) ## we merge the signatures of the default executions and the non-default
            # database = database.drop(['duration_x','spark.executor.bytes_x', 'spark.executor.cores_x', 'spark.executor.memory_x',  'taskspernode_x',  'nExecutors_x',  'tasksincluster_x','memoryPerTask_x'],axis=1) ## QUITADO STATUS
            # database = database.rename(columns={'memoryPerTask_y': 'memoryPerTask', 'spark.executor.bytes_y': 'spark.executor.bytes', 'spark.executor.memory_y': 'spark.executor.memory', 'spark.executor.cores_y': 'spark.executor.cores'
            #                        ,'taskspernode_y' : 'taskspernode' , 'nExecutors_y': 'nExecutors' , 'tasksincluster_y' : 'tasksincluster' , 'status_y' : 'status' , 'duration_y' : 'duration' })
            database = database.append(reference) ## we unify the reference default executions with the non-default. We also eliminate any 0's
            database = database.fillna(0)
            ## Let's reorder the dataframe to have duration at the front
            duration = database['duration']
            database.drop(['duration'],axis=1,inplace = True)
            database['duration'] = duration
            return database

        def prepare_database_2(dfk): ## prepare a dataframe with all the apps from dfk that has {signature of App metrics and system metrics for 1g/1core, configuration, parallelism metrics,
            reference = dfk.loc[(dfk['status'].isin([3,2,1]))] ### This are the stage executions with the default configuration for gigas and cores
            ## we only want to take the configuration parameters of the executions that are not default and attach the signatures of 1G's and 1Core contained in reference dataframe
            to_add = dfk[['appId','memoryPerTask','spark.executor.bytes','spark.executor.memory','spark.executor.cores','taskspernode','nWaves','nExecutorsPerNode',
                          'tasksincluster','spark.app.name','stageId',# we took away status from the features
                         'duration','slotsInCluster','taskCountsNum']].loc[(dfk['status'].isin([3,2,1]))] ## status 4 are stages that are skipped. We don't want those
            database = reference.drop(['appId','spark.executor.memory','nExecutorsPerNode',
                         'duration','slotsInCluster','tasksincluster','nWaves'],axis=1).merge(to_add,on=['spark.app.name','stageId'],suffixes=(['_ref','_if'])) ## we merge the signatures of the default executions and the non-default
            # database = database.drop(['duration_x','spark.executor.bytes_x', 'spark.executor.cores_x', 'spark.executor.memory_x',  'taskspernode_x',  'nExecutors_x',  'tasksincluster_x','memoryPerTask_x'],axis=1) ## QUITADO STATUS
            # database = database.rename(columns={'memoryPerTask_y': 'memoryPerTask', 'spark.executor.bytes_y': 'spark.executor.bytes', 'spark.executor.memory_y': 'spark.executor.memory', 'spark.executor.cores_y': 'spark.executor.cores'
            #                        ,'taskspernode_y' : 'taskspernode' , 'nExecutors_y': 'nExecutors' , 'tasksincluster_y' : 'tasksincluster' , 'status_y' : 'status' , 'duration_y' : 'duration' })
            database = database.fillna(0)
            ## Let's reorder the dataframe to have duration at the front
            duration = database['duration']
            database.drop(['duration'],axis=1,inplace = True)
            database['duration'] = duration
            return database

        def signature_similitude_different_sizes(dfk):
            dfkn = dfk.copy()
            dfkn[['cpu_usr','cpu_wait','JVMGCTime','BytesReadDisk','ShuffleBytesWritten','sys_contswitch','Memory','cpu_idl']] = dfkn[['cpu_usr','cpu_wait','JVMGCTime','BytesReadDisk','ShuffleBytesWritten','sys_contswitch','Memory','cpu_idl']].apply(lambda x: (x - x.min()) / (x.max() - x.min()))
            selected = dfkn.loc[(dfkn['spark.app.name']=='Spark PCA Example') & (dfkn['spark.executor.bytes']==1024) & (dfkn['stageId'].isin([2,4]))]
            slice = ['cpu_usr','cpu_wait','JVMGCTime','BytesReadDisk','ShuffleBytesWritten','sys_contswitch','Memory','stageId','taskCountsSucceeded','cpu_idl']
            selected = selected[slice]
            selected['filesize'] = (selected['taskCountsSucceeded'] * 128)/1024
            selected['Stage'] = 'Stage ' + selected['stageId'].astype('string') + ' of PCA'
            toplot = pd.melt(selected,value_vars=['cpu_usr','cpu_wait','JVMGCTime','BytesReadDisk','ShuffleBytesWritten','sys_contswitch','Memory'],id_vars=['Stage'])
            ax = sns.pointplot(x='variable',y='value',hue='Stage',data = toplot)
            ax.set_ylim(bottom=0,top=1)
            ax.set_xlabel('Metrics')
            ax.set_ylabel('Value (normalised)')
            ax.set_title('Metrics for different execution sizes')

        def grid_search_boost(database):
            # tuned_parameters = [{'max_depth': [3, 5, 7, 10, 12, 20],
            #                      'learning_rate': [0.1,0.05,0.01,0.005,0.001,0.0005],
            #                      'n_estimators': [30,50,100,300,400]}]
            boost = GradientBoostingRegressor()
            tuned_parameters = [{'max_depth': [3, 5, 7, 10, 20],
                                 'learning_rate': [0.1,0.05,0.01,0.005,0.0005],
                                 'n_estimators': [100,150,300,400,500,700]}]
            X_train = database.drop(drop_this_for_training,axis=1).drop('duration',axis=1)
            Y_train = database['duration']
            clf = GridSearchCV(boost,tuned_parameters,cv=10,scoring='neg_mean_absolute_error')
            clf.fit(X_train,Y_train)

        def grid_search_boost_logo(database):
            tuned_parameters = [{'max_depth': [3, 7, 15],
                                 'learning_rate': [0.1,0.01,0.005]}
                                 #'n_estimators': [1000,1500]}
                                ]
            boost = GradientBoostingRegressor(n_estimators=1500)
            # tuned_parameters = [{'learning_rate': [0.1,0.15], 'n_estimators': [100,150,300],
            #                    'min_samples_split':[2,10],'min_samples_leaf':[2,10],
            #                    'subsample':[1,0.7],'max_features': ['auto','sqrt','log2'], 'min_impurity_split': [1e-7, 1e-5]}]
            logo = LeaveOneGroupOut().get_n_splits(database.drop(drop_this_for_training,axis=1).drop('duration',axis=1),y=database['duration'],groups=database['spark.app.name'])
            X_train = database.drop(drop_this_for_training,axis=1).drop('duration',axis=1)
            Y_train = database['duration']
            clf = GridSearchCV(boost,tuned_parameters,cv=logo,scoring='neg_mean_absolute_error')
            clf.fit(X_train,Y_train)

        def predict_with_model(scaler,scalery,train,test):
            train_np = scaler.transform(train.drop(drop_this_for_training,axis=1))
            test_np = scaler.transform(test.drop(drop_this_for_training,axis=1))
            ytrain_np = train_np[:,-1]
            xtrain_np = train_np[:,0:-1]
            ytest_np = test_np[:,-1]
            xtest_np = test_np[:,0:-1]
            clf = GradientBoostingRegressor(learning_rate=0.1,max_features='auto',min_impurity_split=1e-05,min_samples_leaf=2,min_samples_split=10,n_estimators=150,subsample=0.7,max_depth=12)
            #clf = SVR()
            clf.fit(xtrain_np, ytrain_np)
            Result = clf.predict(xtest_np)
            Result = scalery.inverse_transform(Result)
            print(feature_importances(clf,database.columns.drop(drop_this_for_training).drop('duration')))
            return Result

        def different_models_predict(classifiers,names,database):
            train, test = train_test_split(database, test_size = 0.2)
            final_test = test.copy(deep=True)
            for name,clf in zip(names,classifiers):
                print name
                pipe = make_pipeline(preprocessing.StandardScaler(),clf)
                pipe.fit(train.drop(drop_this_for_training,axis=1).drop('duration',axis=1),train['duration'])
                final_test['result '+name] = pipe.predict(test.drop(drop_this_for_training,axis=1).drop('duration',axis=1))
            return final_test

        def different_models_with_lol(classifiers,names,database):
            final_scores = {}
            logo = LeaveOneGroupOut().split(database.drop(drop_this_for_training,axis=1).drop('duration',axis=1),y=database['duration'],groups=database['spark.app.name'])
            for train_index, test_index in logo:
                print database['spark.app.name'].iloc[test_index].unique()
            for name,clf in zip(names,classifiers):
                print name
                logo = LeaveOneGroupOut().split(database.drop(drop_this_for_training,axis=1).drop('duration',axis=1),y=database['duration'],groups=database['spark.app.name'])
                pipe = make_pipeline(preprocessing.StandardScaler(),clf)
                scores= cross_val_score(pipe,database.drop(drop_this_for_training,axis=1).drop('duration',axis=1),database['duration'],cv=logo,scoring='neg_mean_absolute_error')
                final_scores.update({name:scores})
            return final_scores


        def evaluating_different_models(regressors,names,database):## Will return a series of scores for each application
            final_scores = {}
            for name,clf in zip(names,regressors):
                print name
                pipe = make_pipeline(preprocessing.StandardScaler(),clf)
                scores=cross_val_score(pipe,database.drop(drop_this_for_training,axis=1).drop('duration',axis=1),database['duration'],cv=10,scoring='neg_mean_absolute_error')
                final_scores.update({name : scores})
            return final_scores

        def evaluating_classifiers(classifiers,names,database):## Will return a series of scores for each application
            final_scores = {}
            for name,clf in zip(names,regressors):
                print name
                pipe = make_pipeline(preprocessing.StandardScaler(),clf)
                scores=cross_val_score(pipe,database.drop(drop_this_for_training,axis=1).drop('duration',axis=1),database['duration'],cv=10,scoring='neg_mean_absolute_error')
                final_scores.update({name : scores})
            return final_scores

        def sacar_signature_aplicaciones(aplicaciones,database):
            return database[(database['spark.app.name'].isin(aplicaciones)) & ((database['spark.executor.memory']=='1g') & (database['spark.executor.cores']=='1'))]

        def df_to_plot_accuracy(results):
            toplot = results[['stageId','duration','Result','totalTaskDuration','spark.executor.cores_if','spark.executor.memory','taskCountsNum_if']]
            toplot['step'] = toplot['spark.executor.memory'] + '/' + toplot['spark.executor.cores_if']
            res = toplot.loc[(toplot['stageId']==0)&(toplot['taskCountsNum_if']==65)]
            res = pd.melt(res,value_vars=['duration','Result'],id_vars=['stageId','totalTaskDuration','spark.executor.cores_if','spark.executor.memory','step'])
            ax = sns.pointplot(x='step',y='value',hue='variable',data = res,ci=None)
            ax.set_ylim(bottom=(res.value.min() - 3000))
            ax.set_xlabel('Configuration')
            ax.set_ylabel('Duration in ms')
            ax.set_title('Stage 0 of Spark ShortestPath Application')

        def prepare_database_cluster(dfk,dfapps):
            database = dfk.loc[(dfk['spark.executor.memory']=='1g') & (dfk['spark.executor.cores']=='1')]
            database = database.fillna(0)
            drop_for_clustering = ['appId','id','jobId','name','stageId','spark.executor.memory','spark.executor.cores', 'taskCountsRunning' , ## we have to drop parallelism features, noisy ones
                                  'taskCountsSucceeded','slotsInCluster','nExecutorsPerNode', 'tasksincluster'#'totalTaskDuration',     ## and all the identifiers (stageId, jobId and so on)
                                  , 'ResultSize','ResultSerializationTime','memoryPerTask','spark.executor.bytes','disk_read','disk_write','net_recv','net_send',
                                  'paging_in','paging_out','io_total_read','io_total_write','duration','tasksincluster','taskspernode','nWaves','spark.app.name']
            test =  database.loc[(database['spark.app.name'].isin(['Spark PCA Example','SupporVectorMachine','Grep','Spark ShortestPath Application','RDDRelation','Spark ConnectedComponent Application']))]
            train = database.drop(database[(database['spark.app.name'].isin(['Spark PCA Example','SupporVectorMachine','Grep','Spark ShortestPath Application','RDDRelation','Spark ConnectedComponent Application']))# & ~((database['spark.executor.memory']==gigas) & (database['spark.executor.cores']==cores))
                                  ].index)
            train_x = train.drop(drop_for_clustering,axis=1)
            #test_x = test.drop(drop_for_clustering,axis=1)
            # test_x = test.loc[test['appId']=='application_1479154708246_0005'].drop(drop_for_clustering,axis=1)
            scaler = preprocessing.StandardScaler().fit(train_x)
            X = scaler.transform(train_x)
            clf = NearestNeighbors(n_neighbors=4)
            clf.fit(X)
            joblib.dump(clf, cluster_path)
            joblib.dump(scaler,normaliser_path)
            #res = clf.kneighbors(scaler.transform(test_x))
            #train.iloc[res[1][0]]
            #res = database.apply(lambda row: best_conf(row['stageId'],row['id'],row['appId'],dfm),axis=1)




        def df_to_plot_accuracy_grid(results):
            toplot = results[['stageId','duration','Result','totalTaskDuration','spark.executor.cores','spark.executor.memory','taskCountsNum','tasksThatRunned']]
            toplot['step'] = toplot['spark.executor.memory'] + '/' + toplot['spark.executor.cores']
            res = toplot.loc[(toplot['stageId']==2)]
            res = pd.melt(res,value_vars=['duration','Result'],id_vars=['stageId','totalTaskDuration','spark.executor.cores','spark.executor.memory','step','taskCountsNum','tasksThatRunned'])
            ax = sns.FacetGrid(data=res,col="taskCountsNum",hue="variable")
            ax = (ax.map(sns.pointplot,'step','value',edgecolor="w").add_legend())
            ax.set_ylim(bottom=(res.value.min() - 3000))
            ax.set_xlabel('Configuration')
            ax.set_ylabel('Duration in ms')
            ax.set_title('Stage 0 of Grep')


        def return_model(dfk,gigas,cores):
            database = prepare_database_2(dfk)
            #train, test = train_test_split(database, test_size = 0.2)  ## WE CAN DO A SPLIT OR PASS A CONSTRUCT OUR OWN TRAIN AND TEST DATASET
            test = database.loc[(database['spark.app.name'].isin(['Spark PCA Example','Grep','SupporVectorMachine','Spark ShortestPath Application','LogisticRegressionApp Example','Spark ConnectedComponent Application']))]# & ~((database['spark.executor.memory'].isin(['1g','3g'])) & (database['spark.executor.cores'].isin(['1','4']))]
            train = database.drop(database[(database['spark.app.name'].isin(['Spark PCA Example','Grep','SupporVectorMachine','Spark ShortestPath Application','LogisticRegressionApp Example','Spark ConnectedComponent Application']))# & ~((database['spark.executor.memory']==gigas) & (database['spark.executor.cores']==cores))
                                  ].index)
            train_x = train.drop(drop_this_for_training,axis=1).drop('duration',axis=1)
            test_x = test.drop(drop_this_for_training,axis=1).drop('duration',axis=1)
            train_y = train['duration']
            tree= GradientBoostingRegressor(alpha=0.9, criterion='friedman_mse', init=None,
             learning_rate=0.1, loss='ls', max_depth=8, max_features=None,
             max_leaf_nodes=None, min_impurity_split=1e-07,
             min_samples_leaf=1,
                 min_weight_fraction_leaf=0.0, n_estimators=2500, presort='auto',
             random_state=None, subsample=1.0, verbose=0, warm_start=False)
            #neigh = KNeighborsRegressor(n_neighbors=10,weights='distance')
            clf = make_pipeline(preprocessing.StandardScaler(),tree)
            train_x = train_x.sort(axis=1)
            clf.fit(train_x,train_y)  ## remember to always sort a pandas dataframe when passing it to a sklearn method
            test_x = test_x.sort(axis=1)
            Result = clf.predict(test_x)  ## remember to always sort a pandas dataframe when passing it to a sklearn method
            test['Result'] = Result
            test = test.sort(['stageId','name'])
            print_full(test)
            joblib.dump(clf,model_path)
            #joblib.dump(clf,model_path) test.loc[((test['spark.app.name']=="Spark PCA Example") & (test['taskCountsNum']==144) & (test['tasksThatRunned']==81))].groupby("conf")['duration'].sum()


        #scalery = preprocessing.StandardScaler().fit(database['duration'].reshape(-1,1))
        return test





    def build_apps_dataframe(self,dfs):
        cursor = self.dbm.apps.find()
        rows_list = []
        for doc in cursor:
            d = {}
            try:
                d = mongdict.build_dict_apps(doc)
                status = dfs.loc[dfs['appId']==d.get('appId')].status.unique() ## we have to fix a problem where apps are tagged as status 2 even if some of the stages failed after being executed
                if ((1 in status) or (3 in status) or (np.isnan(status).any())): ## if one of the stages of the app has failed then mark the app as failed
                    d.update({'status':3})
            except:
                d.update({'appId':doc.get('_id')}) ## we just insert an _id in case it fails
            rows_list.append(d)
        df = pd.DataFrame(rows_list)
        return df


    def build_environment_dataframe(self):
        cursor = self.dbm.environment.find({},{'spark':1,'appId':1})
        rows_list = []
        for doc in cursor:
            d = {}
            try:
                d = mongdict.build_dict_environment(doc)
            except:
                d.update({'appId':doc.get('_id')})
            rows_list.append(d)
        df = pd.DataFrame(rows_list)
        return df

    def build_jobs_dataframe(self):
        cursor = self.dbm.jobs.find()
        rows_list = []
        for doc in cursor:
            d = {}
            try:
                d = mongdict.build_dict_jobs(doc)
            except:
                d.update({'appId':doc.get('_id')}) ## we just insert an _id in case it fails
            rows_list.append(d)
        df = pd.DataFrame(rows_list)
        return df

    def build_stage_executors_dataframe(self):
        cursor = self.dbm.stage_executors.find()
        rows_list = []
        for doc in cursor:
            d = {}
            try:
                d = mongdict.build_dict_stage_executors(doc)
            except:
                d.update({'appId':doc.get('_id')}) ## we just insert an _id in case it fails
            rows_list.append(d)
        df = pd.DataFrame(rows_list)
        return df

    def energy_consumption_stage(self,dfs,dfm):
        def get_energy_consumption(start,end,appId,stageId,stageAttemptId,status,duration):
            print (appId,stageId,stageAttemptId)
            if status==4: ## If it's an stage that was skipped we don't need the energy
                return 0
            elif isnan(start): ## It it doesn't have a start and duration we cannot calculate the energy
                return 0
            else:
                if isnan(end): ## If it doesn't have a timestamp for end we sum start time +  duration
                    e = int(start/1000) + int(duration/1000)
                else:
                    e = int(end/1000)
                s = int(start/1000)
                proxies = {'http': 'socks5://localhost:5000'}
                machines = dfm.loc[(dfm['appId']==appId) & (dfm['stageId']==stageId) & (dfm['stageAttemptId']==stageAttemptId) ].hostname.unique()
                listOfNodes = ''
                for node in machines:
                    if node!= None: ## If the task failed we can get a None type node
                        listOfNodes = listOfNodes + node + ','
                listOfNodes = listOfNodes.replace('.lyon.grid5000.fr','')[:-1]
                tries = 0
                while True:
                    r = requests.get('http://kwapi.lyon.grid5000.fr:12000/power/timeseries/?from=' + str(s) + '&to=' + str(e) + '&only=' + listOfNodes,proxies=proxies)
                    global_energy = []
                    for item in r.json().get('items'):
                        global_energy.extend(item.get('values'))
                    result = sum(global_energy)
                    if (result==0):
                        print 'Grid 5000 Api error. Will sleep 2 secs and try again'
                        sleep(2)
                        tries = tries + 1
                        if (tries == 20):
                            result = 0
                            break
                        else:
                            continue
                    else:
                        break
                return result
        energy = dfs.apply(lambda row: get_energy_consumption(row['start'],row['end'],row['appId'],row['stageId'],row['id'],row['status'],row['duration']),axis=1)
        return energy

    def energy_consumption_apps(self,dfapps,dfs):
        energy = []
        apps = dfapps.appId
        for a in apps:
            energy.append(dfs.loc[(dfs['appId']==a)].energy.sum())
        return energy




