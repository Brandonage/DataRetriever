from pandas import get_dummies
from sklearn.cross_validation import train_test_split
from sklearn import linear_model
import statsmodels.formula.api as sm




def dfapps_dummy_vars(dfapps):
    dumm_cols = ['spark.broadcast.compress', 'spark.io.compression.codec' , 'spark.GC' , 'spark.shuffle.compress' , 'spark.shuffle.spill.compress',
             'spark.speculation','spark.shuffle.io.preferDirectBufs']
    for col in dumm_cols:
        one_hot = get_dummies(dfapps[col],prefix=col)
        dfapps = dfapps.join(one_hot)

def train_linear_model(dfapps):
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
    train, test = train_test_split(dfapps[col_for_train], test_size = 0.2)
    collist = train.columns.drop('duration')
    X = train[collist]
    Y = train['duration']
    regr = linear_model.LinearRegression()
    regr.fit(X, Y)
    Xtest = test[collist]
    Ytest = test['duration']
    Xtest.join(Ytest)
    Result = regr.predict(Xtest)

def p_values(dfapps):
    col_for_train = [
       'spark.locality.wait', 'spark.memory.fraction',
       'spark.memory.storageFraction', 'spark.reducer.maxSizeInFlight',
       'spark.shuffle.file.buffer',
       'duration',
       'spark.broadcast.compress_false','spark.broadcast.compress_true','spark.io.compression.codec_lz4',
       'spark.io.compression.codec_lzf',  'spark.io.compression.codec_snappy',  'spark.GC_-XX:+UseConcMarkSweepGC',
       'spark.GC_-XX:+UseG1GC',  'spark.GC_-XX:+UseParallelGC',  'spark.GC_-XX:+UseSerialGC',  'spark.shuffle.compress_false',
       'spark.shuffle.compress_true',  'spark.shuffle.spill.compress_false',  'spark.shuffle.spill.compress_true','spark.shuffle.manager_sort']
    collist = dfapps[col_for_train].columns.drop('duration')
    X = dfapps[collist]
    Y = dfapps['duration']
    mod = sm.OLS(Y,X)
    results = mod.fit()
    print(results.summary())

def dfapps_str_to_num(dfapps):
    dfapps['spark.executor.memory'] = dfapps['spark.executor.memory'].replace(to_replace=['5g','2g','3g','512m','4g','1g'],value=[5,2,3,0.512,4,1])
    dfapps['spark.executor.cores'] = dfapps['spark.executor.cores'].astype(float)
    dfapps['spark.executor.instances'] = dfapps['spark.executor.instances'].astype(float)
    dfapps['spark.memory.fraction'] = dfapps['spark.memory.fraction'].astype(float)
    dfapps['spark.locality.wait'] = dfapps['spark.locality.wait'].astype(float)
    dfapps['spark.memory.storageFraction'] = dfapps['spark.memory.storageFraction'].astype(float)
    dfapps['spark.reducer.maxSizeInFlight'] = dfapps['spark.reducer.maxSizeInFlight'].replace(to_replace=['24m','48m','128m','256m'],value=[24,48,128,256])
    dfapps['spark.shuffle.file.buffer'] = dfapps['spark.shuffle.file.buffer'].replace(to_replace=['32k','128k','512k'],value=[32,128,512])
    ## dfapps['spark.locality.wait'] = dfapps['spark.locality.wait'].astype(float) not working because you have to parse the 3s format

def export_forR(path,dfapps):
    dfapps_str_to_num(dfapps)
    col_for_train = [
       'spark.locality.wait', 'spark.memory.fraction',
       'spark.memory.storageFraction', 'spark.reducer.maxSizeInFlight',
       'spark.shuffle.file.buffer',
       'duration',
       'spark.broadcast.compress','spark.io.compression.codec',
       'spark.GC', 'spark.shuffle.compress',
       'spark.shuffle.spill.compress','spark.shuffle.io.preferDirectBufs','spark.shuffle.manager','spark.rdd.compress','spark.serializer']
    dfapps[col_for_train].to_csv(path,index=False)


def gmone_metrics_means(dfapps,dfm,metric):
    means = []
    for index,row in dfapps.iterrows():
        tasks = dfm.loc[dfm['appId']==row['appId']]  ## application_1472731878551_0014 is missing
        print(row['appId'],tasks[metric].mean())
        means.append(tasks[metric].mean())
    return means

def number_of_exec(dfe):
    dfe.loc[dfe['stageId']==0].groupby(['appId','host']).size().reset_index().groupby('appId')[[0]].max()[0]





