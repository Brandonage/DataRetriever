import matplotlib as mpl
import matplotlib.colors as colors
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import seaborn as sns
from pandas import melt
from pandas import DataFrame
from pandas import Series




sns.set_context('paper')
sns.set_style('ticks')
sns.set_context("paper", rc={"font.size":12,"axes.titlesize":12,"axes.labelsize":12,"font.scale":12})
parameters = ['sys_contswitch','cpu_usr','cpu_wait','paging_in','disk_write']
# sns.set_context('paper')
# sns.set_style('whitegrid')
# plt.rc('font', family='serif', serif='Times')
# plt.rc('xtick', labelsize=12)
# plt.rc('ytick', labelsize=12)
# plt.rc('axes', labelsize=12)
# pl



### Auxiliary functions #####
def time_interval_app_stage_host(dfm, application,stageId, hostname): ## The interval of time that an stage for an app is running in a stage
    d = dfm.loc[(dfm['stageId']==stageId) & (dfm['appId']==application) & (dfm['hostname']==hostname),['start','end']]
    return[d.start.min()-1000,d.end.max()+1000]

def create_time_series_host(df,host,parameters,timestart, timeend):  ## Returns a dataframe with the time and the value of each of the metrics specified in parameters
    return df.loc[(df["host"]==host) & (df["parameter"].isin(parameters) ) & (df["time"]<timeend) & (df["time"]>timestart),["time","value","parameter"]]

def p_stage_in_host(dfm,dfg,appId,stageId,parameters,host,normalise): ## plots a time series of the parameters chosen for a given stage, app and host.
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

    features = ['start','end','stageName','duration','status','JVMGCTime','SchedulerDelayTime','ExecutorDeserializeTime','endReason','ClassName','Description','ExecutorRunTime','MemoryBytesSpilled','DiskBytesSpilled']
    [s,e] = time_interval_app_stage_host(dfm, appId,stageId, host)
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
    ## What is going on
    for name, group in tasks:
        print name
        print group[features]


def p_kde_application(dfm,appId):  ### It plots the kerned distribution of a given application for cpu_usr, cpu_wait, mem_used and disk_write
    f, (ax1, ax2, ax3, ax4) = plt.subplots(4)
    sns.kdeplot(dfm['cpu_usr'].loc[dfm["appId"]==appId],ax=ax1)
    sns.kdeplot(dfm['cpu_wait'].loc[dfm["appId"]==appId],ax=ax2)
    sns.kdeplot(dfm['mem_used'].loc[dfm["appId"]==appId],ax=ax3)
    sns.kdeplot(dfm['disk_write'].loc[dfm["appId"]==appId],ax=ax4)

def p_mintask_vs_minapp(dfapps):
    figure3 = dfapps.query("duration==54860 or totalTaskDuration==2005756")
    dffinal = melt(figure3[['totalTaskDuration','duration','parallelism']],value_vars=['totalTaskDuration','duration'],id_vars=['parallelism'])
    g = sns.barplot(x="parallelism",y="value",hue="variable",data=dffinal)
    g.set(ylabel="duration in ms")
    g.set_title("WordCount")

def locality_proportion(dfm,appId): ## Proportion of tasks and its locality
    g = sns.countplot(x="locality",data=dfm.loc[dfm['appId']==appId])

def locality_duration(dfm,appId): ## Mean duration of tasks by locality
    g = sns.barplot(x="locality",y="duration",data=dfm.loc[dfm['appId']==appId])

def parallelism_effect(dfapps,xmetric,ymetric): ## A grid that shows types of applications and how parallelism affects a given metric (could be duration, a gmone metric or any other )
    grid = sns.FacetGrid(dfapps.sort(xmetric), col="name", hue="name", col_wrap=2,sharey=False,sharex=False)
    grid.map(plt.plot, xmetric, ymetric, marker="o", ms=4)

def best_worst_default(dfapps,name,title):
    app = dfapps.loc[(dfapps['name']==name) & (dfapps['status']==2)]
    three = app.loc[(dfapps['spark.executor.cores']=='1') & (dfapps['spark.executor.memory']=='1g')] ## The default configuration
    three = three.append((app.loc[app['duration']==app.duration.min()])) ## We append the app run with minimum duration
    three = three.append((app.loc[app['duration']==app.duration.max()])) ## We append the app run with maximum duration
    three['p'] = three['spark.executor.memory'] + '/' + three['spark.executor.cores']
    toplot = three[['duration','energy','p']].set_index(three['p'])
    fig = plt.figure() # Create matplotlib figure
    ax = fig.add_subplot(111) # Create matplotlib axes
    ax2 = ax.twinx()
    width = 0.25
    toplot.duration.plot(kind='bar',color='#626567',ax=ax,width=width,position=1)
    toplot.energy.plot(kind='bar',color='#1D8348',ax=ax2,width=width,position=0)
    lines, labels = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax2.legend(lines + lines2, labels + labels2, loc=0,fontsize=12)
    ax.set_ylabel('Duration (in ms)',fontsize=12)
    ax2.set_ylabel('Energy (in watts)',fontsize=12)
    ax2.grid(b=False)
    ax.grid(b=False)
    ax.set_xlabel('Memory/Cores configuration',fontsize=12,rotation='horizontal')
    for tick in ax.get_xticklabels():
        tick.set_rotation(0)
    plt.title(title,fontsize=12)
    return three

def plot_cross_validation(scores): ## a dictionary witht he values of the scores of the cross validation
    df = DataFrame(scores)
    df = melt(df)
    sns.set_style(style='ticks')
    g = sns.barplot(x='variable',y='value',data=df,color='lightblue')
    g.set_xlabel('Algorithm',fontsize=12)
    g.set_ylabel('MAE',fontsize=12)
    g.set_title('MAE of different algorithms: 10 CV iterations with 3 K-Fold',fontsize=12)


def correlations_metrics(dfk):
    ## Different file sizes are not so different resource usage
    #dfk2 = dfk.loc[(dfk['taskCountsNum'].isin([128,144,81,100]))]
    dfk2 = dfk.loc[~(dfk['taskCountsNum']==1)] ## & (dfk['taskCountsSucceeded']>90)] I've seen that if you take away the ones that have #tasks less than 90 the correlation values decrease
    database2 = prepare_database(dfk2,gigas,cores).reset_index()
    bestconfs = database2.loc[(database2.groupby(['spark.app.name','stageId','taskCountsNum'])['duration'].idxmin())]
    #bestconfs = bestconfs.loc[bestconfs['taskCountsNum']>70]
    bestconfs = bestconfs.loc[bestconfs['duration']>1000]
    ax = sns.regplot(y="taskspernode", x="Memory", data=bestconfs,fit_reg=True,y_jitter=0.2,x_jitter=0.1)
    ax.set_ylim(bottom=0)
    ax.set_xlim(left=0)
    ax.set_xlabel('Value',fontsize=12)
    ax.set_ylabel('Optimal tasks per node',fontsize=12)
    ax.set_title('Percentage of cached data vs optimal tasks per node',fontsize=14)
    view = ['BytesReadDisk', 'BytesWrittenDisk', 'ExecutorDeserializeTime', 'JVMGCTime', 'ShuffleBytesRead','ShuffleBytesWritten','ShuffleReadTime','ShuffleWriteTime',
    'cpu_idl','cpu_usr','cpu_wait','disk_read','disk_write','io_total_read','io_total_write','net_recv','net_send','paging_in','paging_out',
    'spark.executor.cores','spark.executor.memory','sys_contswitch','sys_interrupts']


def signature_similitude_different_sizes(dfk):
    dfkn = dfk.copy()
    dfkn[['cpu_usr','cpu_wait','JVMGCTime','BytesReadDisk','ShuffleBytesWritten','sys_contswitch','Memory','cpu_idl']] = dfkn[['cpu_usr','cpu_wait','JVMGCTime','BytesReadDisk','ShuffleBytesWritten','sys_contswitch','Memory','cpu_idl']].apply(lambda x: (x - x.min()) / (x.max() - x.min()))
    selected = dfkn.loc[(dfk['spark.app.name'].isin(['LinerRegressionApp Example','Spark PCA Example','BigDataBench Sort'])) & (dfkn['spark.executor.bytes']==1024) & (dfkn['stageId'].isin([0,1,2,4])) & (dfkn['taskCountsSucceeded']>1)]
    slice = ['cpu_usr','cpu_wait','JVMGCTime','BytesReadDisk','ShuffleBytesWritten','sys_contswitch','Memory','stageId','taskCountsSucceeded','cpu_idl','appId','spark.app.name']
    selected = selected[slice]
    selected['filesize'] = (selected['taskCountsSucceeded'] * 128)/1024
    selected['Stage'] = 'Stage ' + selected['stageId'].astype('string') + ' of PCA'
    toplot = pd.melt(selected,value_vars=['cpu_usr','cpu_wait','JVMGCTime','BytesReadDisk','ShuffleBytesWritten','sys_contswitch','Memory'],id_vars=['Stage'])
    ax = sns.pointplot(x='variable',y='value',hue='Stage',data = toplot)
    ax.set_ylim(bottom=0,top=1)
    ax.set_xlabel('Metrics')
    ax.set_ylabel('Value (normalised)')
    ax.set_title('Metrics for different execution sizes')

def signature_similitude_different_sample_sizes(dfk):
    selected = dfk.loc[(dfk['spark.app.name'].isin(['LinerRegressionApp Example','Spark PCA Example','BigDataBench Sort','WordCount'])) & (dfk['spark.executor.bytes']==1024) & (dfk['stageId'].isin([0,1,2])) & (dfk['taskCountsSucceeded']>1)]
    sliceOS = ['cpu_usr','cpu_wait','sys_contswitch','stageId','taskCountsSucceeded','cpu_idl','appId','name']
    sliceAPP = ['JVMGCTime','BytesReadDisk','ShuffleBytesWritten','Memory','stageId','taskCountsSucceeded','appId','name']
    selected = selected[sliceOS]
    selected['filesize'] = (selected['taskCountsSucceeded'] * 128)/1024
    toplot = melt(selected,value_vars=['cpu_usr','cpu_wait','cpu_idl'],id_vars=['name','taskCountsSucceeded'])
    ax = sns.FacetGrid(data=toplot,col="name",hue="taskCountsSucceeded")
    ax = (ax.map(sns.pointplot,'variable','value',edgecolor="w").add_legend())


def signature_similitude_two_apps(dfk):
    dfkn = dfk.copy()
    dfkn[['cpu_usr','cpu_wait','JVMGCTime','BytesReadDisk','ShuffleBytesWritten','sys_contswitch','Memory','cpu_idl']] = dfkn[['cpu_usr','cpu_wait','JVMGCTime','BytesReadDisk','ShuffleBytesWritten','sys_contswitch','Memory','cpu_idl']].apply(lambda x: (x - x.min()) / (x.max() - x.min()))
    selected = dfkn.loc[(dfkn['spark.app.name'].isin(['Spark PCA Example','BigDataBench Sort'])) & (dfkn['spark.executor.bytes']==1024) & (dfkn['stageId'].isin([0,4,6])) & (dfkn['taskCountsNum']!=1)]
    slice = ['cpu_usr','cpu_wait','JVMGCTime','BytesReadDisk','ShuffleBytesWritten','sys_contswitch','Memory','stageId','taskCountsSucceeded','cpu_idl','spark.app.name']
    selected = selected[slice]
    #selected['filesize'] = (selected['taskCountsSucceeded'] * 128)/1024
    selected['Stage'] = 'Stage ' + selected['stageId'].astype('string') + ' of ' + selected['spark.app.name']
    toplot = melt(selected,value_vars=['cpu_usr','cpu_wait','JVMGCTime','BytesReadDisk','ShuffleBytesWritten','sys_contswitch','Memory'],id_vars=['Stage'])
    ax = sns.pointplot(x='variable',y='value',hue='Stage',data = toplot)
    ax.set_ylim(bottom=0,top=1)
    ax.set_xlabel('Metrics')
    ax.set_ylabel('Value (normalised)')
    ax.set_title('Value and variance of metrics for different stages executed several times')

def why_1gb(dfk):
    dfkn = dfk.copy()
    dfkn[['cpu_usr','cpu_wait','JVMGCTime','BytesReadDisk','ShuffleBytesWritten','sys_contswitch','Memory','cpu_idl','paging_out']] = dfkn[['cpu_usr','cpu_wait','JVMGCTime','BytesReadDisk','ShuffleBytesWritten','sys_contswitch','Memory','cpu_idl','paging_out']].apply(lambda x: (x - x.min()) / (x.max() - x.min()))
    selected = dfkn.loc[(dfkn['spark.app.name']=='Spark PCA Example') & (dfkn['stageId'].isin([2,4,6]))]
    slice = ['cpu_usr','cpu_wait','JVMGCTime','BytesReadDisk','ShuffleBytesWritten','sys_contswitch','Memory','stageId','cpu_idl','spark.executor.bytes','paging_out']
    selected = selected[slice]
    selected.fillna(0,inplace=True)
    toplot = melt(selected,value_vars=['cpu_usr','cpu_wait','JVMGCTime','BytesReadDisk','ShuffleBytesWritten','sys_contswitch','Memory','paging_out'],id_vars=['spark.executor.bytes','stageId'])
    ax = sns.FacetGrid(toplot,col="stageId",hue="spark.executor.bytes")
    ax = (ax.map(sns.pointplot,"variable","value",edgecolor="w").add_legend()).set(ylim=(0,1),ylabel='Value (normalised)',xlabel='Metrics',title='Metrics for different execution sizes')

def plot_sweet_sport(dfk):
    toplot = dfk.loc[(dfk['spark.app.name']=='BigDataBench Sort') &
             (dfk['stageId']==2) & (dfk['taskCountsNum'].isin([144,128]))]
    slice = ['name','duration','taskspernode']
    toplot = toplot[slice]
    shortestname = Series(['count at GraphLoader.scala:93'] * 9)
    duration = Series([140658,171265,155963,120879,155063,136281,107845,98455,153063])
    taskper = Series(toplot.taskspernode.values)
    d = DataFrame({'name':shortestname,'duration':duration,'taskspernode':taskper})
    name = Series('count at GraphLoader.scala:93')
    g = sns.pointplot(x='taskspernode',y='duration',hue='name',data=toplot)

def plot_evaluation_of_models():
    toplot = DataFrame({'totalDuration':[910495,961827,1024882,],'method':['Best Duration','Optimised Model Duration','Default']})
    g = sns.barplot(x='method',y='totalDuration',data=toplot)







