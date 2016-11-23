import mongodfbuilder
from pandas import set_option
from pandas import read_pickle
from pandas import set_option,reset_option
import preprocessingutils as pre
import plotutils as pl
from sklearn.linear_model import BayesianRidge
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import SGDRegressor
from sklearn.linear_model import Lasso
from sklearn.svm import SVR
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.neural_network import MLPRegressor

regressors = [
            BayesianRidge(),
            LinearRegression(),
            SGDRegressor(),
            Lasso(max_iter=100),
            SVR(),
            GradientBoostingRegressor(),
            MLPRegressor()]

names = ["BayesianRidge", "LinearRegression", "SGDRegressor", "Lasso",
         "SVR", "GradientBoostingRegressor", "MLPRegressor"]

mr = mongodfbuilder.MongoDFBuilder()
path = "/Users/alvarobrandon/Experiments/memory_and_cores/BigBenchmark"


def export_set(path):
    dfg.to_pickle(path + '/pickle/dfg.pickle')
    dfm.to_pickle(path + '/pickle/dfm.pickle')
    dfj.to_pickle(path + '/pickle/dfj.pickle')
    dfs.to_pickle(path + '/pickle/dfs.pickle')
    dfenv.to_pickle(path + '/pickle/dfenv.pickle')
    dfapps.to_pickle(path + '/pickle/dfapps.pickle')
    dfk.to_pickle(path + '/pickle/dfk.pickle')

def import_set(path):
    dfg2 = read_pickle(path + '/pickle/dfg.pickle')
    dfm2 = read_pickle(path + '/pickle/dfm.pickle')
    dfj2 = read_pickle(path + '/pickle/dfj.pickle')
    dfs2 = read_pickle(path + '/pickle/dfs.pickle')
    dfenv2 = read_pickle(path + '/pickle/dfenv.pickle')
    dfapps2 = read_pickle(path + '/pickle/dfapps.pickle')
    dfk2 = read_pickle(path + '/pickle/dfk.pickle')



def import_set(path):
    dfg = dfg2.append(dfg)
    dfm = dfm2.append(dfm)
    dfj = dfj2.append(dfj)
    dfs = dfs2.append(dfs)
    dfenv = dfenv2.append(dfenv)
    dfapps = dfapps2.append(dfapps)
    dfk = dfk2.append(dfk)



def widen_display():
    set_option('display.max_columns', 500)
    set_option('display.float_format', lambda x: '%.3f' % x)
    set_option('display.width',2000)

def print_full(x):
    set_option('display.max_rows', len(x))
    print(x)
    reset_option('display.max_rows')

def normalise_gmone_df(dfg):
    dfg['value'] = dfg.groupby('parameter')['value'].transform(lambda x:  (x - x.mean()) / (x.max() - x.min()))




widen_display()
dfg = mr.build_readings_dataframe()
dfm = mr.build_task_attempts_dataframe()
dfj = mr.build_jobs_dataframe()
dfs = mr.build_stage_attempts_dataframe()
#dfs['energy'] = mr.energy_consumption_stage(dfs,dfm) ## we attach the energy for the stages
dfe = mr.build_stage_executors_dataframe()
dfenv = mr.build_environment_dataframe()
dfapps = mr.build_apps_dataframe(dfs)
dfapps = dfapps.merge(dfenv,on='appId').sort('start')
#dfapps['energy'] = mr.energy_consumption_apps(dfapps,dfs) ## we attach the energy for the apps
dfs = dfs.merge(dfenv,on='appId').sort('start')
dfk = mr.build_signature_stages_dataframe(dfm,dfs)
test = mr.build_ml_dataframe(dfk,'1g','1')
dfg_norm = dfg
normalise_gmone_df(dfg_norm)

### We will export the things to an excel file for an analysis in R



dfk = read_pickle('/Users/alvarobrandon/Experiments/memory_and_cores/11Apps20MinsTimeout_sep22/pickle/dfk.pickle')
dfm = read_pickle('/Users/alvarobrandon/Experiments/memory_and_cores/11appsgraphsmedium/pickle/dfm.pickle')



