import mongodfbuilder
from pandas import set_option
import preprocessingutils as pre



if __name__ == '__main__':
    mr = mongodfbuilder.MongoDFBuilder()
    dfg = mr.build_readings_dataframe()
    dfm = mr.build_task_attempts_dataframe()
    dfj = mr.build_jobs_dataframe()
    dfs = mr.build_stage_attempts_dataframe()
    dfe = mr.build_stage_executors_dataframe()
    dfenv = mr.build_environment_dataframe()
    dfapps = mr.build_apps_dataframe()
    dfapps = dfapps.merge(dfenv,on='appId').sort('start')
    dfs = dfs.merge(dfenv,on='appId').sort('start')