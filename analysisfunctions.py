import sqlite3
import pymysql
import pandas as pd
import numpy as np


def testfunction():
    print('sohamtestfunction version 2')


def custdimanalysis(df, cx):
    # cols 1 and 2
    x = df[cx].value_counts().to_frame(name='how_many')
    y = (df[cx].value_counts(normalize=1) * 100).to_frame(name='how_many_pct')
    combined = pd.concat([x, y], axis=1)

    # col 3
    x = df.groupby('loan_status')[cx].value_counts() / df.groupby([cx]).loan_id.count() * 100
    x = x['approved'].to_frame(name='approved_pct')
    combined = pd.concat([combined, x], axis=1)

    # col 4
    x = df.groupby(['loan_status', 'overduestatus']) \
        [cx].value_counts()['approved', 'GoodStanding']
    y = df[df.loan_status == 'approved'][cx].value_counts()
    z = 100 * (x / y).to_frame(name='goodstanding_pct')
    combined = pd.concat([combined, z], axis=1)

    #### Col 5: Pct Ever Bounced
    x = df.groupby(['loan_status', 'bounces'])[cx].value_counts()['approved'][0]
    y = df[df.loan_status == 'approved'][cx].value_counts()
    z = (100 - 100 * (x / y)).to_frame(name='everbounced_pct')
    combined = pd.concat([combined, z], axis=1)

    #### Col 6: Default Risk
    x = df.groupby(['loan_status', 'defaultrisk'])[cx].value_counts()['approved', 'DefaultRisk']
    y = df[df.loan_status == 'approved'][cx].value_counts()
    z = 100 * (x / y).to_frame(name='defaultrisk_pct')
    combined = pd.concat([combined, z], axis=1)

    #### Col 7: Elapsed TAT
    x = df[df.tat_elapsed > 0].groupby(cx).tat_elapsed.mean()
    combined = pd.concat([combined, x], axis=1)

    #### Col 8: Elapsed TAT to Sanction
    x = df[df.tat_elapsed_sanction > 0].groupby(cx).tat_elapsed_sanction.mean()
    combined = pd.concat([combined, x], axis=1)

    #### Col 9: TAT Effort
    x = df[df.tat_effort > 0].groupby(cx).tat_effort.mean()
    combined = pd.concat([combined, x], axis=1)

    #### Col 10, 11: Send Backs and Deviations
    #sendback data not easily availble, deviations only for now
    #x = df.groupby(cx).agg({"sendback_count": "mean", "deviations": "mean"})
    x = df.groupby(cx).agg({"deviations": "mean"})
    combined = pd.concat([combined, x], axis=1)

    return combined


def createcustdimtable(df, dimlist):
    combinedtable = pd.DataFrame()

    for dim in dimlist:
        da = custdimanalysis(df, dim)
        combinedtable = pd.concat([combinedtable, da], axis=0)

    return combinedtable
