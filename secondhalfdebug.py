import pandas as pd
import createdatafilefunctions as cdf
import time
import sqlite3


st = pd.Timestamp.now()
print('Starting Analytical Dataset generation at: ', st)

# Read in the analytical dataset
conn = sqlite3.connect('AnalysisDB_Sep_03_2018.db')
q = "select * from analysisdata"

#import pdb; pdb.set_trace()

analytical_df = pd.read_sql(q, conn)
analytical_df.drop(columns=['account_number_y', 'sc_created_on', 'cu_created_on'], inplace=True)
analytical_df.rename(columns={'account_number_x':'account_number'}, inplace=True)


analytical_df = cdf.createDerivedColumns(analytical_df, st)

### write to sqlitedb
#for dname, dset in datasets.items():
#    cdf.writetodb(dset, dname, st)
#    time.sleep(3)

cdf.writetodb(analytical_df, "analysisdata", st)
