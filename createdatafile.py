import pandas as pd
import createdatafilefunctions as cdf
import time
#from importlib import reload
import pdb

### Initiate
time.sleep(1)

st = pd.Timestamp.now()
print('Starting Analytical Dataset generation at: ', st)

datasets = {}

#get connection

conn = cdf.connecttodb(st)

### Collect data from datawarehouses

#variables from loan_information table
datasets["loan_info"] = cdf.getLoanInformationData(conn, st)

# variables from loan_summary_information
datasets["loan_summary_info"] = cdf.getLoanSummaryInformationData(conn, st)

# variables from scores_information
datasets["scores_info"] = cdf.getScoresInformationData(conn, st)

# variables from finacialForms.loan_mitigation
datasets["deviation_info"] = cdf.getDeviationCounts(conn, st)

# leads information
datasets["leads_info"] = cdf.getLeadsInformation(conn, st)

# disbursement variables
datasets["disbursement_info"] = cdf.getDisbursementInformation(conn, st)

# TAT information
datasets["tat_info"] = cdf.getTATInformation(conn, st)

# DelinquentDays Information
datasets["overdue_info"] = cdf.getDeliquentDays(conn, st)

# Bounce Data
datasets["bounce_info"] = cdf.getBounceData(conn, st)

### join the data sets together

analytical_df = datasets['loan_info']
del datasets['loan_info']


for dname, dset in datasets.items():
    #pdb.set_trace()
    analytical_df = pd.merge(analytical_df, dset, how='left', on='loan_id')

time.sleep(1)

###
cdf.writetodb(analytical_df, "analysisdata", st)

### derived columns
analytical_df = cdf.createDerivedColumns(analytical_df, st)

### write to sqlitedb
#for dname, dset in datasets.items():
#    cdf.writetodb(dset, dname, st)
#    time.sleep(3)

cdf.writetodb(analytical_df, "analysisdata", st)
