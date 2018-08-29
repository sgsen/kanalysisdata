import sqlite3
import pandas as pd
import analysisfunctions as af

# Read in the analytical dataset
conn = sqlite3.connect('AnalysisDB_Aug_28_2018')
q = "select * from analysisdata where screening_date > '2017-02-01'"
df = pd.read_sql(q, conn)

# temp modification
#df.overduestatus = df.overduestatus.cat.add_categories(['GoodStanding'])
#df.fillna(value={'overduestatus':'GoodStanding'}, inplace=True)

# create dimentional analysis table
dimlist = ['business_type', 'banked', 'payment', 'returncustomer', 'collateral', 'overall_status', 'BusinFormalityOfTheBusinessui', 'ManagPsychometricScoreui_APP', 'product_code']
custdimtable = af.createcustdimtable(df, dimlist)

#custdimtable

# create the
combinedcattable = ((af.createcustdimtable(df, ['combinedcat'])).sort_values('how_many', ascending=False)).dropna()

# write the outputs to a csv file
custdimtable.to_csv('custdimension.csv')
combinedcattable.to_csv('combineddimensions.csv')