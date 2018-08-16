import pandas as pd
import createdatafilefunctions as cdf


conn = cdf.connecttodb()

q = '''
SELECT loan_id, min(transaction_Date) as disbursement_date
FROM disbursements_information
group by 1
limit 10;
'''

df = cdf.createbasedataset(conn)

print(df.head())