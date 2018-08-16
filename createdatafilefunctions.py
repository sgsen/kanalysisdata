import json
import pymysql
import pandas as pd


def connecttodb():
    f = open('sohamkinaraconfig.json', 'r')
    config = json.load(f)

    connection = pymysql.connect(host=config['host'],
                                 user=config['user'],
                                 password=config['password'],
                                 db=config['db1'])
    return connection

def createbasedataset(conn):
    q = '''
    SELECT 
        L.loan_id,
        L.customer_id,
        L.account_number,
        L.screening_date,
        L.enterprise_id,
        B.business_type,
        L.status,
        D.first_interaction_date,
        L.sanction_date,
        T.Screening_completed,
        T.LoanInitiation_completed,
        L.product_code,
        L.customer_bank,
        L.average_bank_deposit,
        L.cash_sales,
        L.invoice_sales,
        L.total_business_revenue,
        L.overall_score,
        L.overall_status
    FROM 
        loan_information L 
            LEFT JOIN tatuser_information T
                ON L.loan_id = T.loan_id
            LEFT JOIN enterprise_information B
                ON L.enterprise_id = B.enterprise_id
            LEFT JOIN leads_information D
                ON L.loan_id = D.loan_id;
    '''

    df = pd.read_sql(q, con=conn)
    return df