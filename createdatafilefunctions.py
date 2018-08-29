import json
import pymysql
import pandas as pd
import numpy as np


def connecttodb(startTime):
    print('Connecting to the data warehouse... ')
    f = open('sohamkinaraconfig.json', 'r')
    config = json.load(f)

    connection = pymysql.connect(host=config['host'],
                                 user=config['user'],
                                 password=config['password'],
                                 db=config['db4'])
    print('Connected to data warehouse in: ', pd.Timestamp.now() - startTime)
    return connection

def getLoanInformationData(conn, startTime):
    print('Fetching data from loan_information table... ')
    q = '''
SELECT
    L.loan_id,
    L.customer_id,
    L.account_number,
    L.screening_date,
    L.enterprise_id,
    L.status,
    L.sanction_date,
    L.product_type,
    L.product_code,
    L.product_name,
    L.interest_rate,
    L.customer_bank,
    L.overall_score,
    L.overall_status,
    L.hub_id,
    L.hub_name,
    L.spoke_id,
    L.spoke_name,
    L.reject_reason,
    E.business_type,
    E.business_activity
FROM
    loan_information L
        LEFT JOIN
    enterprise_information E ON L.enterprise_id = E.enterprise_id;
    '''
    df = pd.read_sql(q, con=conn)
    print('Completed loan_information fetch: ', pd.Timestamp.now() - startTime)
    return df


def getLoanSummaryInformationData(conn, startTime):
    print('Fetching data from loan_summary_information table... ')
    q = '''
    SELECT
        loan_id,
        cash_sales,
        invoice_sales,
        scrap_sales,
        total_business_revenue,
        net_business_income,
        net_total_income,
        average_bank_deposit,
        average_bank_balance,
        final_kinara_emi
    FROM
        loan_summary_information;
    '''
    df = pd.read_sql(q, con=conn)
    print('Completed loan_summary_information fetch: ', pd.Timestamp.now() - startTime)
    return df


def getScoresInformationData(conn, startTime):
    print('Fetching data from scores_information table... ')
    q = '''
    SELECT
        loan_id,
        BusinbusinessHistoryui,
        BusinBusinessPremisesStatusui,
        BusinBusinessVintageui,
        BusinChequesBouncedui,
        BusinCommercialCIBILui,
        BusinEMIBouncedui,
        BusinExistingCustomerui,
        BusinFormalityOfTheBusinessui,
        BusinProxyIndicatorScoreui,
        BusinReferenceCheckScoreui,
        BusinReferredByui,
        BusinYearsofBusinesspresenceinAreaui,
        ManagAgeui_APP,
        ManagCBscoreui_APP,
        ManagcustomerHouseStatusui_APP,
        ManagExperienceInBusinessui_APP,
        ManagInvolvementInBusinessui_APP,
        ManagMaritalStatusui_APP,
        ManagnoOfYearsOfResidenceinAreaui_APP,
        ManagPsychometricScoreui_APP,
        ManagQualificationui_APP,
        ManagAgeui_COAPP,
        ManagCBscoreui_COAPP,
        ManagcustomerHouseStatusui_COAPP,
        ManagExperienceInBusinessui_COAPP,
        ManagInvolvementInBusinessui_COAPP,
        ManagMaritalStatusui_COAPP,
        ManagnoOfYearsOfResidenceinAreaui_COAPP,
        ManagPsychometricScoreui_COAPP,
        ManagQualificationui_COAPP,
        `FinanABB:KinaraEMIui`,
        `FinanAvgBankDeposit:AvgRevenueui`,
        FinanChequesBouncedui,
        FinanDSCRui,
        FinanDSONonTradingui,
        FinanDSOTradingui,
        FinanEMIBouncedui,
        `FinanKinaraEMI:NetIncomeui`,
        FinannumberOfBouncesInKinaraLoanTrackui,
        LoanPCurrentRatioui,
        LoanPDSCRNewAssetIncomeui,
        LoanPElectricityAvailabiltyui,
        `LoanPHypothecationValue:LoanAmountui`,
        LoanPHypothecatedStatusui,
        LoanPloanProductTypeui,
        LoanPLTVNewAssetui,
        LoanPLTVUsedAssetui,
        LoanPSocialImpactui,
        LoanPSpaceAvailabilityui,
        `LoanPTurnover:LoanAmountui`
    FROM
        scores_information
    '''
    df = pd.read_sql(q, con=conn)
    print('Completed scores_information fetch: ', pd.Timestamp.now() - startTime)
    return df


def getDeviationCounts(conn, startTime):
    print('Fetching data on deviation and mitigation counts... ')
    q = '''
    SELECT
        loan_id,
        COUNT(parameter) as deviations,
        COUNT(mitigant) AS mitigants
    FROM
        financialForms.loan_mitigants
    GROUP BY 1;
    '''
    df = pd.read_sql(q, con=conn)
    print('Completed deviation and mitigation count fetch: ', pd.Timestamp.now() - startTime)
    return df


def getLeadsInformation(conn, startTime):
    print('Fetching data on leads... ')
    q = '''
     SELECT
        loan_id,
        loan_amount_requested,
        first_lead_interaction_date
     FROM
        leads_information
     WHERE
        loan_id IS NOT NULL;
     '''
    df = pd.read_sql(q, con=conn)

    print('Completed leads data fetch: ', pd.Timestamp.now() - startTime)
    return df

#questions about disbursements

#instead of loan amount?
#why is actual date pretty much always null?

def getDisbursementInformation(conn, startTime):
    print('Fetching data on disbursements... ')
    q = '''
    SELECT
        loan_id,
        total_disbursement_amount as loan_amount,
        t1_scheduled_disbursement_date as disbursement_date
    FROM disbursements_luc_information
     '''
    df = pd.read_sql(q, con=conn)
    print('Completed disbursements data fetch: ', pd.Timestamp.now() - startTime)
    return df


def getTATInformation(conn, startTime):
    print('Fetching data on TAT... ')
    q = '''
    SELECT
        loan_id, Screening_completed, LoanInitiation_completed
    FROM
        tatuser_information;
    '''

    df = pd.read_sql(q, con=conn)
    print('Completed TAT data fetch: ', pd.Timestamp.now() - startTime)
    return df


def getDeliquentDays(conn, startTime):
    print('Fetching Delinquents data... ')
    q = '''
    SELECT
        LI.loan_id, K.Delinquentdays as dayspastdue
    FROM
        loan_information LI
            LEFT JOIN
                (
                SELECT
                    L.AccountNumber, DelinquentDays
                FROM
                    loanoutstanding_overdue L
                INNER JOIN
                    (SELECT
                        AccountNumber, MAX(cbs_date) AS max_cbs_date
                    FROM
                        loanoutstanding_overdue
                    GROUP BY AccountNumber
                    ) AS M
                        ON L.cbs_date = M.max_cbs_date AND L.AccountNumber = M.AccountNumber
                ) AS K
                    ON LI.account_number = K.AccountNumber;

    '''
    df = pd.read_sql(q, con=conn)
    print('Completed Delinquent data fetch: ', pd.Timestamp.now() - startTime)
    return df


def getBounceData(conn, startTime):
    print('Fetching Bounce data... ')
    q = '''
    SELECT
        L.loan_id, B.bounces
    FROM
        loan_information L
            LEFT JOIN
                (SELECT
                    account_number, COUNT(*) AS bounces
                FROM
                    GL_ChequeBounceChargesReceivable
                WHERE
                    type_of_ledger = 'Dr'
                GROUP BY 1) AS B
                    ON L.account_number = B.account_number;
    '''
    df = pd.read_sql(q, con=conn)
    print('Completed Bounce data fetch: ', pd.Timestamp.now() - startTime)
    return df

def writetodb(df, dname, startTime):
    print('Writing dataset', dname, ' to file...')
    import sqlite3
    todStr = pd.to_datetime('today').strftime("%b_%d_%Y")
    dbfilename = "AnalysisDB_" + todStr + ".db"
    conn = sqlite3.connect(dbfilename)
    df.to_sql(name=dname, con=conn, if_exists='replace')
    print('Completed writing dataset', dname, 'to SQLite db', dbfilename, 'at:', pd.Timestamp.now() - startTime)
    return

def createCombinedCategoriesCols(df):
    # combinedcat
    df['combinedcat'] = df.business_type + '_' + df.banked + 'bank_' + df.payment + '_col_' + df.collateral

    #combinedcatgroup
    df['combinedcatgroup'] = df['combinedcat']

    def mapsupergroups(st):
        if st == 'Manufacturing_yesbank_cash_payments_col_yes' \
                or st == 'Manufacturing_yesbank_invoice_payments_col_yes':
            return 'GenYesLessRisk'
        elif st == 'Manufacturing_nobank_cash_payments_col_yes' \
                or st == 'Trading_yesbank_invoice_payments_col_no' \
                or st == 'Trading_yesbank_cash_payments_col_no':
            return 'GenYesRiskier'
        elif st == 'Manufacturing_yesbank_invoice_payments_col_no':
            return 'MostlyYesLessRisk'
        elif st == 'Manufacturing_yesbank_cash_payments_col_no':
            return 'MostlyYesRiskier'
        elif st == 'Manufacturing_nobank_cash_payments_col_no':
            return 'TypicallyNoLessRisk'
        elif st == 'Trading_nobank_cash_payments_col_no':
            return 'TypicallyNoRiskier'
        else:
            return 'Other'

    df.combinedcatgroup = df.combinedcatgroup.apply(mapsupergroups)
    return df

def createLoanAmountCatCol(df):
    df['loan_amt_cat'] = pd.cut(df.loan_amount, bins=[0, 2e5, 5e5, 10e5, 20e5, 10e6],
                                labels=['0to2L', '2to5L', '5to10L', 'TentoTwentyL', 'TwentyLorMore'],
                                include_lowest=True)
    return df

def createRiskStatusCols(df):
    # overdue status
    bins = [0, 30, 60, 90, 365, 2000]
    labels = ['Less30', 'Par30', 'Par60', 'Par90', 'Par1YR']
    df['overduestatus'] = pd.cut(df.dayspastdue, bins=bins, labels=labels)
    df.overduestatus = df.overduestatus.cat.add_categories(['GoodStanding'])
    df[df.loan_status == 'approved'].fillna(value={'overduestatus': 'GoodStanding'}, inplace=True)

    #default risk
    bins = [0, 30, 2000]
    labels = ['Less30', 'DefaultRisk']
    df['defaultrisk'] = pd.cut(df.dayspastdue, bins=bins, labels=labels, include_lowest=True)

    return df

def cleanLoanStatusCol(df):
    def clean_loanStatus(inptstr):
        if (inptstr == 'REJECTED') | (inptstr == 'HOLD'):
            return inptstr.lower()
        else:
            return 'approved'

    df['loan_status'] = df.status.apply(clean_loanStatus)
    return df

def cleanBusinessTypeCol(df):
    def clean_biztype(inpstr):
        if inpstr == 'manufacture':
            return 'Manufacturing'
        elif inpstr == 'service':
            return 'Serives'
        elif inpstr == 'trade':
            return 'Trading'
        elif inpstr == 'other':
            return 'Other'
        else:
            return inpstr

    df['business_type'] = df.business_type.apply(clean_biztype)
    return df

def createTATdates(df):
    # tat_effort
    df.LoanInitiation_completed = pd.to_datetime(df.LoanInitiation_completed)
    df.Screening_completed = pd.to_datetime(df.Screening_completed)
    df['tat_effort'] = (df.LoanInitiation_completed - df.Screening_completed) / np.timedelta64(1, 'D')

    # tat_elapsed
    df.disbursement_date = pd.to_datetime(df.disbursement_date)
    df.screening_date = pd.to_datetime(df.screening_date)
    df['tat_elapsed'] = (df.disbursement_date - df.screening_date) / np.timedelta64(1, 'D')

    # tat_elapsed_sanction
    df.sanction_date = pd.to_datetime(df.sanction_date)
    df['tat_elapsed_sanction'] = (df.sanction_date - df.screening_date) / np.timedelta64(1, 'D')

    return df


def createBankedCol(df):
    def bankedornot(rw):
        try:
            r = rw.total_business_revenue / rw.average_bank_deposit
        except:
            return 'no'

        if r >= .5:
            return 'yes'
        else:
            return 'no'

    df['banked'] = df.apply(bankedornot, axis=1)
    return df


def createPaymentModeCol(df):
    def cashorinvoice(rw):
        invoiceratio = 0

        try:
            invoiceratio = rw.invoice_sales / rw.total_business_revenue
        except:
            pass

        if invoiceratio >= .5:
            return "invoice_payments"
        else:
            return 'cash_payments'

    df['payment'] = df.apply(cashorinvoice, axis=1)
    return df

def createExistingCustomerCol(df):
    def returningcustomer(rw):
        firstloan = df[(df.loan_status == 'approved') & (df.customer_id == rw.customer_id)]['sanction_date'].min()
        try:
            rc = firstloan < rw.screening_date
        except:
            return 'no'

        if rc:
            return 'yes'
        else:
            return 'no'

    df['returncustomer'] = df.apply(returningcustomer, axis=1)
    return df

def createCollateralCol(df):
    def hascollateral(pc):
        pc = str(pc)
        pc = pc[-1]
        if pc == "S":
            return "yes"
        elif pc == "U":
            return "no"
        else:
            return None

    df['collateral'] = df.product_code.apply(hascollateral)
    return df

def cleanRejectReason(df):
    reasons = df.reject_reason.copy()

    p = 'c[a-z]b[a-z]l|cb[a-z]l|c[a-z]bl|ci?[a-z]i?l'+'|^cb|c[\s]b'+'|bureau|hig?h?mark'
    reasons[reasons.str.contains(p, case=False, na=False)] = 'CreditBureau'

    p = 'repayment|cheque|overdue|over\sdue|dp[dt]|transaction|indebted|track|bounce[d]?|d[e]?fault|banki?n?g?'
    reasons[reasons.str.contains(p, case=False, na=False)] = 'PastRepaymentIssues'

    p = r"c[ou]stomer(\sis)?\snot[\s]+ready|c[ou]stomer[\s]+not[\s]+available|c[ou]stomer\snot\sresponding|no[t]?\sint.*[ea]st|not[\s]+respon[ds][e]?|qu?o?tation|c[ou]stomer.*policy"
    reasons[reasons.str.contains(p, case=False, na=False)] = 'CustomerNotInterested'

    p = r"business?"
    p_exc = r"vintage|area|address|new|place"
    reasons[reasons.str.contains(p, case=False, na=False)&(~reasons.str.contains(p_exc, case=False, na=False))]='BusinessPerformanceIssue'

    p = r"hiding|fake|fraud"
    reasons[reasons.str.contains(p, case=False, na=False)]='CustomerFraud'

    p = r"not\savailable|.*l[a]?bl[e]?|inv[a]?l[i?]d"
    pex = r"g[au][au]?ra[uan]+t[oe]?r|machine"
    reasons[reasons.str.contains(p, case=False, na=False)&(~reasons.str.contains(pex, case=False, na=False))]='DocsUnavailableInvalid'

    p = r"g[au][au]?ra[uan]+t[oe]?r"
    reasons[reasons.str.contains(p, case=False, na=False)] = 'GuarantorNotAvailable'

    p = r"v[i]?nt[a]?ge"
    reasons[reasons.str.contains(p, case=False, na=False)] = 'BusinessVintage'

    p = r"r[e]?f[e]?r[e]?n[cs][e]?"
    reasons[reasons.str.contains(p, case=False, na=False)] = 'ReferenceCheck'

    p = r"\br[a]?t[e]?"
    pex = r"machine"
    reasons[(reasons.str.contains(p, case=False, na=False))&(~reasons.str.contains(pex, case=False, na=False))]='InterestRate'

    p = r"am[ou]?[ou]?nt"
    reasons[reasons.str.contains(p, case=False, na=False)]='LoanAmount'

    p = r"en?t?r?y"
    reasons[reasons.str.contains(p, case=False, na=False)]='DoubleEntry'

    p = r"pro?file|ar[e]?a"
    pex = r"Guarantor"
    reasons[reasons.str.contains(p, case=False, na=False) & ~reasons.str.contains(pex, case=False, na=False)]='BusProfileArea'

    p = r"days|slow"
    reasons[reasons.str.contains(p, case=False, na=False)]='TATTooLong'

    p = ['CreditBureau', 'PastRepayment', 'CustomerNotInterested',\
     'BusinessPerformance', 'TATTooLong', 'CustomerFraud',\
     'DocsUnavailableInvalid', 'InterestRate', 'GuarantorNotAvailable',\
    'Doubleentry', 'LoanAmount', 'BusProfileArea', 'BusinessVintage']
    reasons = ['Other' if x not in p else x for x in reasons]
    reasons = pd.Series(reasons)
    reasons.replace('Other', np.nan, inplace=True)

    df['rejectreasoncat'] = reasons

    return df

def createDerivedColumns(df, startTime):
    print('Cleaning dataset and generating derived features... ')
    # cleans up the loan status and business type columns
    df = cleanLoanStatusCol(df)

    df = cleanBusinessTypeCol(df)

    # tat columns
    df = createTATdates(df)

    # banked column
    # does most of business revenue make it into the bank? (50%+)
    df = createBankedCol(df)

    # payment
    # cash or invoice
    df = createPaymentModeCol(df)

    # returning customer - very slow
    df = createExistingCustomerCol(df)

    # has collateralized product
    df = createCollateralCol(df)

    # loan_amount_cat col
    # cuts loan amount into bins
    df = createLoanAmountCatCol(df)

    # overduestatus and defaultrisk cols
    # cuts days past due into Par30, Par60, etc
    # cuts days past due into less than 30 days (Less30) or more (DefaultRisk)
    df = createRiskStatusCols(df)

    # combinedcat and combinedcatgroup cols
    ## this generates the combined categores like Manufacturing_yesbank_invoice_payments_col_yes
    ## it also maps these combined categories to super groups like GenYesLessRisk
    df = createCombinedCategoriesCols(df)

    # clean reject reasons
    import warnings
    warnings.filterwarnings("ignore", 'This pattern has match groups')
    df = cleanRejectReason(df)

    print('Completed dataset cleaning and feature generation:', pd.Timestamp.now() - startTime)

    return df
