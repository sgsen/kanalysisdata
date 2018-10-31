show tables;

describe loan_summary_information;
describe loan_information;

describe scores_information;

select loan_id, kinara_emi, final_kinara_emi from loan_summary_information where kinara_emi is null limit 100;

describe financialForms.loan_mitigants;

describe leads_information;

describe disbursements_information;


describe enterprise_information;

describe individual_information;

describe disbursements_luc_information;


select count(loan_id) 
from disbursements_luc_information;

select count(DISTINCT loan_id)
from loan_information;

select count(DISTINCT loan_id)
from disbursements_luc_information;

select count(loan_id)
from disbursements_luc_information;


select
	loan_id,
    number_of_disbursements,
    loan_amount,
    total_disbursement_amount, 
    total_disbursed_amount,
    current_stage, 
    t1_disbursement_amount,
    t1_scheduled_disbursement_date,
    t1_actual_disbursement_date,
    t2_disbursement_amount,
    t3_disbursement_amount,
    t4_disbursement_amount
from disbursements_luc_information
where loan_id > 10000 and number_of_disbursements > 1 and current_stage is NULL
limit 100;

 select loan_id, nominee_state
 from loan_information
 where loan_id>15000
 limit 50;
 
 show tables;
 
 describe loanoutstanding_overdue;
 
 select count(*)
 from loanoutstanding_overdue;
 #5762035 rows
 
 select count(DISTINCT loan_id)
 from loanoutstanding_overdue;
 #22017
 # assuming this means that the other loans have never bounced
 
 select loan_id, count(*)
 from loanoutstanding_overdue
 GROUP BY 1;
 
 describe loanoutstanding_overdue;
 
 describe tatuser_information;
 
 describe repayments_information;
 
 DESCRIBE GL_ChequeBounceChargesReceivable;
 
SELECT 
    account_number, COUNT(*) AS bounces
FROM
    GL_ChequeBounceChargesReceivable
WHERE
    type_of_ledger = 'Dr'
GROUP BY 1;

describe financialForms.loan_collateral_details;

select
	loan_id, 
    collateral_type, 
    collateral_value,
    collateral_description, 
    collateral_category
from 
	financialForms.loan_collateral_details
where loan_id = 23303;
    
describe financialForms.machine_details;
DESCRIBE financialForms.enterprise_assets;
desc financialForms.stock_details;

select * from financialForms.machine_details limit 50;
select * from financialForms.enterprise_assets limit 50;
select * from financialForms.stock_details limit 50;

select 
	L.loan_id,
	count(type) as machine_count
from 
	loan_information L 
		left join financialForms.machine_details M
			on L.loan_id = M.loan_monitoring_id
limit 100;
        
        
describe scores_information;

select loan_id, `LoanPHypothecationValue:LoanAmountui`, LoanPHypothecatedStatusui, BalanHypothecatedStatusui
from scores_information
where (loan_id > 18000) and 
	(`LoanPHypothecationValue:LoanAmountui` is not null OR LoanPHypothecatedStatusui IS NOT NULL OR BalanHypothecatedStatusui IS NOT NULL)
limit 100;

describe loan_information;

describe enterprise_information;

select business_type, business_activity, business_sector, business_sub_type, subsector_mostly_involved_with, sector_mostly_involved_with
from enterprise_information
limit 50;

select business_sector, count(*)
from enterprise_information
group by 1;

select business_sub_type, count(*)
from enterprise_information
group by 1;

describe leads_information;