REGISTER /home/rukhsaar/pig/contrib/piggybank/java/piggybank.jar;

raw = LOAD '/loan_project/raw_sanitized/loan.csv'
USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'YES_MULTILINE', 'NOCHANGE')
AS (
  id:chararray,
  member_id:chararray,
  loan_amnt:chararray,
  funded_amnt:chararray,
  funded_amnt_inv:chararray,
  term:chararray,
  int_rate:chararray,
  installment:chararray,
  grade:chararray,
  sub_grade:chararray,
  emp_title:chararray,
  emp_length:chararray,
  home_ownership:chararray,
  annual_inc:chararray,
  verification_status:chararray,
  issue_d:chararray,
  loan_status:chararray,
  pymnt_plan:chararray,
  url:chararray,
  desc:chararray,
  purpose:chararray,
  title:chararray,
  zip_code:chararray,
  addr_state:chararray,
  dti:chararray,
  delinq_2yrs:chararray,
  earliest_cr_line:chararray,
  inq_last_6mths:chararray,
  mths_since_last_delinq:chararray,
  mths_since_last_record:chararray,
  open_acc:chararray,
  pub_rec:chararray,
  revol_bal:chararray,
  revol_util:chararray,
  total_acc:chararray
);

-- remove header row if it appears
f = FILTER raw BY id IS NOT NULL AND TRIM(id) != 'id';


-- Normalize verification_status: Source Verified -> Verified
norm = FOREACH f GENERATE
  loan_amnt,
  term,
  int_rate,
  installment,
  grade,
  sub_grade,
  emp_title,
  emp_length,
  home_ownership,
  annual_inc,
  (TRIM(verification_status) == 'Source Verified' ? 'Verified' : verification_status) AS verification_status,
  issue_d,
  loan_status,
  purpose,
  title,
  addr_state,
  dti,
  delinq_2yrs,
  earliest_cr_line,
  inq_last_6mths,
  open_acc,
  pub_rec,
  revol_bal,
  revol_util,
  total_acc,
  -- months-since blanks -> 0
  ((mths_since_last_delinq IS NULL OR TRIM(mths_since_last_delinq) == '') ? '0' : mths_since_last_delinq) AS mths_since_last_delinq,
  ((mths_since_last_record IS NULL OR TRIM(mths_since_last_record) == '') ? '0' : mths_since_last_record) AS mths_since_last_record;

-- Store curated (as strings; good for serving/BI export)
STORE norm INTO '/loan_project/curated_debug/loan_clean' USING PigStorage(',');

-- KPI: count by loan_status
g = GROUP norm BY loan_status;
kpi = FOREACH g GENERATE group AS loan_status, COUNT(norm) AS total_loans;
STORE kpi INTO '/loan_project/curated_debug/kpi_loan_status_counts' USING PigStorage(',');
