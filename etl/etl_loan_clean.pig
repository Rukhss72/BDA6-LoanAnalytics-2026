SET mapreduce.input.linerecordreader.line.maxlength 5000000;

raw = LOAD '/loan_project/raw/loan.csv' USING PigStorage(',') AS (
  id:chararray,
  member_id:chararray,
  loan_amnt:double,
  term:chararray,
  int_rate:double,
  installment:double,
  grade:chararray,
  sub_grade:chararray,
  emp_title:chararray,
  emp_length:chararray,
  home_ownership:chararray,
  annual_inc:double,
  verification_status:chararray,
  issue_d:chararray,
  loan_status:chararray,
  pymnt_plan:chararray,
  url:chararray,
  desc:chararray,
  purpose:chararray,
  title:chararray,
  addr_state:chararray,
  dti:double,
  delinq_2yrs:int,
  earliest_cr_line:chararray,
  inq_last_6mths:int,
  mths_since_last_delinq:int,
  mths_since_last_record:int,
  open_acc:int,
  pub_rec:int,
  revol_bal:double,
  revol_util:double,
  total_acc:int
);

-- remove header row
d1 = FILTER raw BY id != 'id';

-- cleaning + selection (drops unwanted columns by not selecting them)
clean = FOREACH d1 GENERATE
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
  (verification_status IS NOT NULL AND TRIM(verification_status) == 'Source Verified'
     ? 'Verified'
     : verification_status) AS verification_status,
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
  (mths_since_last_delinq IS NULL ? 0 : mths_since_last_delinq) AS mths_since_last_delinq,
  (mths_since_last_record IS NULL ? 0 : mths_since_last_record) AS mths_since_last_record;

STORE clean INTO '/loan_project/curated/loan_clean' USING PigStorage(',');

g = GROUP clean BY loan_status;
kpi = FOREACH g GENERATE group AS loan_status, COUNT(clean) AS total_loans;
STORE kpi INTO '/loan_project/curated/kpi_loan_status_counts' USING PigStorage(',');
