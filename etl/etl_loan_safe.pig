SET mapreduce.input.linerecordreader.line.maxlength 5000000;
SET mapred.linerecordreader.maxlength 50000000;

-- Load as raw lines (avoids CSV parsing failures)
lines = LOAD '/loan_project/raw_sanitized/loan.csv' USING TextLoader() AS (line:chararray);

-- Remove header and empty lines
data = FILTER lines BY line IS NOT NULL AND TRIM(line) != '' AND NOT STARTSWITH(line, 'id,');

-- Split CSV by comma (simple split; good enough for KPI fields that don't contain commas)
f = FOREACH data GENERATE
  FLATTEN(STRSPLIT(line, ',', 40)) AS (
    c0:chararray,  -- id (we will drop)
    c1:chararray,  -- member_id (drop)
    loan_amnt:chararray,        -- c2
    funded_amnt:chararray,      -- c3
    funded_amnt_inv:chararray,  -- c4
    term:chararray,             -- c5
    int_rate:chararray,         -- c6
    installment:chararray,      -- c7
    grade:chararray,            -- c8
    sub_grade:chararray,        -- c9
    emp_title:chararray,        -- c10
    emp_length:chararray,       -- c11
    home_ownership:chararray,   -- c12
    annual_inc:chararray,       -- c13
    verification_status:chararray, -- c14
    issue_d:chararray,          -- c15
    loan_status:chararray,      -- c16
    pymnt_plan:chararray,       -- c17 (drop)
    url:chararray,              -- c18 (drop)
    desc:chararray,             -- c19 (drop)
    purpose:chararray,          -- c20
    title:chararray,            -- c21
    zip_code:chararray,         -- c22
    addr_state:chararray,       -- c23
    dti:chararray,              -- c24
    delinq_2yrs:chararray,      -- c25
    earliest_cr_line:chararray, -- c26
    inq_last_6mths:chararray,   -- c27
    mths_since_last_delinq:chararray, -- c28
    mths_since_last_record:chararray, -- c29
    open_acc:chararray,         -- c30
    pub_rec:chararray,          -- c31
    revol_bal:chararray,        -- c32
    revol_util:chararray,       -- c33
    total_acc:chararray         -- c34
  );

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
STORE norm INTO '/loan_project/curated/loan_clean' USING PigStorage(',');

-- KPI: count by loan_status
g = GROUP norm BY loan_status;
kpi = FOREACH g GENERATE group AS loan_status, COUNT(norm) AS total_loans;
STORE kpi INTO '/loan_project/curated/kpi_loan_status_counts' USING PigStorage(',');
