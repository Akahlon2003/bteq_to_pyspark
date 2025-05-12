-- Log in to the Teradata server
.LOGON your_teradata_server/username,password;

-- Set the default database
DATABASE $DATABASE;

-- Optional: Set session parameters if needed
.SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
.SET SESSION CHARACTER SET 'UTF8';

-- Create MULTISET VOLATILE tables for intermediate steps with WHERE clauses
CREATE MULTISET VOLATILE TABLE volatile_customer_info AS (
    SELECT customer_id, customer_name, customer_address, customer_phone, customer_email
    FROM customer_info
    WHERE customer_status = 'Active' -- Example condition
) WITH DATA
ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE volatile_bank_savings_account AS (
    SELECT account_id, customer_id, account_balance, account_open_date, account_status
    FROM bank_savings_account
    WHERE account_type = 'Savings' -- Example condition
) WITH DATA
ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE volatile_credit_card_transactions AS (
    SELECT transaction_id, customer_id, card_number, transaction_date, transaction_amount, merchant_name, transaction_status
    FROM credit_card_transactions
    WHERE transaction_type = 'Purchase' -- Example condition
) WITH DATA
ON COMMIT PRESERVE ROWS;

CREATE MULTISET VOLATILE TABLE volatile_payment_due AS (
    SELECT due_id, customer_id, due_date, due_amount, payment_status
    FROM payment_due
    WHERE payment_type = 'Credit Card' -- Example condition
) WITH DATA
ON COMMIT PRESERVE ROWS;

-- Insert consolidated data into exception_records table using UNION
INSERT INTO exception_records (exception_id, due_id, customer_id, exception_date, exception_reason, exception_status, exception_update_date, exception_update_remarks)
SELECT
    ROW_NUMBER() OVER (ORDER BY customer_id) AS exception_id,
    NULL AS due_id,
    customer_id,
    CURRENT_DATE AS exception_date,
    'Customer Info Exception' AS exception_reason,
    'Open' AS exception_status,
    CURRENT_DATE AS exception_update_date,
    'Initial exception record' AS exception_update_remarks
FROM volatile_customer_info
UNION
SELECT
    ROW_NUMBER() OVER (ORDER BY customer_id) AS exception_id,
    NULL AS due_id,
    customer_id,
    CURRENT_DATE AS exception_date,
    'Bank Savings Account Exception' AS exception_reason,
    'Open' AS exception_status,
    CURRENT_DATE AS exception_update_date,
    'Initial exception record' AS exception_update_remarks
FROM volatile_bank_savings_account
UNION
SELECT
    ROW_NUMBER() OVER (ORDER BY customer_id) AS exception_id,
    NULL AS due_id,
    customer_id,
    CURRENT_DATE AS exception_date,
    'Credit Card Transaction Exception' AS exception_reason,
    'Open' AS exception_status,
    CURRENT_DATE AS exception_update_date,
    'Initial exception record' AS exception_update_remarks
FROM volatile_credit_card_transactions
UNION
SELECT
    ROW_NUMBER() OVER (ORDER BY customer_id) AS exception_id,
    due_id,
    customer_id,
    CURRENT_DATE AS exception_date,
    'Payment Due Exception' AS exception_reason,
    'Open' AS exception_status,
    CURRENT_DATE AS exception_update_date,
    'Initial exception record' AS exception_update_remarks
FROM volatile_payment_due;

-- Move records from exception_records to exception_records_hist daily
INSERT INTO exception_records_hist
SELECT * FROM exception_records
WHERE exception_date < CURRENT_DATE;

DELETE FROM exception_records
WHERE exception_date < CURRENT_DATE;

-- Update exception_records based on payment_records
UPDATE exception_records
SET exception_status = CASE
    WHEN p.payment_status = 'Paid' THEN 'Closed'
    ELSE 'Open'
END,
exception_update_date = CURRENT_DATE,
exception_update_remarks = CASE
    WHEN p.payment_status = 'Paid' THEN 'Payment received and exception closed'
    ELSE 'Exception remains open'
END
FROM exception_records e
JOIN payment_records p ON e.due_id = p.due_id
WHERE e.exception_status = 'Open';

-- Drop the VOLATILE tables
DROP TABLE volatile_customer_info;
DROP TABLE volatile_bank_savings_account;
DROP TABLE volatile_credit_card_transactions;
DROP TABLE volatile_payment_due;

.LOGOFF;
.QUIT;
