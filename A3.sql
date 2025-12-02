set serveroutput on;
set echo off;
set feedback on;

start C:\cprg307\Assignment 3 Part 1 Scripts\create_wkis.sql;
start C:\cprg307\Assignment 3 Part 1 Scripts\constraints_wkis.sql;
start C:\cprg307\Assignment 3 Part 1 Scripts\load_wkis.sql;

DECLARE
    CURSOR new_transactions_cursor (v_transaction_no new_transactions.transaction_no%TYPE) IS
    SELECT account_no, transaction_no, transaction_type, transaction_amount, transaction_date, description
    FROM new_transactions
    WHERE transaction_type = v_transaction_type;

    v_account_no new_transactions.account_no%TYPE;
    v_transaction_no new_transactions.transaction_no%TYPE;
    v_transaction_type new_transactions.transaction_type%TYPE;
    v_transaction_amount new_transactions.transaction_amount%TYPE;
    v_transaction_date new_transactions.transaction_date%TYPE;
    v_description new_transactions.description%TYPE;
BEGIN
    OPEN new_transactions_cursor(v_transaction_no);
    LOOP
        FETCH new_transactions_cursor into v_account_no, v_transaction_no, v_transaction_type, v_transaction_amount, v_transaction_date, v_description;
        EXIT WHEN new_transactions_cursor%NOTFOUND;
        INSERT INTO transaction_detail(account_no, transaction_no, transaction_type, transaction_amount)
        VALUES (account_no, transaction_no, transaction_type, transaction_amount);
        INSERT INTO transaction_history(transaction_no, transaction_date, description)
        VALUES(v_transaction_no, v_transaction_date, v_description);
    END LOOP;
    CLOSE new_transactions_cursor;