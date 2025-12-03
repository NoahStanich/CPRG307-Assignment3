set serveroutput on;
set echo off;
set feedback on;


start "C:\cprg307\Assignment 3 Part 1 Scripts\create_wkis.sql";
start "C:\cprg307\Assignment 3 Part 1 Scripts\constraints_wkis.sql";
start "C:\cprg307\Assignment 3 Part 1 Scripts\load_wkis.sql";
start "C:\cprg307\Assignment 3 Part 1 Scripts\A3_test_dataset_1_-_Clean[1].sql";

DECLARE
    CURSOR new_transactions_cursor IS
    SELECT account_no, transaction_no, transaction_type, transaction_amount, transaction_date, description
    FROM new_transactions;

    v_account_no new_transactions.account_no%TYPE;
    v_transaction_no new_transactions.transaction_no%TYPE;
    v_transaction_type new_transactions.transaction_type%TYPE;
    v_transaction_amount new_transactions.transaction_amount%TYPE;
    v_transaction_date new_transactions.transaction_date%TYPE;
    v_description new_transactions.description%TYPE;
    v_old_transaction_no new_transactions.transaction_no%TYPE;
BEGIN
    OPEN new_transactions_cursor;
    DBMS_OUTPUT.PUT_LINE('test');
    v_old_transaction_no := -1;
    LOOP
        FETCH new_transactions_cursor into v_account_no, v_transaction_no, v_transaction_type, v_transaction_amount, v_transaction_date, v_description;
        EXIT WHEN new_transactions_cursor%NOTFOUND;
        DBMS_OUTPUT.PUT_LINE('test2');
        DBMS_OUTPUT.PUT_LINE(v_transaction_no);
        IF (v_old_transaction_no != v_transaction_no) THEN
            INSERT INTO transaction_history(transaction_no, transaction_date, description)
            VALUES(v_transaction_no, v_transaction_date, v_description);
        END IF;
        INSERT INTO transaction_detail(account_no, transaction_no, transaction_type, transaction_amount)
        VALUES (v_account_no, v_transaction_no, v_transaction_type, v_transaction_amount);
        
        -- Updates the accounts
        UPDATE ACCOUNT
        SET account_balance = v_transaction_amount
        WHERE account_no = v_account_no;
        v_old_transaction_no := v_transaction_no;
        
    END LOOP;
    CLOSE new_transactions_cursor;
END;
/
