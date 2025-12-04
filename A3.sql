set serveroutput on;
set echo off;
set feedback on;


start "C:\cprg307\Assignment 3 Part 2 Scripts\create_wkis.sql";
start "C:\cprg307\Assignment 3 Part 2 Scripts\constraints_wkis.sql";
start "C:\cprg307\Assignment 3 Part 2 Scripts\load_wkis.sql";
start "C:\cprg307\Assignment 3 Part 2 Scripts\A3_test dataset_2 - Clean and Erroneous.sql";

DECLARE
    CURSOR new_transactions_cursor IS
    SELECT account_no, transaction_no, transaction_type, transaction_amount, transaction_date, description
    FROM new_transactions
    FOR UPDATE;

    v_account_no new_transactions.account_no%TYPE;
    v_transaction_no new_transactions.transaction_no%TYPE;
    v_transaction_type new_transactions.transaction_type%TYPE;
    v_transaction_amount new_transactions.transaction_amount%TYPE;
    v_transaction_date new_transactions.transaction_date%TYPE;
    v_description new_transactions.description%TYPE;
    v_old_transaction_no new_transactions.transaction_no%TYPE;
    v_account_balance account.account_balance%TYPE;
    v_new_account_amount account.account_balance%TYPE;

    v_test_transaction_no new_transactions.transaction_no%TYPE;
    v_test_continue BOOLEAN;
    v_test_errorcode NUMBER;

    e_missing_transaction_data  EXCEPTION;
BEGIN
    OPEN new_transactions_cursor;
    -- Percaution to not skip over details
    v_old_transaction_no := -1;
    v_test_continue := TRUE;
    LOOP
        FETCH new_transactions_cursor into v_account_no, v_transaction_no, v_transaction_type, v_transaction_amount, v_transaction_date, v_description;
        EXIT WHEN new_transactions_cursor%NOTFOUND;
        
        -- ALLOWING CONTINUED WORK
        IF (NOT v_test_continue) THEN
            IF (v_transaction_no IS NOT NULL) THEN
                DBMS_OUTPUT.PUT_LINE('Does it continue??');
                v_test_continue := TRUE;
            END IF;
        END IF;

        -- ERROR CHECKING
        IF (v_transaction_no IS NULL) THEN
            DBMS_OUTPUT.PUT_LINE('Missing Transaction Number');
            v_test_continue := FALSE;
            
        END IF;

        IF v_test_continue THEN
            IF (v_old_transaction_no != v_transaction_no) THEN
                INSERT INTO transaction_history(transaction_no, transaction_date, description)
                VALUES(v_transaction_no, v_transaction_date, v_description);
            END IF;
            INSERT INTO transaction_detail(account_no, transaction_no, transaction_type, transaction_amount)
            VALUES (v_account_no, v_transaction_no, v_transaction_type, v_transaction_amount);
            
            -- Grabs account balance
            SELECT account_balance INTO v_account_balance FROM ACCOUNT
            WHERE account_no = v_account_no;
            -- Decides whether to add or subtract values from account
            IF (v_transaction_type = 'D') THEN
                v_new_account_amount := v_account_balance + v_transaction_amount;
            ELSIF (v_transaction_type = 'C') THEN
                v_new_account_amount := v_account_balance - v_transaction_amount;
            ELSE
                DBMS_OUTPUT.PUT_LINE('????');
                RAISE_APPLICATION_ERROR(-20001, 'Invalid Transaction Type');
            END IF;
            -- Updates the accounts
            UPDATE ACCOUNT
            SET account_balance = v_new_account_amount
            WHERE account_no = v_account_no;
            v_old_transaction_no := v_transaction_no;
            -- Deletes the item after being processed
            DELETE FROM NEW_TRANSACTIONS WHERE CURRENT OF new_transactions_cursor;
        END IF;
    END LOOP;
    CLOSE new_transactions_cursor;
    COMMIT;

END;
/
