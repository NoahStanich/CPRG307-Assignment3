SET SERVEROUTPUT ON;

DECLARE
    -- Transaction type constants (required)
    c_debit  CONSTANT CHAR(1) := 'D';
    c_credit CONSTANT CHAR(1) := 'C';

    -- Cursor: list of transaction numbers (including NULL)
    CURSOR cur_trans_list IS
        SELECT DISTINCT transaction_no
        FROM   new_transactions
        ORDER  BY transaction_no;

    -- Cursor: all rows for one transaction number
    CURSOR cur_trans_rows (p_tno new_transactions.transaction_no%TYPE) IS
        SELECT transaction_no, transaction_date, description,
               account_no, transaction_type, transaction_amount
        FROM   new_transactions
        WHERE  transaction_no = p_tno
        ORDER  BY account_no, transaction_amount;

    -- Cursor: rows where transaction_no is NULL
    CURSOR cur_null_rows IS
        SELECT transaction_no, transaction_date, description,
               account_no, transaction_type, transaction_amount
        FROM   new_transactions
        WHERE  transaction_no IS NULL;

    -- Working variables
    v_tno       new_transactions.transaction_no%TYPE;
    v_tdate     new_transactions.transaction_date%TYPE;
    v_tdesc     new_transactions.description%TYPE;

    v_has_error BOOLEAN := FALSE;
    v_err_msg   VARCHAR2(2000);

    v_debit_sum   NUMBER := 0;
    v_credit_sum  NUMBER := 0;

    v_acct_count  NUMBER;

    v_acct_balance      account.account_balance%TYPE;
    v_new_acct_balance  account.account_balance%TYPE;
    v_default_type      account_type.default_trans_type%TYPE;

    v_unexp_msg VARCHAR2(2000);

BEGIN
    -- Loop through each transaction number
    FOR trans_header IN cur_trans_list LOOP
        -- Reset values
        v_tno        := trans_header.transaction_no;
        v_tdate      := NULL;
        v_tdesc      := NULL;
        v_has_error  := FALSE;
        v_err_msg    := NULL;
        v_debit_sum  := 0;
        v_credit_sum := 0;

        -- Error 1: NULL transaction number
        IF v_tno IS NULL THEN
            FOR r_null IN cur_null_rows LOOP
                v_tdate := r_null.transaction_date;
                v_tdesc := r_null.description;
                EXIT;
            END LOOP;

            v_err_msg := 'Missing (NULL) transaction number.';

            INSERT INTO wkis_error_log
                (transaction_no, transaction_date, description, error_msg)
            VALUES (NULL, v_tdate, v_tdesc, v_err_msg);

            CONTINUE;
        END IF;

        -- Validate each row of the transaction
        FOR trans_row IN cur_trans_rows(v_tno) LOOP

            -- Capture header info
            IF v_tdate IS NULL THEN
                v_tdate := trans_row.transaction_date;
                v_tdesc := trans_row.description;
            END IF;

            -- Error 3: invalid account number
            SELECT COUNT(*)
            INTO   v_acct_count
            FROM   account
            WHERE  account_no = trans_row.account_no;

            IF v_acct_count = 0 THEN
                v_err_msg := 'Invalid account number ' || trans_row.account_no ||
                             ' for transaction ' || v_tno || '.';
                v_has_error := TRUE;
            END IF;

            -- Error 4: negative amount
            IF NOT v_has_error AND trans_row.transaction_amount < 0 THEN
                v_err_msg := 'Negative transaction amount ' || trans_row.transaction_amount ||
                             ' for account ' || trans_row.account_no ||
                             ' in transaction ' || v_tno || '.';
                v_has_error := TRUE;
            END IF;

            -- Error 5: invalid transaction type
            IF NOT v_has_error AND
               trans_row.transaction_type NOT IN (c_debit, c_credit) THEN
                v_err_msg := 'Invalid transaction type ''' ||
                              trans_row.transaction_type ||
                             ''' for transaction ' || v_tno || '.';
                v_has_error := TRUE;
            END IF;

            -- Accumulate debit/credit totals
            IF NOT v_has_error THEN
                IF trans_row.transaction_type = c_debit THEN
                    v_debit_sum := v_debit_sum + trans_row.transaction_amount;
                ELSE
                    v_credit_sum := v_credit_sum + trans_row.transaction_amount;
                END IF;
            END IF;

            EXIT WHEN v_has_error;
        END LOOP;

        -- Error 2: debits not equal credits
        IF NOT v_has_error AND v_debit_sum <> v_credit_sum THEN
            v_err_msg := 'Debits and credits are not equal for transaction ' ||
                          v_tno || ' (Debits = ' || v_debit_sum ||
                          ', Credits = ' || v_credit_sum || ').';
            v_has_error := TRUE;
        END IF;

        -- If error found → log and skip processing
        IF v_has_error THEN
            INSERT INTO wkis_error_log
                (transaction_no, transaction_date, description, error_msg)
            VALUES (v_tno, v_tdate, v_tdesc, v_err_msg);

            CONTINUE;
        END IF;

        -- No errors → insert into history
        INSERT INTO transaction_history
            (transaction_no, transaction_date, description)
        VALUES (v_tno, v_tdate, v_tdesc);

        -- Process detail lines and update accounts
        FOR trans_row IN cur_trans_rows(v_tno) LOOP
            INSERT INTO transaction_detail
                (account_no, transaction_no, transaction_type, transaction_amount)
            VALUES
                (trans_row.account_no,
                 trans_row.transaction_no,
                 trans_row.transaction_type,
                 trans_row.transaction_amount);

            -- Fetch account balance and default type
            SELECT a.account_balance, t.default_trans_type
            INTO   v_acct_balance, v_default_type
            FROM   account a
                   JOIN account_type t
                     ON a.account_type_code = t.account_type_code
            WHERE  a.account_no = trans_row.account_no
            FOR UPDATE;

            -- Apply accounting rules
            IF v_default_type = c_debit THEN
                IF trans_row.transaction_type = c_debit THEN
                    v_new_acct_balance := v_acct_balance + trans_row.transaction_amount;
                ELSE
                    v_new_acct_balance := v_acct_balance - trans_row.transaction_amount;
                END IF;
            ELSE
                IF trans_row.transaction_type = c_credit THEN
                    v_new_acct_balance := v_acct_balance + trans_row.transaction_amount;
                ELSE
                    v_new_acct_balance := v_acct_balance - trans_row.transaction_amount;
                END IF;
            END IF;

            UPDATE account
            SET    account_balance = v_new_acct_balance
            WHERE  account_no = trans_row.account_no;
        END LOOP;

        -- Delete processed rows
        DELETE FROM new_transactions
        WHERE  transaction_no = v_tno;

    END LOOP;

    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        v_unexp_msg := 'Unanticipated error: ' || SQLERRM;

        INSERT INTO wkis_error_log
            (transaction_no, transaction_date, description, error_msg)
        VALUES (v_tno, v_tdate, v_tdesc, v_unexp_msg);

        ROLLBACK;
END;
/
