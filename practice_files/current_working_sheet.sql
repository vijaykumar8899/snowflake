create database scdp;
create schema raw;


 create or replace table t1(c1 number);
select c1, from t1;

show tables ;

select * from information_schema.tables where created is not null;

select * from table(result_scan('01ba576e-3201-6f92-0000-000bef0630c1'))
where "rows" >= 3;


select concat(current_organization_name(),'/',current_account());

app.snowflake.com/NLPYAJY/HV97788/snowflakecomputing/console/login;


select * from table();
show tables;
SELECT * FROM TABLE(GET_DDL('table', 't1'));


CREATE TABLE EMP
       (EMPNO NUMBER(4) NOT NULL,
        ENAME VARCHAR2(10),
        JOB VARCHAR2(9),
        MGR NUMBER(4),
        HIREDATE DATE,
        SAL NUMBER(7, 2),
        COMM NUMBER(7, 2),
        DEPTNO NUMBER(2));
 
INSERT INTO EMP VALUES
        (7369, 'SMITH',  'CLERK',     7902,
        TO_DATE('17-DEC-1980', 'DD-MON-YYYY'),  800, NULL, 20);
INSERT INTO EMP VALUES
        (7499, 'ALLEN',  'SALESMAN',  7698,
        TO_DATE('20-FEB-1981', 'DD-MON-YYYY'), 1600,  300, 30);
INSERT INTO EMP VALUES
        (7521, 'WARD',   'SALESMAN',  7698,
        TO_DATE('22-FEB-1981', 'DD-MON-YYYY'), 1250,  500, 30);
INSERT INTO EMP VALUES
        (7566, 'JONES',  'MANAGER',   7839,
        TO_DATE('2-APR-1981', 'DD-MON-YYYY'),  2975, NULL, 20);
INSERT INTO EMP VALUES
        (7654, 'MARTIN', 'SALESMAN',  7698,
        TO_DATE('28-SEP-1981', 'DD-MON-YYYY'), 1250, 1400, 30);
INSERT INTO EMP VALUES
        (7698, 'BLAKE',  'MANAGER',   7839,
        TO_DATE('1-MAY-1981', 'DD-MON-YYYY'),  2850, NULL, 30);
INSERT INTO EMP VALUES
        (7782, 'CLARK',  'MANAGER',   7839,
        TO_DATE('9-JUN-1981', 'DD-MON-YYYY'),  2450, NULL, 10);
INSERT INTO EMP VALUES
        (7788, 'SCOTT',  'ANALYST',   7566,
        TO_DATE('09-DEC-1982', 'DD-MON-YYYY'), 3000, NULL, 20);
INSERT INTO EMP VALUES
        (7839, 'KING',   'PRESIDENT', NULL,
        TO_DATE('17-NOV-1981', 'DD-MON-YYYY'), 5000, NULL, 10);
INSERT INTO EMP VALUES
        (7844, 'TURNER', 'SALESMAN',  7698,
        TO_DATE('8-SEP-1981', 'DD-MON-YYYY'),  1500,    0, 30);
INSERT INTO EMP VALUES
        (7876, 'ADAMS',  'CLERK',     7788,
        TO_DATE('12-JAN-1983', 'DD-MON-YYYY'), 1100, NULL, 20);
INSERT INTO EMP VALUES
        (7900, 'JAMES',  'CLERK',     7698,
        TO_DATE('3-DEC-1981', 'DD-MON-YYYY'),   950, NULL, 30);
INSERT INTO EMP VALUES
        (7902, 'FORD',   'ANALYST',   7566,
        TO_DATE('3-DEC-1981', 'DD-MON-YYYY'),  3000, NULL, 20);
INSERT INTO EMP VALUES
        (7934, 'MILLER', 'CLERK',     7782,
        TO_DATE('23-JAN-1982', 'DD-MON-YYYY'), 1300, NULL, 10);
 
CREATE TABLE DEPT
       (DEPTNO NUMBER(2),
        DNAME VARCHAR2(14),
        LOC VARCHAR2(13) );
 
INSERT INTO DEPT VALUES (10, 'ACCOUNTING', 'NEW YORK');
INSERT INTO DEPT VALUES (20, 'RESEARCH',   'DALLAS');
INSERT INTO DEPT VALUES (30, 'SALES',      'CHICAGO');
INSERT INTO DEPT VALUES (40, 'OPERATIONS', 'BOSTON');

select * from emp;

create table emp_clone clone emp;

create database clone_scdp clone scdp;
drop database clone_scdp;

undrop database clone_scdp;

select * from emp_clone;


select * from information_schema.tables where created is not null;

alter table emp set RETENTION_TIME  = 90;
alter table emp set retention_time_in_days = 10; --upto 10
ALTER TABLE emp SET DATA_RETENTION_TIME_IN_DAYS = 90;


show parameters like 'timezone';
alter session set timezone='America/Chicago';--2025-02-14 08:00:51.777 -0600
alter session set timezone='Asia/Kolkata';   --2025-02-14 19:31:46.105 +0530

select current_timestamp(); 


select current_account();



create storage integration <storage_integration_name>
    type = external_stage
    storage_provider = s3
    storage_aws_role_arn = '<iam_role_arn>'
    enabled = true
    storage_allowed_locations = ( 's3://<location1>', 's3://<location2>' );





SELECT value FROM TABLE(SPLIT_TO_TABLE('a,b,c', ','));

    
SELECT value FROM TABLE(FLATTEN(input => PARSE_JSON('[1, 2, 3]')));
SELECT * FROM TABLE(ALERT_HISTORY('2025-01-01', '2025-01-31'));


----giving one procedure's result set to another procedure's input
--let's create 1st procedure to generate the result set
select * from emp;

CREATE OR REPLACE PROCEDURE pr_emp_result(p_empno number)
RETURNS TABLE(empno NUMBER, ename STRING, sal NUMBER, deptno NUMBER)
LANGUAGE SQL
EXECUTE AS OWNER 
AS
$$
DECLARE
    v_result resultset;
    v_sql varchar;
BEGIN
    v_sql := 'SELECT EMPNO, ENAME, SAL, DEPTNO FROM EMP WHERE deptno = ' || :p_empno ;
    v_result := (EXECUTE IMMEDIATE :v_sql);
 
 RETURN TABLE (v_result);
END;
$$;


CREATE OR REPLACE PROCEDURE pr_new_emp(p_input_table TABLE(empno NUMBER, ename STRING, sal NUMBER, deptno NUMBER))
RETURNS TABLE(empno NUMBER, ename STRING, sal NUMBER, deptno NUMBER)
LANGUAGE sql
EXECUTE AS OWNER
as
$$
DECLARE
    v_result resultset;
    v_sql varchar;
BEGIN
    v_sql := 'select * from p_input_table where sal > 2000';
    v_result := (EXECUTE IMMEDIATE :v_sql);
    
RETURN TABLE(v_result);
END;
$$;

CREATE OR REPLACE PROCEDURE pr_new_emp(p_table_data variant)
RETURNS TABLE(empno NUMBER, ename STRING, sal NUMBER, deptno NUMBER)
LANGUAGE sql
EXECUTE AS OWNER
as
$$
DECLARE
    v_result resultset;
    v_sql varchar;
BEGIN
    v_sql := 'select * from p_input_table where sal > 2000';
    v_result := (EXECUTE IMMEDIATE :v_sql);
    
RETURN TABLE(v_result);
END;
$$;

call pr_emp_result(10);

  
CREATE OR REPLACE PROCEDURE pr_emp_result(p_empno number)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER 
AS
$$
DECLARE
    v_resultset resultset;
    v_sql varchar;
    v_result VARIANT;
BEGIN
    v_sql := 'SELECT EMPNO, ENAME, SAL, DEPTNO FROM EMP WHERE deptno = ' || :p_empno ;
   -- Execute the SQL statement and store the result
    v_resultset = (EXECUTE IMMEDIATE :v_sql);

    -- Aggregate the resultset into a VARIANT format (array of objects)
     v_result = (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) FROM TABLE(v_resultset));

    -- Return the aggregated result
    RETURN v_result;
 
 RETURN v_result;
END;
$$;

CREATE OR REPLACE PROCEDURE pr_emp_result(p_empno NUMBER)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_resultset RESULTSET;
    v_sql STRING;
    v_result VARIANT;
BEGIN
    -- Construct the dynamic SQL
    v_sql := 'SELECT EMPNO, ENAME, SAL, DEPTNO FROM EMP WHERE EMPNO = ' || p_empno;

    -- Execute the SQL statement and store the result
    LET v_resultset = (EXECUTE IMMEDIATE :v_sql);

    -- Aggregate the resultset into a VARIANT format (array of objects)
    LET v_result = (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) FROM TABLE(v_resultset));

    -- Return the aggregated result
    RETURN v_result;
END;
$$;

CREATE OR REPLACE PROCEDURE pr_emp_result(p_empno NUMBER)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_sql STRING;
    v_result VARIANT;
BEGIN
    -- Construct the dynamic SQL
    v_sql := 'SELECT EMPNO, ENAME, SAL, DEPTNO FROM EMP WHERE EMPNO = ' || p_empno;

    -- Execute the SQL statement and return the result as VARIANT
    -- Using EXECUTE IMMEDIATE with RETURN QUERY to directly return the result set as VARIANT
    RETURN (EXECUTE IMMEDIATE :v_sql);
END;
$$;

CREATE OR REPLACE PROCEDURE pr_emp_result(p_empno NUMBER)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_sql STRING;
    v_result VARIANT;
BEGIN
    -- Construct the dynamic SQL to get the employee data
    v_sql := 'SELECT EMPNO, ENAME, SAL, DEPTNO FROM EMP WHERE EMPNO = ' || p_empno;

    -- Use EXECUTE IMMEDIATE to fetch the results into a resultset
    FOR result IN EXECUTE IMMEDIATE :v_sql DO
        -- Convert each row to VARIANT and return as a single value
        LET v_result = OBJECT_CONSTRUCT('EMPNO', result.EMPNO, 'ENAME', result.ENAME, 'SAL', result.SAL, 'DEPTNO', result.DEPTNO);
    END FOR;

    -- Return the result
    RETURN v_result;
END;
$$;

CREATE OR REPLACE PROCEDURE pr_emp_result(p_empno NUMBER)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_sql STRING;
    v_result VARIANT;
BEGIN
    -- Construct the dynamic SQL to get the employee data
    v_sql := 'SELECT EMPNO, ENAME, SAL, DEPTNO FROM EMP WHERE EMPNO = ' || p_empno;

    -- Use EXECUTE IMMEDIATE to fetch results and aggregate them into VARIANT
    EXECUTE IMMEDIATE :v_sql INTO v_result;

    -- Return the result as a VARIANT
    RETURN v_result;
END;
$$;

CREATE OR REPLACE PROCEDURE pr_emp_result(p_empno NUMBER)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_sql STRING;
    v_result VARIANT;
    v_resultset RESULTSET;
BEGIN
    -- Construct the dynamic SQL to get the employee data
    v_sql := 'SELECT EMPNO, ENAME, SAL, DEPTNO FROM EMP WHERE EMPNO = ' || p_empno;

    -- Execute the SQL dynamically and fetch the resultset
    LET v_resultset = EXECUTE IMMEDIATE :v_sql;

    -- Convert the resultset to VARIANT
    LET v_result = (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) FROM TABLE(v_resultset));

    -- Return the result as a VARIANT
    RETURN v_result;
END;
$$;


CREATE OR REPLACE PROCEDURE pr_emp_result(p_empno NUMBER)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_sql STRING;
    v_result VARIANT;
BEGIN
    -- Construct the dynamic SQL to get the employee data
    v_sql := 'SELECT EMPNO, ENAME, SAL, DEPTNO FROM EMP WHERE EMPNO = ' || p_empno;

    -- Execute the SQL dynamically and return the result as VARIANT
    -- Using the OBJECT_CONSTRUCT to convert the result to VARIANT
    EXECUTE IMMEDIATE :v_sql INTO :v_result;

    -- Return the result as a VARIANT
    RETURN v_result;
END;
$$;

create or replace temporary table temp_emp(c1 variant); 

INSERT INTO temp_emp (c1)
SELECT OBJECT_CONSTRUCT(*)
FROM emp;

select * from temp_emp;


__________________
;
CREATE OR REPLACE PROCEDURE pr_new_emp_result(p_empno number)
RETURNS TABLE(empno NUMBER, ename STRING, sal NUMBER, deptno NUMBER)
LANGUAGE SQL
EXECUTE AS OWNER 
AS
$$
DECLARE
    v_result resultset;
    v_sql varchar;
BEGIN
    v_sql := 'SELECT EMPNO, ENAME, SAL, DEPTNO FROM EMP WHERE deptno = ' || :p_empno ;
    v_result := (EXECUTE IMMEDIATE :v_sql);
 
 RETURN TABLE (v_result);
END;
$$;

CREATE OR REPLACE PROCEDURE pr_emp_result(p_empno number)
RETURNS variant
LANGUAGE SQL
EXECUTE AS OWNER 
AS
$$
DECLARE
    v_result resultset;
    v_sql varchar;
    v_variant variant;
BEGIN
    v_sql := 'SELECT EMPNO, ENAME, SAL, DEPTNO FROM EMP WHERE deptno = ' || :p_empno ;
    v_result := (EXECUTE IMMEDIATE :v_sql);
 
 RETURN v_variant (select object_construct(*) from v_result);
END;
$$;

call pr_emp_result(10);

call pr_new_emp_result(pr_emp_result(10));


_________________________________________
;

CREATE OR REPLACE PROCEDURE pr_emp_name_result(p_ename varchar)
RETURNS TABLE(empno NUMBER, ename STRING, sal NUMBER, deptno NUMBER)
LANGUAGE SQL
EXECUTE AS OWNER 
AS
$$
DECLARE
    v_result resultset;
    v_sql varchar;
BEGIN
    v_sql := 'SELECT EMPNO, ENAME, SAL, DEPTNO FROM EMP WHERE ename LIKE ''%' || p_ename || '%'' ';
    v_result := (EXECUTE IMMEDIATE :v_sql);
 
 RETURN TABLE (v_result);
END;
$$;

CREATE OR REPLACE PROCEDURE pr_emp_result(p_empno number)
RETURNS varchar
LANGUAGE SQL
EXECUTE AS OWNER 
AS
$$
DECLARE
    v_ename varchar;
BEGIN
    SELECT ENAME into v_ename FROM EMP WHERE empno = :p_empno ;
   
 
 RETURN v_ename;
EXCEPTION
        when statement_Error then
            return object_Construct('sqlcode',sqlcode,'sqlerrm',sqlerrm,'sqlstate',sqlstate);

END;
$$;

call pr_emp_result(7369);
call pr_emp_name_result('SMITH');
'SELECT EMPNO, ENAME, SAL, DEPTNO FROM EMP WHERE ename LIKE ''%' || p_ename || '%'' ';
select * from emp WHERE deptno = 7369;
    SELECT ENAME FROM EMP WHERE deptno = 7369 ;

    DECLARE v_emp_variant VARIANT;

-- Call the first procedure and store the result in the variable
LET v_emp_variant = CALL pr_emp_result(7369);
;
-- Call the second procedure and pass the variable containing the VARIANT data
;
CALL pr_emp_name_result(v_emp_variant);

create or replace task call_two_procedures()
warehouse='compute_wh'
schedule = '2 minutes'
as
DECLARE v_emp_variant VARIANT
-- Call the first procedure and store the result in the variable
LET v_emp_variant = CALL pr_emp_result(7369)
-- Call the second procedure and pass the variable containing the VARIANT data
CALL pr_emp_name_result(v_emp_variant)
;



create storage integration dev_emp_s3_integration
type= external_stage
storage_provider=s3
storage_aws_role_arn='arn:aws:iam::361769583202:role/dev_emp_role'
enabled=true
storage_allowed_locations= ('s3://dev-emp-s3/');


desc storage integration dev_emp_s3_integration;
 
create stage my_csv_stage;
show stages;
SELECT * FROM emp NATURAL JOIN dept;

select * from emp where sal < (select max(sal) from emp) limit 1; --2073.21428571
select * from emp order by sal desc;

show tables;



select * from emp;