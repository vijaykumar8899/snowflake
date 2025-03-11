
--this procedures takes input of empno and returns name of employee
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

--this procedures takes input of name of employee and gives all details of that paticular employee;
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

--to call both procedures we are using another procedure 
CREATE OR REPLACE PROCEDURE pr_get_emp_details(p_empno NUMBER)
RETURNS TABLE(empno NUMBER, ename STRING, sal NUMBER, deptno NUMBER)
LANGUAGE SQL
EXECUTE AS OWNER 
AS
$$
DECLARE
    v_ename VARCHAR;
    v_result RESULTSET;
BEGIN
    -- Call the first procedure to get the employee name
    v_ename := 'CALL pr_emp_result(p_empno)';
    --v_ename := 'SMITH';

    EXECUTE IMMEDIATE v_ename;
    
    -- Call the second procedure to get employee details using the name
    v_result := 'CALL pr_emp_name_result(v_ename)';

    EXECUTE IMMEDIATE v_result;

    RETURN TABLE(v_result);
EXCEPTION
        when statement_Error then
            return object_Construct('sqlcode',sqlcode,'sqlerrm',sqlerrm,'sqlstate',sqlstate);
        when expression_Error then
            return object_Construct('sqlcode',sqlcode,'sqlerrm',sqlerrm,'sqlstate',sqlstate);
        when other then
            return object_Construct('sqlcode',sqlcode,'sqlerrm',sqlerrm,'sqlstate',sqlstate);



END;
$$;

CREATE OR REPLACE PROCEDURE pr_get_emp_details(p_empno NUMBER)
RETURNS TABLE(empno NUMBER, ename STRING, sal NUMBER, deptno NUMBER)
LANGUAGE SQL
EXECUTE AS OWNER 
AS
$$
DECLARE
    v_ename STRING;
BEGIN
    -- Call the first procedure to get the employee name
    CALL pr_emp_result(p_empno) INTO v_ename;
    
    -- Return the result of the second procedure as a table
    RETURN TABLE(
        SELECT empno, ename, sal, deptno
        FROM TABLE(pr_emp_name_result(v_ename))
    );
EXCEPTION
    WHEN statement_Error THEN
        RETURN object_Construct('sqlcode', sqlcode, 'sqlerrm', sqlerrm, 'sqlstate', sqlstate);
    WHEN expression_Error THEN
        RETURN object_Construct('sqlcode', sqlcode, 'sqlerrm', sqlerrm, 'sqlstate', sqlstate);
    WHEN others THEN
        RETURN object_Construct('sqlcode', sqlcode, 'sqlerrm', sqlerrm, 'sqlstate', sqlstate);
END;
$$;

call pr_emp_result(7369);
call pr_emp_name_result('SMITH');
call pr_get_emp_details(7369)
