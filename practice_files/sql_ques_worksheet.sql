

show stages;
desc stage my_csv_stage;

list @my_csv_stage;

select $1 from @my_csv_stage file_format = (format_name =>  my_csv_format);
select $1 from @my_csv_stage (file_format => my_csv_format);

create stage emp_s3_stage
url = 's3://dev-emp-s3/'
storage_integration = dev_emp_s3_integration
directory = (enable = true)
comment = 'creating stage on integration dev_emp_s3_integration';

select $1::varchar,$2::varchar from @emp_s3_stage (file_format => my_csv_format);

with infer as(
select * from table(infer_schema(LOCATION => '@my_csv_stage' , FILE_FORMAT => 'my_csv_format' , FILES => 'emp_new.csv.gz' ))
),
with dollor as (
select $1,$2,$3,$4,$5,$6,$7,$8,$9,$10 from @my_csv_stage limit 1
)
select concat(infer.EXPRESSION, " as ", dollor.);

WITH infer AS (
    SELECT * 
    FROM TABLE(infer_schema(LOCATION => '@my_csv_stage', FILE_FORMAT => 'my_csv_format', FILES => 'emp_new.csv.gz'))
),
dollor AS (
    SELECT $1 AS col1, $2 AS col2, $3 AS col3, $4 AS col4, $5 AS col5, $6 AS col6, $7 AS col7, $8 AS col8, $9 AS col9, $10 AS col10
    FROM @my_csv_stage 
    LIMIT 1
)
SELECT 
    CONCAT(infer.EXPRESSION, ' as ', dollor.col1, ', ') AS concatenated_result
FROM infer, dollor;

CREATE OR REPLACE PROCEDURE pr_dynamic_concat()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
var infer_query = `SELECT EXPRESSION FROM TABLE(infer_schema(LOCATION => '@my_csv_stage', FILE_FORMAT => 'my_csv_format', FILES => 'emp_new.csv.gz'))`;
var dollor_query = `SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10 FROM @my_csv_stage LIMIT 1`;

var infer_result = snowflake.execute({sqlText: infer_query});
var dollor_result = snowflake.execute({sqlText: dollor_query});

var infer_expression = '';
if (infer_result.next()) {
    infer_expression = infer_result.getColumnValue(1);
}

var concatenated_result = '';
while (dollor_result.next()) {
    for (var i = 1; i <= 10; i++) {
        var col_value = dollor_result.getColumnValue(i);
        concatenated_result += infer_expression + ' as ' + col_value + ', ';
    }
}

// Remove the trailing comma and space
concatenated_result = concatenated_result.slice(0, -2);

return JSON.stringify(concatenated_result);
$$;

call pr_dynamic_concat();

list @my_csv_stage/emp_new.csv.gz;
select $1 from @my_csv_stage/emp_new.csv.gz;

create or replace table emp_data as
select 
$1::NUMBER(4, 0) as EMPNO, $1::NUMBER(4, 0) as ENAME, $1::NUMBER(4, 0) as JOB, $1::NUMBER(4, 0) as MGR, $1::NUMBER(4, 0) as HIREDATE, $1::NUMBER(4, 0) as SAL, $1::NUMBER(4, 0) as COMM, $1::NUMBER(4, 0) as DEPTNO, $1::NUMBER(4, 0) as MOBILE, $1::NUMBER(4, 0) as STATUS
from @my_csv_stage (file_format  => my_csv_format);

alter stage my_csv_stage set file_format = my_csv_format;

copy into emp_data from ( 
SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10 FROM @my_csv_stage
);



select * from information_schema.stages where STAGE_NAME LIKE '%MY_CSV_STAGE%'  ;
desc stage my_csv_stage ;

select DATEADD(day, -7, CURRENT_TIMESTAMP);
select dateadd(day, current_timestamp(), -10);
select dateadd(day, current_timestamp(), 2025-02-12 07:08:50.271 -0800);

CREATE TABLE Table1 (
    col1 NUMBER
);

CREATE TABLE Table2 (
    col1 NUMBER
);

-- Insert data into Table1
INSERT INTO Table1 (col1) VALUES (1);
INSERT INTO Table1 (col1) VALUES (1);
INSERT INTO Table1 (col1) VALUES (1);
INSERT INTO Table1 (col1) VALUES (2);
INSERT INTO Table1 (col1) VALUES (2);
INSERT INTO Table1 (col1) VALUES (3);
INSERT INTO Table1 (col1) VALUES (5);

-- Insert data into Table2
INSERT INTO Table2 (col1) VALUES (1);
INSERT INTO Table2 (col1) VALUES (1);
INSERT INTO Table2 (col1) VALUES (2);
INSERT INTO Table2 (col1) VALUES (3);
INSERT INTO Table2 (col1) VALUES (4);
INSERT INTO Table2 (col1) VALUES (NULL);

select * from table2;

select t1.col1,t2.col1 from table1 t1 inner join table2 t2 on t1.col1=t2.col1 ; --9
select t1.col1,t2.col1from table1 t1 left join table2 t2 on t1.col1=t2.col1 ;      --10
select t1.col1,t2.col1 from table1 t1 right join table2 t2 on t1.col1=t2.col1 ;      --11
select t1.col1,t2.col1 from table1 t1 full outer join table2 t2 on t1.col1=t2.col1 ; --12
select t1.col1,t2.col1 from table1 t1 cross join table2 t2  ; --12


_______________________________________________
;
CREATE or replace TABLE Table1 (
    col1 NUMBER
);

CREATE or replace TABLE Table2 (
    col1 NUMBER
);

-- Insert data into Table1
INSERT INTO Table1 (col1) VALUES (1);
INSERT INTO Table1 (col1) VALUES (1);
INSERT INTO Table1 (col1) VALUES (2);
INSERT INTO Table1 (col1) VALUES (null);
INSERT INTO Table1 (col1) VALUES (null);
-- Insert data into Table2
INSERT INTO Table2 (col1) VALUES (1);
INSERT INTO Table2 (col1) VALUES (1);
INSERT INTO Table2 (col1) VALUES (1);
INSERT INTO Table2 (col1) VALUES (2);
INSERT INTO Table2 (col1) VALUES (3);
INSERT INTO Table2 (col1) VALUES (NULL);

select * from table2;

select t1.col1,t2.col1 from table1 t1 inner join table2 t2 on t1.col1=t2.col1 ; --7
select t1.col1,t2.col1 from table1 t1 left join table2 t2 on t1.col1=t2.col1 ;      --9
select t1.col1,t2.col1 from table1 t1 right join table2 t2 on t1.col1=t2.col1 ;      --9
select t1.col1,t2.col1 from table1 t1 full outer join table2 t2 on t1.col1=t2.col1 ; --11
select t1.col1,t2.col1 from table1 t1 cross join table2 t2  ; --30

;
____________________________________________________________
;
CREATE or replace TABLE Table1 (
    col1 NUMBER
);

CREATE or replace TABLE Table2 (
    col1 NUMBER
);

-- Insert data into Table1
INSERT INTO Table1 (col1) VALUES (1);
INSERT INTO Table1 (col1) VALUES (2);
INSERT INTO Table1 (col1) VALUES (3);
INSERT INTO Table1 (col1) VALUES (4);
INSERT INTO Table1 (col1) VALUES (null);
-- Insert data into Table2
INSERT INTO Table2 (col1) VALUES (2);
INSERT INTO Table2 (col1) VALUES (3);
INSERT INTO Table2 (col1) VALUES (3);
INSERT INTO Table2 (col1) VALUES (5);
INSERT INTO Table2 (col1) VALUES (NULL);

select * from table2;

select t1.col1,t2.col1 from table1 t1 inner join table2 t2 on t1.col1=t2.col1 ; --7
select t1.col1,t2.col1 from table1 t1 left join table2 t2 on t1.col1=t2.col1 ;      --9
select t1.col1,t2.col1 from table1 t1 right join table2 t2 on t1.col1=t2.col1 ;      --9
select t1.col1,t2.col1 from table1 t1 full outer join table2 t2 on t1.col1=t2.col1 ; --11
select t1.col1,t2.col1 from table1 t1 cross join table2 t2  ; --30



______________________________________________________________
;
select count(summ) from( 
select t1.col1,t2.col1,count(*) as summ from table1 t1 cross join table2 t2
group by t1.col1,t2.col1
);
SELECT COUNT(summ) 
FROM ( 
    SELECT t1.col1, t2.col1, COUNT(*) AS summ 
    FROM table1 t1 
    CROSS JOIN table2 t2 
    GROUP BY t1.col1, t2.col1
);
insert into dept values(50,'java','HYDERABAD');

select * from dept where not exists ( select 1 from emp where emp.deptno=dept.deptno );

select * from dept where dept.deptno not exists (select emp.deptno from emp where emp.deptno=dept.deptno);

--find employee name and his manager name
select * from emp;
select e1.ename,e2.ename 
from emp e1
left join emp e2
on e1.mgr = e2.empno
order by e1.empno;
--find employee with sal greater than his manager 

select * from emp e1 where e1.sal > ( 
    select sal from emp e2 where e1.mgr=e2.empno 
);

select e1.empno,e1.ename from emp e1
left join emp e2 
on e1.mgr = e2.empno
where e1.sal > e2.sal
;

--department that have no employee; 
select * from dept where not exists ( select deptno from emp where emp.deptno = dept.deptno);

--cumulative sal of employee 
select empno,sal, sum(sal) over(partition by empno order by empno) as cumulative_sal 
from emp;
group by empno;


employee
a       id
john  100
max   200
david 300
 
sales
emp_id      month   amt
100        jan      150
100        feb      300
200        jan      100
200        feb      150
200        mar      50
300        jan      150
300        feb      250
300        mar      650
;



	select e.a, s.month, sum(s.amt) over(partition by s.emp_id order by 
            case
                when s.month = 'jan' then 1
                when s.month = 'feb' then 2
            end
            ) as cumulative_sal
	from employee e 
	left join sales s 
	on e.id = s.emp_id;

select * from employee;
select * from sales;
create or replace table employee(a varchar, id number);
insert into employee values('John',100),('Max',200),('David',300);

create or replace table sales (emp_id number, month varchar, amt number);
insert into sales values(100,'jan',150),(100,'feb',300),(200,'jan',100),(200,'feb',150),(200,'mar',50),(300,'jan',150),(300,'feb',250),(300,'mar',650);

empno=7369 ename= smith; mgr=7902 mgrName ford;

select max(sal) from emp where sal < (select max(sal) from emp) ; --3000.00

select * from emp order by sal desc limit 1; --5000

select * from ( 
select *,row_number() over(order by sal desc) as rnk from emp 
) where rnk = 2; ---- one records

select * from ( 
select *,dense_rank() over(order by sal desc) as rnk from emp 
) where rnk = 2; -- two records

select * from ( 
select *,rank() over(order by sal desc) as rnk from emp 
) where rnk = 2; ---- two records

SELECT sal  
FROM emp  
QUALIFY ROW_NUMBER() OVER (ORDER BY sal DESC) = 2;

--qualify
select * from( 
select sal,qualify row_number() over(order by sal desc)  rnk from emp
) where rnk = 2;

select sal,qualify row_number() over(order by sal desc) = 2 from emp;

select * , row_number() over (order by sal desc) as rnk from emp
qualify rnk = 2;



--employees who joned 6months back
select * from emp where hiredate <= dateadd(day, -30*6 , current_date());
select dateadd(day, -30*3 , current_date());

--max sal in each department 
SELECT empno, ename, sal, deptno 
FROM (
    SELECT empno, ename, sal, deptno, 
           RANK() OVER (PARTITION BY deptno ORDER BY sal DESC) AS rnk
    FROM emp
) WHERE rnk = 1;

--Find the Total Salary Paid for Each Department
SELECT empno, ename, sal, deptno, 
           SUM(SAL) OVER (ORDER BY sal DESC) AS total_sal
    FROM emp;

--Find Employees With Consecutive Joining Dates
SELECT empno, ename, hiredate, 
       LAG(hiredate) OVER (ORDER BY hiredate) AS prev_hiredate,
       LEAD(hiredate) OVER (ORDER BY hiredate) AS next_hiredate
FROM emp;

-- Find the Manager with the Most Employees
SELECT mgr, COUNT(*) AS num_employees 
FROM emp
GROUP BY mgr
ORDER BY num_employees DESC
LIMIT 1;

--Find employees who earn more than the average salary for their department but less than the highest salary in their department.
SELECT e.empno, e.ename, e.sal, e.deptno
FROM emp e
JOIN (
    SELECT deptno, AVG(sal) AS avg_sal, MAX(sal) AS max_sal
    FROM emp
    GROUP BY deptno
) dept_stats
ON e.deptno = dept_stats.deptno
WHERE e.sal > dept_stats.avg_sal
AND e.sal < dept_stats.max_sal;


SELECT empno, COUNT(*) AS num_employees 
FROM emp
GROUP BY empno
ORDER BY num_employees DESC
;

--Retrieve Employees Who Have the Same Job as ‘SCOTT’
select * from emp where job = ( select job from emp where ename = 'SCOTT');

--Find the First and Last Employee Hired in Each Department
SELECT empno, ename, hiredate, deptno 
FROM (
    SELECT empno, ename, hiredate, deptno, 
           ROW_NUMBER() OVER (PARTITION BY deptno ORDER BY hiredate ASC) AS first_emp,
           ROW_NUMBER() OVER (PARTITION BY deptno ORDER BY hiredate DESC) AS last_emp
    FROM emp
) WHERE first_emp = 1 OR last_emp = 1;

-- Find the Percentage Contribution of Each Employee's Salary to Their Department’s Total Salary
SELECT empno, ename, sal, deptno, 
       sal * 100 / SUM(sal) OVER (PARTITION BY deptno) AS percentage_contribution
FROM emp;

--Find Employees Who Have Worked the Most Years
select ename,round(months_between(current_date(),hiredate)/12,1) as Experience 
from emp 
order by Experience desc;

--Find Employees Who Were Hired in the Same Year as Their Manager
SELECT e1.empno, e1.ename, e1.hiredate, e1.mgr 
FROM emp e1
JOIN emp e2 ON e1.mgr = e2.empno
WHERE YEAR(e1.hiredate) = YEAR(e2.hiredate);


select * from table(information_Schema.copy_history(table_name => 'T_PRODUCT', start_time
=> dateadd('hours', -1, current_timestamp())));

select * from table(information_schema.query_history()); 


select * from information_schema.table where created is not null;
select * from information_schema.table where table_name = 'DIM_TABLE';


select $1,$2,$3,$4,$5,$6,$7,$8,$9,$10 from @my_csv_stage limit 1;
select * from table(infer_schema(LOCATION => '@my_csv_stage' , FILE_FORMAT => 'my_csv_format' , FILES => 'emp_new.csv.gz' ));

CREATE OR REPLACE TABLE emp_data AS
SELECT
    $1::NUMBER(4, 0) AS EMPNO,
    $2::TEXT AS ENAME,
    $3::TEXT AS JOB,
    $4::NUMBER(4, 0) AS MGR,
    $5::DATE AS HIREDATE,
    $6::NUMBER(10, 2) AS SAL, -- Adjusted the precision for SAL
    $7::NUMBER(10, 2) AS COMM, -- Assuming COMM is a numeric field
    $8::NUMBER(2, 0) AS DEPTNO,
    $9::NUMBER(10, 0) AS MOBILE,
    $10::BOOLEAN AS STATUS
FROM @MY_CSV_STAGE
(FILE_FORMAT => my_csv_format);

CREATE or replace TABLE emp_data
       (EMPNO NUMBER(4) NOT NULL,
        ENAME VARCHAR2(10),
        JOB VARCHAR2(9),
        MGR NUMBER(4),
        HIREDATE DATE,
        SAL NUMBER(7, 2),
        COMM NUMBER(7, 2),
        DEPTNO NUMBER(2),
        MOBILE NUMBER(10, 0),
        STATUS BOOLEAN
        );

copy into emp_data from @my_csv_stage;

select * from emp_data;
truncate table emp_data;

select * from table(information_Schema.copy_history(table_name => 'EMP_DATA', start_time
=> dateadd('hours', -1, current_timestamp())));

show stages;

create pipe emp_csv_internal_pipe
auto_ingest = false
as
copy into emp_data from @my_csv_stage;

create or replace pipe emp_csv_s3_pipe
auto_ingest = true
as
copy into emp_data from @EMP_S3_STAGE;

DESC PIPE emp_csv_s3_pipe;

SHOW PIPES;

select parse_json(system$pipe_status('emp_csv_s3_pipe'));

select * from table(validate_pipe_load('emp_csv_s3_pipe',
start_time=>dateadd('hours',-1,current_timestamp())));

alter pipe emp_csv_s3_pipe refresh;

select * from emp_data;

select * from information_schema.table_storage_metrics
 where table_name ='EMP' and table_dropped is null;


CREATE OR REPLACE TABLE employee_data (id INT,name STRING,details VARIANT );
INSERT INTO employee_data VALUES(1, 'John', '[{"skill": "SQL", "experience": 5}, {"skill": "Python", "experience": 3}]'),
(2, 'Alice', '[{"skill": "Java", "experience": 4}, {"skill": "Scala", "experience": 2}]');

CREATE OR REPLACE TABLE t_sample(id number,c1 VARIANT);

INSERT INTO t_sample(id,c1 ) select 1,PARSE_JSON('{
    "name": "John",
    "age": 25,
    "city": "Sampleville",
    "married": false,
    "hobbies": ["reading", "traveling", "programming"],
    "address": {
      "street": "123 Main Street",
      "city": "Sample City",
      "postal_code": "12345"
    }
  }');


  

  select * from t_sample;
  
create stage vijay_stage;


show stages;
show file formats;

create file format json_format
type=json;
select $1 from @vijay_stage (file_format => json_format);
copy into t_sample from @vijay_stage file_format = (format_name = json_format);

alter stage vijay_stage set file_format=json_format;

SELECT c1:name::String AS name
FROM t_sample;

select s.value from @vijay_stage t,
lateral flatten(t.$1) s;






CREATE OR REPLACE TABLE t_sample(c1 VARIANT);

INSERT INTO t_sample(c1 ) select PARSE_JSON('{
    "name": "John",
    "age": 25,
    "city": "Sampleville",
    "married": false,
    "hobbies": ["reading", "traveling", "programming"],
    "address": {
      "street": "123 Main Street",
      "city": "Sample City",
      "postal_code": "12345"
    }
  }');

select $1:name::string from t_sample;

select t.$1:name::string as Name,
t.$1:age::number as Age,
t.$1:city::varchar as City,
t.$1:married::varchar as Married,
LISTAGG(s.value::VARCHAR, ', ') AS Hobbies,
t.$1:address.street::VARCHAR AS Street,
t.$1:address.city::VARCHAR AS Address_City,
t.$1:address.postal_code::VARCHAR AS Postal_Code

from t_sample t,
lateral flatten(t.$1:hobbies) s,
GROUP BY 
    Name, Age, City, Married, Street, Address_City, Postal_Code;
;

SELECT s.value:name::STRING AS name
FROM @vijay_stage t,
LATERAL FLATTEN(input => t.$1) s
WHERE s.value:name IS NOT NULL;
;

SELECT 
  value:name AS name
FROM 
  @vijay_stage t,
  LATERAL FLATTEN(input => t.$1);

select s.* from t_sample t,
lateral flatten(t.$1) s
;







source destination distance 
pune    mumbai      100
mumbai  pune        100
;
create or replace temporary table travel (source varchar, destination varchar, distance number);

insert into travel values('Pune','Mumbai',100), ('Mumbai','Pune',100);


select t1.source, t1.destination, t1.distance from travel t1
join travel t2
on t1.source = t2.destination
group by t1.source,t1.destination, t1.distance;

SELECT LEAST(source, destination) AS source,
       GREATEST(source, destination) AS destination,
       distance
FROM travel
GROUP BY LEAST(source, destination), GREATEST(source, destination), distance;


CREATE TABLE travel (
    source VARCHAR,
    destination VARCHAR,
    distance NUMBER,
    CONSTRAINT pk_travel PRIMARY KEY (source, destination)
);











 