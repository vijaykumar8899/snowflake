create or replace table emp (empno number, ename varchar, sal number,com number);

insert into emp values(1,'Vijay',10000,null);
insert into emp values(2,'Deepak',12000,null);
insert into emp values(3,'Ramesh',8000,null);

select * from emp;

call p_comm_update();


create or replace procedure p_comm_update ()
returns varchar 
language sql 
as 
$$
declare 

 c1 cursor for 
 select empno,sal from emp ;
 
 v_empno number;
 v_sal number;
 v_comm number;


begin 
 for i in c1 loop 
   v_empno:=i.empno;
   v_sal := i.sal;

   
   select case when :v_sal<10000 then  :v_Sal*(10/100) 
               when :v_sal>10000 then  :v_Sal*(5/100) 
			   else com end
			   into v_comm 
			   from emp where empno=:v_empno;
			   
  update emp set com=:v_comm where empno=:v_empno;
   
   
 
 end loop;
 
 return 'procedure completed';
 exception 
 when statement_error then 
 return object_construct('sqlcode',sqlcode, 'sqlstate',sqlstate ,'sqlerrm',sqlerrm);

end;

$$;


select * from emp;













