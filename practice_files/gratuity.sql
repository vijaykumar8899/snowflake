create or replace procedure pr_gratuity_check_all_employees()
returns number 
language sql
as 
$$
DECLARE
  id INTEGER DEFAULT 0;
  minimum_price NUMBER(13,2) DEFAULT 22.00;
  maximum_price NUMBER(13,2) DEFAULT 33.00;
  c1 CURSOR FOR SELECT empno FROM emp WHERE empno > ? AND empno < ?;
BEGIN
  OPEN c1 USING (minimum_price, maximum_price);
  FETCH c1 INTO id;
  RETURN id;
END;

$$;

call pr_gratuity_check_all_employees();

create or replace temporary table temp_emp clone emp;
select * from temp_emp;
alter table temp_emp add (gratuity number default 0);

create or replace procedure pr_gratuity_check_all_employees()
returns varchar 
language sql
as 
$$
declare
 v_gratuity number;
 c1 cursor for 
 select empno,ename,sal,datediff('years',hiredate,current_Date()) as experience
 from temp_emp ;
begin
    OPEN c1;
    for i in c1 loop

       if (i.experience>=5 )  then 
          v_gratuity:=round(15 * i.sal * i.experience / 26) ;
          update temp_emp set gratuity=:v_gratuity where empno=i.empno;
  
       end if;
    end loop;


  return 'procedure completed';

  EXCEPTION
   when statement_Error then 
      return object_construct('sqlcode',sqlcode,'sqlerrm',sqlerrm,'sqlstate',sqlstate);
   when expression_Error then 
      return object_construct('sqlcode',sqlcode,'sqlerrm',sqlerrm,'sqlstate',sqlstate);
    when other then 
      return object_construct('sqlcode',sqlcode,'sqlerrm',sqlerrm,'sqlstate',sqlstate);
      
end;
$$;

call pr_gratuity_check_all_employees();