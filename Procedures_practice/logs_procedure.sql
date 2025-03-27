create or replace table logs(logid number AUTOINCREMENT start 1 increment 1, description varchar, log_at timestamp);
insert into logs (description, log_at) values('invalid p_empno', current_timestamp());

alter session set timezone='Asia/Kolkata';
show parameters like 'timezone';

create or replace procedure pr_update_com(p_empno number)
returns varchar
language sql 
execute as caller 
as
$$
DECLARE
    v_updated_comm number;
    v_sal number;
BEGIN
    select sal,comm into v_sal,v_updated_comm from emp where empno = p_empno;
    if(v_sal > 10000) then
        update emp set comm = (v_sal * 0.1);
    elseif(v_sal < 1000) then
        update emp set comm = (v_sal * 0.05);
    end if;
        
    return 'comm updated for ' || TO_VARCHAR(p_empno);

EXCEPTION
    when expression_error then
        insert into logs (description, log_at) values('sqlerrm : ' || TO_VARCHAR(:sqlerrm), current_timestamp());
        return object_construct('sqlstate':sqlstate,'sqlerrm',SQLERRM);

    when STATEMENT_ERROR then
        insert into logs (description, log_at) values('sqlerrm : ' || TO_VARCHAR(:sqlerrm), current_timestamp());
        return object_construct('sqlstate':sqlstate,'sqlerrm',SQLERRM);
    
    when other then
        insert into logs (description, log_at) values('sqlerrm : ' || TO_VARCHAR(:sqlerrm), current_timestamp());
        return object_construct('sqlstate':sqlstate,'sqlerrm',SQLERRM);
    
END;

$$;

select current_timestamp();
select * from logs order by logid desc;
call pr_update_com(123);

