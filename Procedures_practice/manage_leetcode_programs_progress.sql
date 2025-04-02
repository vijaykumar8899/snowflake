--Create a procedure to manage and record the progress of solving problems on LeetCode. The procedure should handle two states (START and END)
--When you send parameter as START, it should insert a record to the table with start_time as current_timestamp
--when you send parameter as END, it should calculate the time difference between start_time and current_timestamp
--and update both end_time and time_taken columns;
create or replace procedure insert_leetcode_record(p_state varchar, p_description varchar)
returns varchar
language sql
as
$$
DECLARE 
    v_id number;
    v_start_time timestamp;
    v_timediff number;
begin

    select coalesce(max(id),0) into :v_id from sql_leetcode_records;

    if(upper(:p_state) = 'START') then
        insert into sql_leetcode_records (id, description, start_time, end_time) 
        values(:v_id + 1,:p_description,current_timestamp(),current_timestamp());
    elseif(upper(:p_state) = 'END') then
        select start_time into v_start_time from sql_leetcode_records where id = :v_id;
         select timediff(minute, start_time, current_timestamp()) 
        into v_timediff from sql_leetcode_records where id = :v_id;
        update sql_leetcode_records set end_time = current_timestamp(), TIME_TAKEN =:v_timediff || ' Minutes';

    end if;    
   
return 'SUCCESS' ;

exception
    when other then 
        return object_construct('sqlstate',sqlstate,'sqlerrm',sqlerrm);
 
end;
$$;

--Call the procedure
alter session set timezone='Asia/Kolkata';
call insert_leetcode_record('end','testsss'); 

--Check output
truncate table sql_leetcode_records;
SELECT * FROM sql_leetcode_records;

--OTHER USED QUERIES
alter table sql_leetcode_records add time_taken_new varchar;
update sql_leetcode_records set time_taken_new = time_taken || ' Minutes';
alter table sql_leetcode_records drop column time_taken;
alter table sql_leetcode_records rename column time_taken_new to time_taken;

