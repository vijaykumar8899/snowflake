
create or replace procedure pr_hana_to_sf()
returns varchar
language sql 
as
$$
DECLARE 
    V_RUN_OUT_SCENARIO INT;
    V_CURR_FISC_YEAR INT;
    V_BEGIN_FISC_YEAR INT;
    V_CURR_FISC_QTR STRING;
    V_CURR_DATE STRING;
    V_CURR_FISC_OTR STRING;

BEGIN
    BEGIN TRANSACTION;
    --select * from INFO_TECH_COMP;
    SELECT DISTINCT CURR_DATE INTO V_CURR_DATE FROM INFO_TECH_COMP limit 1;
SELECT DISTINCT SUBSTRING (FY_AND_FQ_NAME, 0, 4), FY_AND_FQ_NAME INTO V_CURR_FISC_YEAR, V_CURR_FISC_OTR
FROM INFO_TECH_COMP WHERE DATE_KEY = :V_CURR_DATE;

    V_BEGIN_FISC_YEAR:= V_CURR_FISC_YEAR - 7;

     SELECT CASE 
        WHEN DAY_NUMBER_IN_FQ = 1 THEN 1
        WHEN DAY_NUMBER_IN_FQ = 2 THEN 2
        WHEN DAY_NUMBER_IN_FQ = 3 THEN 3
        WHEN DAY_NUMBER_IN_FQ >= 4 THEN 4
    END AS RUN_OUT_SCENARIO INTO V_RUN_OUT_SCENARIO 
    FROM INFO_TECH_COMP
    WHERE DATE_KEY = :V_CURR_DATE
    LIMIT 1;

    UPDATE INFO_TECH_COMP DCT
    SET CC_FBD_QTD_FLAG = 0;

    COMMIT;

    
    BEGIN TRANSACTION;

     IF (V_RUN_OUT_SCENARIO = 1) THEN 
        BEGIN
            UPDATE INFO_TECH_COMP 
            SET CC_FBD_QTD_FLAG = 1
            WHERE FQS_FROM_CURRENT_FQ < 0 
              AND DATE_KEY < V_CURR_DATE 
              AND TO_NUMBER(FY_NAME) >= V_BEGIN_FISC_YEAR
              AND TO_NUMBER(FY_NAME) <= V_CURR_FISC_YEAR
              AND DATE_KEY <= V_CURR_DATE;
        END;
    END IF;

    UPDATE INFO_TECH_COMP
SET CC_FBD_QTD_FLAG = 0
WHERE FQS_FROM_CURRENT_FQ >= 0 AND (DATE_KEY >= V_CURR_DATE) AND (TO_INTEGER(FY_NAME) >= V_BEGIN_FISC_YEAR
 AND (TO_INTEGER( FY_NAME) <= V_CURR_FISC_YEAR AND DATE_KEY <= V_CURR_DATE )); 

    
    COMMIT;
    return 'successfull';

EXCEPTION
     when statement_Error then 
      return object_construct('sqlcode',sqlcode,'sqlerrm',sqlerrm,'sqlstate',sqlstate);
    when expression_error then 
      return object_construct('sqlcode',sqlcode,'sqlerrm',sqlerrm,'sqlstate',sqlstate); 
       when other then 
      return object_construct('sqlcode',sqlcode,'sqlerrm',sqlerrm,'sqlstate',sqlstate); 
    

END;
$$;


call pr_hana_to_sf();

