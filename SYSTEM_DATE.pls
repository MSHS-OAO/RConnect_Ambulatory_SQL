create or replace FUNCTION SYSTEM_DATE 
(TIMES IN VARCHAR2) 
RETURN DATE AS
date_result DATE;

BEGIN

       date_result := TO_DATE(to_char(SYSDATE, 'YYYY-MM-DD') || ' ' ||TIMES , 'yyyy-mm-dd HH24:MI:SS'); 
       RETURN date_result;
       
END SYSTEM_DATE;