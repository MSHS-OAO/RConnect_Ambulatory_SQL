create or replace FUNCTION UTIL_FUNCTION 
(TIMES IN VARCHAR2 , START_DATE IN DATE , END_DATE IN DATE) 

RETURN NUMBER AS
result_date NUMBER(9,2);

BEGIN
 result_date:= 

   CASE WHEN SYSTEM_DATE(TIMES) BETWEEN START_DATE AND END_DATE THEN 
        CASE WHEN (END_DATE- SYSTEM_DATE(TIMES))*24*60 >= 60 THEN 60
        ELSE (END_DATE- SYSTEM_DATE(TIMES))*24*60
        END
          
   ELSE
       CASE WHEN TRUNC(START_DATE, 'hh')= SYSTEM_DATE(TIMES) THEN
            
            CASE WHEN TRUNC(END_DATE, 'hh') =  SYSTEM_DATE(TIMES) THEN (END_DATE- START_DATE)*24*60
            ELSE (round((SYSTEM_DATE(TIMES)+(30/(60*24))), 'hh') - START_DATE)*24*60
            END
            
      ELSE 0
      END
      
  END;
                            
RETURN result_date;
  
END UTIL_FUNCTION;