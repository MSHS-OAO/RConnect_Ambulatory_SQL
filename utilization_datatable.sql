CREATE TABLE "utilization_table" 
(
 DEPARTMENT_ID NUMBER
, RESOURCES VARCHAR2(100) NOT NULL 
, PROVIDER VARCHAR2(100) NOT NULL 
, VISIT_METHOD VARCHAR2(100) NOT NULL 
, APPT_DATE_YEAR DATE NOT NULL 
, APPT_MONTH_YEAR VARCHAR2(10) NOT NULL 
, APPT_YEAR NUMBER NOT NULL 
, APPT_WEEK DATE NOT NULL 
, APPT_DAY VARCHAR2(10) NOT NULL 
, HOLIDAY VARCHAR2(100) 
, APPT_DUR NUMBER 
, Appt_Start DATE 
, Appt_End DATE 
, APPT_DTTM DATE 
, APPT_STATUS VARCHAR2(100) 
, MRN VARCHAR2(100) 
, SUM NUMBER  
, UTIL_TYPE VARCHAR2(100) NOT NULL
, APPT_TYPE VARCHAR2(200) 
, H_07_00 NUMBER 
, H_08_00 NUMBER
, H_09_00 NUMBER
, H_10_00 NUMBER
, H_11_00 NUMBER
, H_12_00 NUMBER
, H_13_00 NUMBER
, H_14_00 NUMBER
, H_15_00 NUMBER
, H_16_00 NUMBER
, H_17_00 NUMBER
, H_18_00 NUMBER
, H_19_00 NUMBER
, H_20_00 NUMBER
  
,CONSTRAINT utilization_pk PRIMARY KEY (DEPARTMENT_ID, PROVIDER, MRN, APPT_DTTM, UTIL_TYPE)
);
