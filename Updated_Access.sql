CREATE View AMBULATORY_ACCESS AS
SELECT i.*, 
CASE i.ACCESS_CENTER WHEN 'Y' THEN 'Access Center' ELSE i.APPT_SOURCE_MAP_OTHER END AS Appt_Source_New
FROM(
    SELECT c.*, APPT_DATE_YEAR - DAYS_SUBTRACT AS APPT_WEEK, NVL(c.APPT_SOURCE_MAP, 'Other') AS APPT_SOURCE_MAP_OTHER
    FROM(
    SELECT d.*, b.holiday, f.DAYS_SUBTRACT, h.APPT_SOURCE_NEW AS APPT_SOURCE_MAP
    FROM(
        SELECT a.DEP_RPT_GRP_SEVENTEEN AS Campus, a.DEPT_SPECIALTY_NAME AS Campus_Specialty, a.DEPARTMENT_NAME AS Department, a.PROV_NAME_WID, a.DEPARTMENT_ID AS Department_ID, a.REFERRING_PROV_NAME_WID AS Refferring_Provider,
                             a.MRN, a.PAT_NAME AS Patient_Name, a.ZIP_CODE, a.BIRTH_DATE, a.FINCLASS AS Coverage,
                             a.APPT_MADE_DTTM, a.APPT_DTTM, a.PRC_NAME AS APPT_TYPE, a.APPT_LENGTH AS APPT_DUR, a.DERIVED_STATUS_DESC AS APPT_STATUS,
                             a.APPT_CANC_DTTM AS APPT_CANCEL_DTTM, a.CANCEL_REASON_NAME AS CANCEL_REASON,
                             a.SIGNIN_DTTM, a.PAGED_DTTM, a.CHECKIN_DTTM, a.ARVL_LIST_REMOVE_DTTM AS ARRIVAL_REMOVE_DTTM,
                             a.ROOMED_DTTM AS ROOMIN_DTTM, a.FIRST_ROOM_ASSIGN_DTTM AS ROOM_ASSIGNED_DTTM, 
                             a.PHYS_ENTER_DTTM AS PROVIDERIN_DTTM, a.VISIT_END_DTTM, a.CHECKOUT_DTTM,
                             a.TIME_IN_ROOM_MINUTES AS TIMEIN_ROOM, a.CYCLE_TIME_MINUTES AS CYCLE_TIME, a.LOS_NAME AS CLASS_PT, a.LOS_CODE,
                             a.DEP_RPT_GRP_THIRTYONE AS CADENCE, a.APPT_ENTRY_USER_NAME_WID AS APPT_SOURCE, 
                             a.ACCESS_CENTER_SCHEDULED_YN AS ACCESS_CENTER, a.VISIT_METHOD, a.VISIT_PROV_STAFF_RESOURCE_C,
                             a.PRIMARY_DX_CODE, a.ENC_CLOSED_CHARGE_STATUS, a.Y_ENC_COSIGN_TIME, a.Y_ENC_CLOSE_TIME, a.Y_ENC_OPEN_TIME, a.NPI, a.VISIT_GROUP_NUM AS NEW_PT, 
                             a.PAT_ENC_CSN_ID, a.VISITPLAN, a.ATTRIB_BILL_AREA,
                             EXTRACT(year from a.APPT_DTTM) Appt_Year,
                             CONCAT(TO_CHAR(a.APPT_DTTM, 'HH24'), ':00') APPT_TM_HR,
                             TO_CHAR(a.APPT_DTTM, 'MON') AS Appt_Month,
                             TO_CHAR(a.APPT_DTTM, 'yyyy-mm') AS Appt_Month_Year,
                             TO_CHAR(a.APPT_MADE_DTTM, 'yyyy-mm') AS Appt_Made_Month_Year,
                             --TO_CHAR(trunc(TO_DATE(a.APPT_DTTM, 'yyyy-mm-dd HH24:MI:SS'))) AS Appt_Date_Year,
                             trunc(a.APPT_DTTM) AS Appt_Date_Year,
                             TO_CHAR(a.APPT_DTTM, 'mm-dd') AS Appt_Date,
                             TRIM(TRAILING FROM REGEXP_REPLACE(a.PROV_NAME_WID, '\[(.*?)\]', '')) AS Provider,
                             LEAST(a.VISIT_END_DTTM, a.CHECKOUT_DTTM) AS min_of_checkout_visit,
                             (LEAST(a.VISIT_END_DTTM, a.CHECKOUT_DTTM)  - a.CHECKIN_DTTM)*24*60 AS cycleTime,
                             (a.ROOMED_DTTM - a.CHECKIN_DTTM) * 24*60 AS checkinToRoomin, 
                             CASE a.VISIT_PROV_STAFF_RESOURCE_C WHEN 1 THEN 'Provider' ELSE 'Resource' END AS Resources,
                             --(Provider_Leave_DTTM - PHYS_ENTER_DTTM)*24*60 AS providerinToOut 
                             (a.CHECKOUT_DTTM - a.VISIT_END_DTTM)*24*60 AS visitEndToCheckout,  
                             (a.Appt_DTTM - a.APPT_MADE_DTTM) AS Wait_Time,
                             (a.APPT_DTTM - a.APPT_CANC_DTTM) AS Lead_Days,
                             TRIM( ',' FROM a.DEPARTMENT_NAME||','||a.PROV_NAME_WID||','||a.MRN||','||a.APPT_DTTM ) AS uniqueID,
                             TO_CHAR(a.APPT_DTTM, 'DY') AS APPT_DAY,
                             CASE WHEN a.VISIT_GROUP_NUM = 4 THEN 'NEW' ELSE 'ESTABLISHED' END AS NEW_PT2,
                             REGEXP_SUBSTR(a.LOS_NAME, 'NEW') AS NEW_PT3
    FROM MV_DM_PATIENT_ACCESS a
        WHERE a.CONTACT_DATE BETWEEN TO_DATE('2021-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
                        AND TO_DATE('2022-08-25 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
                             OR a.APPT_MADE_DTTM BETWEEN TO_DATE('2021-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
                        AND TO_DATE('2022-08-25 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
                    ) d
                    LEFT JOIN holidays b on d.Appt_Date_Year = b.dates
                    LEFT JOIN SUBTRACT_DAYS f on d.APPT_DAY = f.WEEKDAY
                    LEFT JOIN AMBULATORY_APPT_SOURCE h on d.APPT_SOURCE = h.APPT_SOURCE
                    ) c
                ) i       
                
                
                
                
                
                
                
     DROP VIEW AMBULATORY_ACCESS
CREATE VIEW AMBULATORY_ACCESS AS
SELECT c.*, CASE c.ACCESS_CENTER WHEN 'Y' THEN 'Access Center' ELSE c.APPT_SOURCE_MAP_OTHER END AS Appt_Source_New 
FROM(
    SELECT b.*, APPT_DATE_YEAR - DAYS_SUBTRACT AS APPT_WEEK, NVL(b.APPT_SOURCE_MAP, 'Other') AS APPT_SOURCE_MAP_OTHER
    FROM(
        SELECT a.*,
            EXTRACT(year from a.APPT_DTTM) Appt_Year,
            CONCAT(TO_CHAR(a.APPT_DTTM, 'HH24'), ':00') APPT_TM_HR,
            TO_CHAR(a.APPT_DTTM, 'MON') AS Appt_Month,
            TO_CHAR(a.APPT_DTTM, 'yyyy-mm') AS Appt_Month_Year,
            TO_CHAR(a.APPT_MADE_DTTM, 'yyyy-mm') AS Appt_Made_Month_Year,
            TO_CHAR(a.APPT_DTTM, 'mm-dd') AS Appt_Date,
            LEAST(a.VISIT_END_DTTM, a.CHECKOUT_DTTM) AS min_of_checkout_visit,
            (LEAST(a.VISIT_END_DTTM, a.CHECKOUT_DTTM)  - a.CHECKIN_DTTM)*24*60 AS cycleTime,
            (a.ROOMIN_DTTM - a.CHECKIN_DTTM) * 24*60 AS checkinToRoomin, 
            (a.CHECKOUT_DTTM - a.VISIT_END_DTTM)*24*60 AS visitEndToCheckout,  
            (a.Appt_DTTM - a.APPT_MADE_DTTM) AS Wait_Time,
            (TRUNC(a.APPT_DTTM) - TRUNC(a.APPT_CANCEL_DTTM)) AS Lead_Days,
            TRIM( ',' FROM a.DEPARTMENT||','||a.PROV_NAME_WID||','||a.MRN||','||a.APPT_DTTM ) AS uniqueID,
            CASE WHEN a.NEW_PT = 4 THEN 'NEW' ELSE 'ESTABLISHED' END AS NEW_PT2,
            REGEXP_SUBSTR(a.CLASS_PT, 'NEW') AS NEW_PT3,
            CONCAT('Q',TO_CHAR(a.APPT_DTTM, 'Q')) AS APPT_QUARTER
        FROM AMBULATORY_ACCESS_TABLE a ) b
    ) c
    
