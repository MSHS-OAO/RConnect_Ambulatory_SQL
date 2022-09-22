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
            (a.APPT_DTTM - a.APPT_CANCEL_DTTM) AS Lead_Days,
            TRIM( ',' FROM a.DEPARTMENT||','||a.PROV_NAME_WID||','||a.MRN||','||a.APPT_DTTM ) AS uniqueID,
            CASE WHEN a.NEW_PT = 4 THEN 'NEW' ELSE 'ESTABLISHED' END AS NEW_PT2,
            REGEXP_SUBSTR(a.CLASS_PT, 'NEW') AS NEW_PT3,
            CONCAT('Q',TO_CHAR(a.APPT_DTTM, 'Q')) AS APPT_QUARTER
        FROM AMBULATORY_ACCESS_TABLE a ) b
    ) c
