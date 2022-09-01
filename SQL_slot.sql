CREATE VIEW AMBULATORY_SLOT AS
SELECT b.*, c.CAMPUS, c.CAMPUS_SPECIALTY, b.APPT_DATE_YEAR - f.DAYS_SUBTRACT AS APPT_WEEK
FROM (
    SELECT
    a.DEPARTMENT_NAME, a.PROVIDER_NAME AS PROVIDER,
               a.SLOT_BEGIN_TIME AS APPT_DTTM, a.NUM_APTS_SCHEDULED, a.SLOT_LENGTH,
               a.AVAIL_MINUTES, a.ARRIVED_MINUTES,
               a.CANCELED_MINUTES, a.NOSHOW_MINUTES, a.LEFTWOBEINGSEEN_MINUTES,
               a.AVAIL_SLOTS, a.BOOKED_SLOTS, a.ARRIVED_SLOTS, a.CANCELED_SLOTS,
               a.NOSHOW_SLOTS, a.LEFTWOBEINGSEEN_SLOTS, a.ORG_REG_OPENINGS,
               a.ORG_OVBK_OPENINGS, a.PRIVATE_YN, a.DAY_UNAVAIL_YN, a.TIME_UNAVAIL_YN,
               a.DAY_HELD_YN, a.TIME_HELD_YN, a.OUTSIDE_TEMPLATE_YN, a.VISIT_PROV_STAFF_RESOURCE_C AS RESOURCES,
               a.AVAIL_MINUTES/60 AS AVAILABLE_HOURS,
               a.BOOKED_MINUTES/60 AS BOOKED_HOURS,
               a.ARRIVED_MINUTES/60 AS ARRIVED_HOURS,
               a.CANCELED_MINUTES/60 AS CANCELED_HOURS,
               (a.NOSHOW_MINUTES + a.LEFTWOBEINGSEEN_MINUTES)/60 AS NO_SHOW_HOURS,
               (a.BOOKED_MINUTES + a.CANCELED_MINUTES) AS BOOKED_MINUTES,
                --TO_CHAR(trunc(TO_DATE(a.SLOT_BEGIN_TIME, 'yyyy-mm-dd HH24:MI:SS'))) AS Appt_Date_Year,
                trunc(a.SLOT_BEGIN_TIME) AS Appt_Date_Year,
                TO_CHAR(a.SLOT_BEGIN_TIME, 'yyyy-mm') AS Appt_Month_Year,
                EXTRACT(year from a.SLOT_BEGIN_TIME) Appt_Year,
                TO_CHAR(a.SLOT_BEGIN_TIME, 'MON') AS Appt_Month,
                TO_CHAR(a.SLOT_BEGIN_TIME, 'DY') AS APPT_DAY,
                TO_CHAR(trunc(a.SLOT_BEGIN_TIME, 'HH24'), 'HH24:MI') AS APPT_TM_HR,
                TO_CHAR(a.SLOT_BEGIN_TIME, 'HH24:MI') AS TIME,
                'In Person' AS VISIT_METHOD
        FROM Y_DM_BOOKED_FILLED_RATE a
        WHERE a.SLOT_BEGIN_TIME BETWEEN TO_DATE('2021-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
                        AND TO_DATE('2022-08-25 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
                        ) b
    RIGHT JOIN AMBUALTORY_UNIQUE_DEPARTMENT c on b.DEPARTMENT_NAME = c.DEPARTMENT  
    LEFT JOIN SUBTRACT_DAYS f on b.APPT_DAY = f.WEEKDAY
    

