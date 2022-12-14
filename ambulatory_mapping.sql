CREATE VIEW AMBULATORY_MAPPING AS
SELECT a.*, regexp_replace(DEPARTMENT_OLD,'_DEACTIVATED|X_','') AS DEPARTMENT
FROM(
    SELECT distinct CAMPUS, CAMPUS_SPECIALTY, DEPARTMENT AS DEPARTMENT_OLD, DEPARTMENT_ID
    FROM AMBULATORY_ACCESS_TABLE
) a
