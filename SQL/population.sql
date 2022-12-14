CREATE TABLE POPULATION_SQL AS 
SELECT g.*
FROM(
SELECT e.*,
CASE 
   WHEN (e.ZIP_CODE_LAYER_A = 'Out of NYS'  AND e.ZIP_CODE_LAYER_B is NULL)
     THEN
         CASE
           WHEN e.STATES = 'NJ' THEN 'New Jersey'
           WHEN e.STATES = 'CT' THEN 'Connecticut'
           WHEN e.STATES = 'FL' THEN 'Florida'
           WHEN e.STATES = 'PA' THEN 'Pennsylvania'
           ELSE 'Other'
           END
    ELSE   e.ZIP_CODE_LAYER_B  
 END AS NEW_ZIP_CODE_LAYER_B,
 CASE 
   WHEN (e.ZIP_CODE_LAYER_A is NULL  AND 
        ( e.STATES is NOT NULL OR e.STATES != 'NY')
        )
        THEN 'Out of NYS'
        ELSE e.ZIP_CODE_LAYER_A
END AS NEW_ZIP_CODE_LAYER_A
FROM(
SELECT b.*, c.ZIP_CODE_LAYER_A, c.ZIP_CODE_LAYER_B, c.ZIP_CODE_LAYER_C, d.CITY, d.STATES, d.LATITUDE, d.LONGITUDE
FROM(
SELECT a.*,
 SUBSTR(a.ZIP_CODE, 1, 5) AS NEW_ZIP 
FROM ACCESS_SQL a 
    ) b
    INNER JOIN ZIP_CODE_LAYER c ON b.NEW_ZIP = c.ZIP_CODE AND b.APPT_STATUS = 'Arrived'
    INNER JOIN ZIP_CITY_STATE d ON b.NEW_ZIP = d.ZIP
       ) e
       
       )g
WHERE g.ZIP_CODE_LAYER_A is NOT NULL

                 


