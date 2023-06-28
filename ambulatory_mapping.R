library(odbc)
library(DBI)
library(glue)

load_data <- function() {
    ambulatory_mapping_drop <- glue("DROP TABLE AMBULATORY_MAPPING_TEST_MAGE")

    ambulatory_mapping_query <- glue("CREATE TABLE AMBULATORY_MAPPING_TEST_MAGE AS
                                        SELECT b.CAMPUS, b.CAMPUS_SPECIALTY, b.DEPARTMENT_OLD, b.DEPARTMENT, b.DEPARTMENT_ID, d.LAST_ARRIVED
                                        FROM(
                                        SELECT a.*, regexp_replace(DEPARTMENT_OLD,'_DEACTIVATED|X_','') AS DEPARTMENT,
                                        CASE CAMPUS_OLD WHEN 'MSDD' THEN 'MSDMG' ELSE CAMPUS_OLD END AS CAMPUS
                                        FROM(
                                            SELECT distinct DEP_RPT_GRP_SEVENTEEN  AS CAMPUS_OLD, DEPT_SPECIALTY_NAME  AS CAMPUS_SPECIALTY, DEPARTMENT_NAME AS DEPARTMENT_OLD, DEPARTMENT_ID
                                            FROM MV_DM_PATIENT_ACCESS where DERIVED_STATUS_DESC = 'Arrived'
                                            ) a
                                        ) b 
                                            LEFT JOIN
                                            (SELECT c.*
                                            FROM(
                                                SELECT DEPARTMENT_ID, max(APPT_DTTM) as LAST_ARRIVED
                                                FROM MV_DM_PATIENT_ACCESS where DERIVED_STATUS_DESC = 'Arrived'
                                                GROUP BY DEPARTMENT_ID
                                                ) c
                                                ) d
                                                on b.DEPARTMENT_ID= d.DEPARTMENT_ID"
                                    )



    conn <- dbConnect(drv = odbc(), "OAO Cloud DB", timeout = 30)
        dbBegin(conn)
        if(dbExistsTable(conn, "AMBULATORY_MAPPING_TEST_MAGE")){
               dbExecute(conn, ambulatory_mapping_drop) 
        }
        dbExecute(conn, ambulatory_mapping_query)


        dbCommit(conn)
        dbDisconnect(conn)


}
