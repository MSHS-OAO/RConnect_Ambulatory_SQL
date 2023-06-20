conn <- dbConnect(odbc(), "OAO Cloud DB")
utilization_tbl <- tbl(conn, "utilization_table")

##SQL query that returns rows that are in scheduled but not in actual
missing_actual_data_query <- glue("SELECT q1.*
FROM
(SELECT * FROM \"utilization_table\" WHERE UTIL_TYPE = 'scheduled') q1
LEFT JOIN
(SELECT * FROM \"utilization_table\" WHERE UTIL_TYPE = 'actual') q2
   on q1.DEPARTMENT_ID = q2.DEPARTMENT_ID AND
       q1.PROV_ID = q2.PROV_ID AND
       q1.MRN = q2.MRN AND
       q1.APPT_DTTM = q2.APPT_DTTM
       WHERE q2.DEPARTMENT_ID IS NULL
       AND q2.PROV_ID IS NULL")

missing_actual_data <- dbGetQuery(conn, missing_actual_data_query)

if(nrow(missing_actual_data) > 0) {
  
 
  
  median_roomtime_mapping <- utilization_tbl %>% 
                             filter(UTIL_TYPE == 'actual') %>%
                             group_by(DEPARTMENT_ID, APPT_TYPE) %>%
                             summarise(median_room_time = median(SUM, na.rm = TRUE)) %>%
                             collect()
  
  mapped_actual_data <- left_join(missing_actual_data, median_roomtime_mapping)
  
  

  
  table(is.na(mapped_actual_data$median_room_time)) ## CHeck how many NAs after joining
  
  

  
  
  #### For remaing 115000 NAs
  # Pull scheduled data for specific missing encounters timestamps
  # Convert pulled missing scheudled ecnouters to actual 
  #Wrtie back converted 
  ## Create new column if row was converted from sceduled to actual
                            
  
  
}