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
  
  
  # mapped_actual_data_na_count <- mapped_actual_data %>%
  #                       group_by(DEPARTMENT_ID, APPT_TYPE) %>%
  #                       filter(is.na(median_room_time)) %>%
  #                       summarise(count_na = n())
  # 
  # mapped_actual_data_non_na_count <- mapped_actual_data %>%
  #                                     group_by(DEPARTMENT_ID, APPT_TYPE) %>%
  #                                     filter(!is.na(median_room_time)) %>%
  #                                     summarise(count_non_na = n())
  # 
  # mapped_actual_data_tot_count <- full_join(mapped_actual_data_na_count, mapped_actual_data_non_na_count)
  # 
  # mapped_actual_data_tot_count <- left_join(mapped_actual_data_tot_count, ambulatory_mapping)
  
  table(is.na(mapped_actual_data$median_room_time)) ## CHeck how many NAs after joining
  
  

  
  
  # ambulatory_mapping_tbl <- tbl(conn, "AMBULATORY_MAPPING")
  # ambulatory_mapping <- ambulatory_mapping_tbl %>% collect()
  # 
  # mapped_actual_data_na <- left_join(mapped_actual_data_na, ambulatory_mapping)
  
  mapped_actual_data_na <- mapped_actual_data_na %>% filter(LAST_ARRIVED >= '2022-01-01 00:00:00')
  
  unique(mapped_actual_data_na$APPT_TYPE)
  
  
  #### For remaing 115000 NAs
  # Pull scheduled data for specific missing encounters timestamps
  # Convert pulled missing scheudled ecnouters to actual 
  #Wrtie back converted 
  ## Create new column if row was converted from sceduled to actual
                            
  
  
}