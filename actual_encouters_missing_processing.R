

library(tidyverse)
library(lubridate)
library(odbc)
library(glue)

timeOptionsHr_filter <- c("07:00","08:00","09:00", "10:00","11:00","12:00","13:00",
                          "14:00","15:00","16:00","17:00","18:00","19:00",
                          "20:00") ## Time Range by Hour Filter


conn <- dbConnect(odbc(), "OAO Cloud DB")
utilization_tbl <- tbl(conn, "utilization_table")

##SQL query that returns rows that are in scheduled but not in actual
missing_actual_data_query <- glue("SELECT q1.*
FROM
(SELECT * FROM \"utilization_table_new\" WHERE UTIL_TYPE = 'scheduled' AND PAT_ENC_CSN_ID IS NOT NULL) q1
LEFT JOIN
(SELECT * FROM \"utilization_table_new\" WHERE UTIL_TYPE = 'actual' AND PAT_ENC_CSN_ID IS NOT NULL) q2
   on q1.PAT_ENC_CSN_ID = q2.PAT_ENC_CSN_ID
       WHERE q2.PAT_ENC_CSN_ID IS NULL")

missing_actual_data <- dbGetQuery(conn, missing_actual_data_query)
missing_actual_data <- missing_actual_data %>% select(-SCHEDULE_TO_ACTUAL_CONVERSION, -APPT_START, -APPT_END, -UTIL_TYPE)

# missing_data_processing <- function(missing_data) {    
  ## Create new column if row was converted from scheduled to actual
  # Pull scheduled data for specific missing encounters time stamps
  median_roomtime_mapping <- utilization_tbl %>% 
                             filter(UTIL_TYPE == 'actual') %>%
                             group_by(DEPARTMENT_ID, APPT_TYPE) %>%
                             summarise(median_room_time = median(SUM, na.rm = TRUE)) %>%
                             collect()
  
 
  mapped_actual_data <- left_join(missing_actual_data, median_roomtime_mapping)
  
  
  table(is.na(mapped_actual_data$median_room_time)) ## CHeck how many NAs after joining
  
  # Convert pulled missing scheudled ecnouters to actual 
   mapped_actual_data <- mapped_actual_data %>% 
   mutate(SCHEDULE_TO_ACTUAL_CONVERSION = ifelse(is.na(median_room_time), "TRUE", "FALSE")) %>%
   mutate(util_type = 'actual')%>% 
   mutate(median_room_time = ifelse(is.na(median_room_time),SUM, median_room_time))
   
   #mapped_actual_data <- mapped_actual_data %>% mutate(median_room_time =ifelse(median_room_time == 0 , 5, median_room_time))
   columns_to_remove <- c("H_07_00", "H_08_00", "H_09_00", "H_10_00", "H_11_00", "H_12_00", "H_13_00",
                           "H_14_00", "H_15_00", "H_16_00", "H_17_00", "H_18_00","H_19_00", "H_20_00")
   mapped_actual_data <- mapped_actual_data %>% select(-c(all_of(columns_to_remove)))
   
   mapped_actual_data <- mapped_actual_data %>% mutate(Appt.Start = as.POSIXct(APPT_DTTM, format = "%H:%M"))
   mapped_actual_data <- mapped_actual_data %>% mutate(Appt.End = as.POSIXct(Appt.Start + median_room_time*60, format = "%H:%M"))
   mapped_actual_data <- mapped_actual_data %>% mutate(Appt.Date = as.Date(APPT_DTTM))
   
   
   
   #Create new columns
   hours <- c("0", "1", "2", "3", "4", "5", "6", "7","8","9",
              "10","11","12","13","14","15","16","17","18","19",
              "20", "21", "22", "23")
   
   
   ##Fill new columns with NA
   mapped_actual_data[, hours] <- NA
   
   
   
   
   mapped_actual_data <- mapped_actual_data %>% pivot_longer(all_of(hours), names_to = "hour", values_to = "min") %>% mutate(hour = as.numeric(hour))
   
   
   mapped_actual_data <- mapped_actual_data %>% group_by(DEPARTMENT_ID, PROV_ID, MRN, APPT_DTTM) %>% filter(hour(Appt.Start) <= hour)
   mapped_actual_data <- mapped_actual_data %>% group_by(DEPARTMENT_ID, PROV_ID, MRN, APPT_DTTM) %>% filter(hour(Appt.End) >= hour)
   
   suppressWarnings(
     mapped_actual_data <- mapped_actual_data %>% 
       mutate(min = ifelse(hour(Appt.Start) == hour(Appt.End), difftime(Appt.End, Appt.Start, units = "mins"),
                           ifelse(hour(Appt.Start) == hour, difftime(ceiling_date(Appt.Start, "hour", change_on_boundary = T), Appt.Start, units = "mins"),
                                  ifelse(hour(Appt.End) == hour,  difftime(Appt.End, floor_date(Appt.End, "hour"), units = "mins"),
                                         ifelse(hour %in% hour(Appt.Start):hour(Appt.End) , 60, NA
                                         )
                                  )
                           )
       )
       )
   )
   mapped_actual_data <- mapped_actual_data %>% mutate(min = ifelse(min == 0, NA, min))
   
   ##Rename columns to HH:MM format
   mapped_actual_data <- mapped_actual_data %>%
     mutate(hour= str_pad(hour, width = nchar("xx"), pad = "0", side = "left")) %>%
     mutate(hour = paste0(hour, ":00"))
   
   
   mapped_actual_data <- mapped_actual_data %>%
                dplyr::group_by(DEPARTMENT_ID, RESOURCES, PROV_ID, VISIT_METHOD, APPT_DATE_YEAR, APPT_MONTH_YEAR, APPT_YEAR, APPT_WEEK, APPT_DAY, HOLIDAY, APPT_DUR, APPT_DTTM, APPT_STATUS, MRN, APPT_TYPE, Appt.Start, Appt.End, Appt.Date, hour) %>%
                dplyr::mutate(n = dplyr::n()) %>%
                filter(n <=1) %>%
                  select(-n) %>%
                ungroup()
    

   
   ##Pviot data on hour 
   mapped_actual_data <- mapped_actual_data %>% pivot_wider(names_from = "hour", values_from = "min")
   
   
   
   timeOptionsHr <- c("00:00", "01:00", "02:00" , "03:00", "04:00", "05:00", "06:00", "07:00","08:00","09:00",
                      "10:00","11:00","12:00","13:00","14:00","15:00","16:00","17:00","18:00","19:00",
                      "20:00", "21:00", "22:00", "23:00")
   
   
   
   col_index <- which(!(timeOptionsHr %in% names(mapped_actual_data))) 
   
   if(length(col_index) >= 1) {
     column_to_add <- timeOptionsHr[col_index]
     
     
     for(i in 1:length(column_to_add)){
       
       mapped_actual_data[[column_to_add[i]]] <- NA
     }
   }
   
   
   
   # mapped_actual_data <- mapped_actual_data[,c(names(mapped_actual_data[,1:19]), timeOptionsHr) ]
   
   # check this ,
   mapped_actual_data <- mapped_actual_data %>% 
     select(DEPARTMENT_ID, PROV_ID, RESOURCES,  VISIT_METHOD, PROV_ID, APPT_DATE_YEAR, APPT_MONTH_YEAR, 
            APPT_YEAR, APPT_WEEK, APPT_DAY, HOLIDAY, APPT_DUR, Appt.Start, Appt.End, APPT_DTTM, APPT_STATUS,
            MRN, APPT_TYPE, median_room_time, SCHEDULE_TO_ACTUAL_CONVERSION, all_of(timeOptionsHr), PAT_ENC_CSN_ID,util_type) 
   
   
   mapped_actual_data$sum <- rowSums(mapped_actual_data[,which(colnames(mapped_actual_data)=="00:00"): which(colnames(mapped_actual_data) =="23:00")], na.rm = T)
   
   mapped_actual_data <- mapped_actual_data %>% mutate(actual = as.numeric(difftime(Appt.End, Appt.Start, units = "mins")))
   mapped_actual_data <- mapped_actual_data  %>% mutate(comparison = ifelse(sum == actual, 0, 1))
   mapped_actual_data <- mapped_actual_data %>% filter(comparison == 0)
#}

   mapped_actual_data <- mapped_actual_data %>% select(DEPARTMENT_ID, RESOURCES, PROV_ID, VISIT_METHOD, APPT_DATE_YEAR, 
                                                       APPT_MONTH_YEAR, APPT_YEAR, APPT_WEEK, APPT_DAY, HOLIDAY, APPT_DUR, 
                                                       Appt.Start, Appt.End, APPT_DTTM, APPT_STATUS, MRN, sum, util_type, 
                                                       APPT_TYPE, all_of(timeOptionsHr_filter), SCHEDULE_TO_ACTUAL_CONVERSION, PAT_ENC_CSN_ID)

   
   TABLE_NAME <-  "utilization_table"
   
   # section to write mapped_actual_data to database
   get_values <- function(x, table_name){
     
     DEPARTMENT_ID <- x[1]
     RESOURCES <- x[2]
     PROV_ID <- x[3]
     VISIT_METHOD <- x[4]
     APPT_DATE_YEAR <- x[5]
     APPT_MONTH_YEAR <- x[6]
     APPT_YEAR <- x[7]
     APPT_WEEK <- x[8]
     APPT_DAY <- x[9]
     HOLIDAY <- x[10]
     APPT_DUR <- x[11]      
     Appt.Start <- x[12]
     Appt.End <- x[13]
     APPT_DTTM <- x[14]
     APPT_STATUS <- x[15]
     MRN <-  x[16]
     sum <- x[17]
     util_type <- x[18]
     APPT_TYPE <- x[19]
     `07:00` <- x[20]
     `08:00` <-  x[21]
     `09:00` <-  x[22]
     `10:00` <-  x[23]
     `11:00` <-  x[24]
     `12:00` <-  x[25]
     `13:00` <-  x[26]
     `14:00` <-  x[27]
     `15:00` <-  x[28]
     `16:00` <-  x[29]
     `17:00` <-  x[30]
     `18:00` <-  x[31]
     `19:00` <-  x[32]
     `20:00` <-  x[33]
     SCHEDULE_TO_ACTUAL_CONVERSION <- x[34]
     PAT_ENC_CSN_ID <- x[35]
     
     
     values <- glue("INTO \"{table_name}\" (DEPARTMENT_ID, 
                   RESOURCES, PROV_ID, VISIT_METHOD, APPT_DATE_YEAR, 
                   APPT_MONTH_YEAR, APPT_YEAR, APPT_WEEK, APPT_DAY, HOLIDAY, 
                   APPT_DUR, Appt_Start, Appt_End, APPT_DTTM, APPT_STATUS, MRN, SUM, util_type, APPT_TYPE,
                   H_07_00, H_08_00, H_09_00, H_10_00, H_11_00, H_12_00, H_13_00, 
                   H_14_00, H_15_00, H_16_00, H_17_00, H_18_00, H_19_00, H_20_00, SCHEDULE_TO_ACTUAL_CONVERSION, PAT_ENC_CSN_ID
                   )
                   VALUES('{DEPARTMENT_ID}', '{RESOURCES}','{PROV_ID}',
                   '{VISIT_METHOD}',  TO_DATE('{APPT_DATE_YEAR}', 'YYYY-MM-DD'),
                   '{APPT_MONTH_YEAR}', '{APPT_YEAR}', TO_DATE('{APPT_WEEK}', 'YYYY-MM-DD'), 
                   '{APPT_DAY}', '{HOLIDAY}', '{APPT_DUR}', TO_DATE('{Appt.Start}', 'YYYY-MM-DD HH24:MI:SS'), 
                    TO_DATE('{Appt.End}', 'YYYY-MM-DD HH24:MI:SS'), TO_DATE('{APPT_DTTM}', 'YYYY-MM-DD HH24:MI:SS'),
                    '{APPT_STATUS}', '{MRN}', '{sum}', '{util_type}', '{APPT_TYPE}', '{`07:00`}','{`08:00`}','{`09:00`}', '{`10:00`}',
                    '{`11:00`}','{`12:00`}', '{`13:00`}', '{`14:00`}','{`15:00`}',
                    '{`16:00`}', '{`17:00`}','{`18:00`}','{`19:00`}','{`20:00`}', '{SCHEDULE_TO_ACTUAL_CONVERSION}', '{PAT_ENC_CSN_ID}')")
     
     
     return(values)
   }
   
   
   
   # Add UPDATE_TIME and check for all the fields are characters
   mapped_actual_data <-  mapped_actual_data %>%
     mutate(DEPARTMENT_ID = as.numeric(DEPARTMENT_ID),
            RESOURCES = as.character(RESOURCES), 
            PROV_ID = as.character(PROV_ID),
            VISIT_METHOD = as.character(VISIT_METHOD),
            APPT_DATE_YEAR = as.character(APPT_DATE_YEAR), 
            APPT_MONTH_YEAR = as.character(APPT_MONTH_YEAR),
            APPT_YEAR = as.numeric(APPT_YEAR), 
            APPT_WEEK = as.character(APPT_WEEK),
            APPT_DAY = as.character(APPT_DAY),
            HOLIDAY = as.character(HOLIDAY),
            APPT_DUR= as.numeric(APPT_DUR), 
            Appt.Start= as.character(Appt.Start), 
            Appt.End= as.character(Appt.End), 
            APPT_DTTM= as.character(APPT_DTTM), 
            APPT_STATUS= as.character(APPT_STATUS),
            MRN= as.character(MRN),
            sum = as.numeric(sum), 
            util_type = as.character(util_type),
            APPT_TYPE = as.character(APPT_TYPE),
            `07:00`= as.numeric(`07:00`),
            `08:00`= as.numeric(`08:00`),
            `09:00`= as.numeric(`09:00`),
            `10:00`= as.numeric(`10:00`),
            `11:00`= as.numeric(`11:00`),
            `12:00`= as.numeric(`12:00`),
            `13:00`= as.numeric(`13:00`),
            `14:00`= as.numeric(`14:00`),
            `15:00`= as.numeric(`15:00`),
            `16:00`= as.numeric(`16:00`),
            `17:00`= as.numeric(`17:00`),
            `18:00`= as.numeric(`18:00`),
            `19:00`= as.numeric(`19:00`),
            `20:00`= as.numeric(`20:00`),
            SCHEDULE_TO_ACTUAL_CONVERSION = as.character(SCHEDULE_TO_ACTUAL_CONVERSION),
            PAT_ENC_CSN_ID <- as.character(PAT_ENC_CSN_ID))%>%
     
     select(DEPARTMENT_ID, RESOURCES, PROV_ID, 
            VISIT_METHOD, APPT_DATE_YEAR, APPT_MONTH_YEAR, APPT_YEAR, APPT_WEEK, 
            APPT_DAY, HOLIDAY, APPT_DUR, Appt.Start, Appt.End, APPT_DTTM, 
            APPT_STATUS, MRN,sum, util_type, APPT_TYPE, `07:00`,`08:00`,`09:00`, `10:00`,`11:00`,`12:00`,
            `13:00`, `14:00`,`15:00`,`16:00`,`17:00`,`18:00`,`19:00`,`20:00`, SCHEDULE_TO_ACTUAL_CONVERSION,PAT_ENC_CSN_ID )
   
   special_character_columns <- c("RESOURCES", "VISIT_METHOD", "APPT_STATUS", "APPT_TYPE")
   mapped_actual_data <-  mapped_actual_data %>% mutate_at(all_of(special_character_columns), ~ str_replace(., "'", "''"))
   
   
   # Convert the each record/row of tibble to INTO clause of insert statment
   inserts <- lapply(
     lapply(
       lapply(split( mapped_actual_data , 
                    1:nrow(mapped_actual_data)),
              as.list), 
       as.character),
     FUN = get_values ,TABLE_NAME)
   
   
   
   chunk_length <- 250
   split_queries <- split(inserts, ceiling(seq_along(inserts) / chunk_length))
   
   
   
   connection <- dbConnect(odbc(), "OAO Cloud DB")
 
   split_queries_test <- list()
   for (i in 1:length(split_queries)) {
     row <- glue_collapse(split_queries[[i]], sep = "\n\n")
     row <- gsub('NA', "", row)
     row <- gsub("&", " ' || chr(38) || ' ", row)
     sql <- glue('INSERT ALL {row} SELECT 1 FROM DUAL;')
     split_queries_test <- append(split_queries_test, sql)
   }
   registerDoParallel()
   system.time(
     outputPar <- foreach(i = 1:length(split_queries_test), .packages = c("DBI", "odbc"))%dopar%{
       #Connecting to database through DBI
       ch = dbConnect(odbc(), "OAO Cloud DB")
       #Test connection
       tryCatch({
         dbBegin(ch)
         dbExecute(ch, split_queries_test[[i]])
         dbCommit(ch)
       },
       error = function(err){
         print("error")
         dbRollback(ch)
         dbDisconnect(ch)
         
       })
     }
   )
   registerDoSEQ()
