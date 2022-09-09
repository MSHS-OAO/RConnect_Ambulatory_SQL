scheduled_data_query <- glue("SELECT CAMPUS, CAMPUS_SPECIALTY, DEPARTMENT, RESOURCES, PROVIDER,
                              VISIT_METHOD, APPT_TYPE, APPT_STATUS, APPT_DATE_YEAR, APPT_MONTH_YEAR, APPT_DTTM, APPT_DUR,
                                      APPT_YEAR, APPT_WEEK, APPT_DAY, APPT_TM_HR, HOLIDAY
                               FROM AMBULATORY_ACCESS  where APPT_STATUS= 'Arrived' and APPT_DTTM >= add_months(sysdate, -2)")

conn <- dbConnect(drv = odbc(), "OAO Cloud DB", timeout = 30)
scheduled.data <- dbGetQuery(conn, scheduled_data_query)
dbDisconnect(conn)


# Function for formatting date and time by hour
system_date <- function(time){
  result <- as.POSIXct(paste0(as.character(Sys.Date())," ",time), format="%Y-%m-%d %H:%M:%S")
  return(result)
}

util.function <- function(time, df){
  result <- ifelse(system_date(time) %within% df$time.interval == TRUE,
                   ifelse(difftime(df$Appt.End.Time, system_date(time), units = "mins") >= 60, 60,
                          as.numeric(difftime(df$Appt.End.Time, system_date(time), units = "mins"))),
                   ifelse(floor_date(df$Appt.Start.Time, "hour") == system_date(time),
                          ifelse(floor_date(df$Appt.End.Time, "hour") == system_date(time),
                                 difftime(df$Appt.End.Time, df$Appt.Start.Time, units = "mins"),
                                 difftime(system_date(time) + 60*60, df$Appt.Start.Time, units = "mins")), 0))
  return(result)
}


# # Pre-process Utilization by Hour based on Scheduled Appointment Times --------------------------------------------------
data.hour.scheduled <- scheduled.data %>% filter(Appt.Status == "Arrived")
data.hour.scheduled$actual.visit.dur <- data.hour.scheduled$Appt.Dur * 1.2

data.hour.scheduled$Appt.Start <- as.POSIXct(data.hour.scheduled$Appt.DTTM, format = "%H:%M")
data.hour.scheduled$Appt.End <- as.POSIXct(data.hour.scheduled$Appt.Start + data.hour.scheduled$Appt.Dur*60, format = "%H:%M")

data.hour.scheduled$Appt.Start.Time <- as.POSIXct(paste0(Sys.Date()," ", format(data.hour.scheduled$Appt.Start, format="%H:%M:%S")))
data.hour.scheduled$Appt.End.Time <- as.POSIXct(paste0(Sys.Date()," ", format(data.hour.scheduled$Appt.End, format="%H:%M:%S")))


data.hour.scheduled$time.interval <- interval(data.hour.scheduled$Appt.Start.Time, data.hour.scheduled$Appt.End.Time)

# Excluding visits without Roomin or Visit End Tines
data.hour.scheduled$`00:00` <- util.function("00:00:00", data.hour.scheduled)
data.hour.scheduled$`01:00` <- util.function("01:00:00", data.hour.scheduled)
data.hour.scheduled$`02:00` <- util.function("02:00:00", data.hour.scheduled)
data.hour.scheduled$`03:00` <- util.function("03:00:00", data.hour.scheduled)
data.hour.scheduled$`04:00` <- util.function("04:00:00", data.hour.scheduled)
data.hour.scheduled$`05:00` <- util.function("05:00:00", data.hour.scheduled)
data.hour.scheduled$`06:00` <- util.function("06:00:00", data.hour.scheduled)
data.hour.scheduled$`07:00` <- util.function("07:00:00", data.hour.scheduled)
data.hour.scheduled$`08:00` <- util.function("08:00:00", data.hour.scheduled)
data.hour.scheduled$`09:00` <- util.function("09:00:00", data.hour.scheduled)
data.hour.scheduled$`10:00` <- util.function("10:00:00", data.hour.scheduled)
data.hour.scheduled$`11:00` <- util.function("11:00:00", data.hour.scheduled)
data.hour.scheduled$`12:00` <- util.function("12:00:00", data.hour.scheduled)
data.hour.scheduled$`13:00` <- util.function("13:00:00", data.hour.scheduled)
data.hour.scheduled$`14:00` <- util.function("14:00:00", data.hour.scheduled)
data.hour.scheduled$`15:00` <- util.function("15:00:00", data.hour.scheduled)
data.hour.scheduled$`16:00` <- util.function("16:00:00", data.hour.scheduled)
data.hour.scheduled$`17:00` <- util.function("17:00:00", data.hour.scheduled)
data.hour.scheduled$`18:00` <- util.function("18:00:00", data.hour.scheduled)
data.hour.scheduled$`19:00` <- util.function("19:00:00", data.hour.scheduled)
data.hour.scheduled$`20:00` <- util.function("20:00:00", data.hour.scheduled)
data.hour.scheduled$`21:00` <- util.function("21:00:00", data.hour.scheduled)
data.hour.scheduled$`22:00` <- util.function("22:00:00", data.hour.scheduled)
data.hour.scheduled$`23:00` <- util.function("23:00:00", data.hour.scheduled)

# Data Validation
# colnames(data.hour.scheduled[89])
data.hour.scheduled$sum <- rowSums(data.hour.scheduled[,which(colnames(data.hour.scheduled)=="00:00"):which(colnames(data.hour.scheduled)=="23:00")])
# data.hour.scheduled$sum <- rowSums(data.hour.scheduled [,66:89])
data.hour.scheduled$actual <- as.numeric(difftime(data.hour.scheduled$Appt.End.Time, data.hour.scheduled$Appt.Start.Time, units = "mins"))
data.hour.scheduled$comparison <- ifelse(data.hour.scheduled$sum ==data.hour.scheduled$actual, 0, 1)
data.hour.scheduled <- data.hour.scheduled %>% filter(comparison == 0)


# Pre-process Utilization by Hour based on Actual Room in to Visit End Times ---------------------------------------------------
data.hour.arrived.all <- scheduled.data %>% filter(Appt.Status == "Arrived")
data.hour.arrived.all$actual.visit.dur <- round(difftime(data.hour.arrived.all$Visitend.DTTM, data.hour.arrived.all$Roomin.DTTM, units = "mins"))

########### Analysis of % of visits with actual visit start and end times ############

data.hour.arrived.all$Appt.Start <- format(strptime(as.ITime(data.hour.arrived.all$Roomin.DTTM), "%H:%M:%S"),'%H:%M:%S')
data.hour.arrived.all$Appt.Start <- as.POSIXct(data.hour.arrived.all$Appt.Start, format = "%H:%M")
data.hour.arrived.all$Appt.End <- as.POSIXct(data.hour.arrived.all$Appt.Start + data.hour.arrived.all$actual.visit.dur, format = "%H:%M")

data.hour.arrived.all$Appt.Start.Time <- data.hour.arrived.all$Appt.Start
data.hour.arrived.all$Appt.End.Time <- data.hour.arrived.all$Appt.End

data.hour.arrived.all$time.interval <- interval(data.hour.arrived.all$Appt.Start.Time, data.hour.arrived.all$Appt.End.Time)

data.hour.arrived <- data.hour.arrived.all
# Excluding visits without Roomin or Visit End Tines
data.hour.arrived$`00:00` <- util.function("00:00:00", data.hour.arrived)
data.hour.arrived$`01:00` <- util.function("01:00:00", data.hour.arrived)
data.hour.arrived$`02:00` <- util.function("02:00:00", data.hour.arrived)
data.hour.arrived$`03:00` <- util.function("03:00:00", data.hour.arrived)
data.hour.arrived$`04:00` <- util.function("04:00:00", data.hour.arrived)
data.hour.arrived$`05:00` <- util.function("05:00:00", data.hour.arrived)
data.hour.arrived$`06:00` <- util.function("06:00:00", data.hour.arrived)
data.hour.arrived$`07:00` <- util.function("07:00:00", data.hour.arrived)
data.hour.arrived$`08:00` <- util.function("08:00:00", data.hour.arrived)
data.hour.arrived$`09:00` <- util.function("09:00:00", data.hour.arrived)
data.hour.arrived$`10:00` <- util.function("10:00:00", data.hour.arrived)
data.hour.arrived$`11:00` <- util.function("11:00:00", data.hour.arrived)
data.hour.arrived$`12:00` <- util.function("12:00:00", data.hour.arrived)
data.hour.arrived$`13:00` <- util.function("13:00:00", data.hour.arrived)
data.hour.arrived$`14:00` <- util.function("14:00:00", data.hour.arrived)
data.hour.arrived$`15:00` <- util.function("15:00:00", data.hour.arrived)
data.hour.arrived$`16:00` <- util.function("16:00:00", data.hour.arrived)
data.hour.arrived$`17:00` <- util.function("17:00:00", data.hour.arrived)
data.hour.arrived$`18:00` <- util.function("18:00:00", data.hour.arrived)
data.hour.arrived$`19:00` <- util.function("19:00:00", data.hour.arrived)
data.hour.arrived$`20:00` <- util.function("20:00:00", data.hour.arrived)
data.hour.arrived$`21:00` <- util.function("21:00:00", data.hour.arrived)
data.hour.arrived$`22:00` <- util.function("22:00:00", data.hour.arrived)
data.hour.arrived$`23:00` <- util.function("23:00:00", data.hour.arrived)

# Data Validation
# colnames(data.hour.arrived[89])
data.hour.arrived$sum <- rowSums(data.hour.arrived[,which(colnames(data.hour.arrived)=="00:00"):which(colnames(data.hour.arrived)=="23:00")])
data.hour.arrived$actual <- as.numeric(difftime(data.hour.arrived$Appt.End.Time, data.hour.arrived$Appt.Start.Time, units = "mins"))
data.hour.arrived$comparison <- ifelse(data.hour.arrived$sum == data.hour.arrived$actual, 0, 1)
data.hour.arrived$comparison[is.na(data.hour.arrived$comparison)] <- "No Data"
data.hour.arrived <- data.hour.arrived %>% filter(comparison != 1)

# Combine Utilization Data
data.hour.scheduled$util.type <- "scheduled"
data.hour.arrived$util.type <- "actual"

timeOptionsHr_filter <- c("07:00","08:00","09:00",
                          "10:00","11:00","12:00","13:00","14:00","15:00","16:00","17:00","18:00","19:00",
                          "20:00") ## Time Range by Hour Filter

utilization.data <- rbind(data.hour.scheduled, data.hour.arrived)
utilization.data <- utilization.data %>%
  select(Campus, Campus.Specialty, Department, Resource, Provider,
         Visit.Method, Appt.Type, Appt.Status,
         Appt.DateYear, Appt.MonthYear, Appt.Year, Appt.Week, Appt.Day, Appt.TM.Hr, holiday, util.type,
         timeOptionsHr_filter, sum, comparison, NPI)



path <- "/nfs/data/Applications/Ambulatory/Data/"
saveRDS(utilization.data, paste0(path, "utilization_data_sql.rds"))