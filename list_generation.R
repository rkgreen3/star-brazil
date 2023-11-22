# Ensure required packages are available
if (!require(c("dplyr", "redcapAPI", "lubridate"))) {
  install.packages(c("dplyr", "redcapAPI", "lubridate"), repos="http://cran.rstudio.com/")
  library(dplyr)
  library(redcapAPI)
  library(keyring)
  library(lubridate)
}

# Import REDCap data via API
#key_set('star_enroll_cc')
api_token <- 'insert token here'
api_url <- 'https://redcap.iths.org/api/'
project <- redcapAPI:::redcapConnection(url = api_url, token = api_token)

df <- exportRecords(project,
                      factors = TRUE,
                      fields = NULL,
                      forms = 'enrollment_visit',
                      records = NULL,
                      events = NULL,
                      labels = FALSE,
                      dates = FALSE,
                      survey = TRUE,
                      dag = TRUE,
                      checkboxLabels = FALSE)

# Downsize df, create vars, subset to active participants
df <- df %>% select("enroll_date", "ppt_id", "info_text", "enroll_type", "ppt_arm")
df$enroll_date <- as.Date(df$enroll_date)
df$last_day <- df$enroll_date + 10
df <- subset(df, enroll_date != as.Date(with_tz(Sys.Date(), tzone = "America/Sao_Paulo")))
df$message_indicator <- ifelse(df$last_day >= as.Date(with_tz(Sys.Date(), tzone = "America/Sao_Paulo")), 1, 0)
activedf <- subset(df, message_indicator==1)

# Create list of index cases to message
activedf_i <- subset(activedf, enroll_type=="Index Case")
activedf_i <- subset(activedf_i, !is.na(info_text))
to_message_i <- activedf_i %>% select("info_text")

# Create list of close contacts to message by arm
activedf_cc1 <- subset(activedf, enroll_type=="Close Contact" & ppt_arm=="Arm 1: Control")
activedf_cc1 <- subset(activedf_cc1, !is.na(info_text))
to_message_cc1 <- activedf_cc1 %>% select("info_text")
activedf_cc2 <- subset(activedf, enroll_type=="Close Contact" & ppt_arm=="Arm 2: Treatment")
activedf_cc2 <- subset(activedf_cc2, !is.na(info_text))
to_message_cc2 <- activedf_cc2 %>% select("info_text")

# Write list of numbers to message today
write.table(to_message_i, file = "C:/Users/rgreen/PycharmProjects/SendWhatsApp/index_daily_list.csv", sep=",",  row.names = FALSE, col.names=FALSE)
write.table(to_message_cc1, file = "C:/Users/rgreen/PycharmProjects/SendWhatsApp/cc1_daily_list.csv", sep=",",  row.names = FALSE, col.names=FALSE)
write.table(to_message_cc2, file = "C:/Users/rgreen/PycharmProjects/SendWhatsApp/cc2_daily_list.csv", sep=",",  row.names = FALSE, col.names=FALSE)
