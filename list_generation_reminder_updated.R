# Ensure required packages are available
if (!require(c("dplyr", "redcapAPI", "keyring", "lubridate"))) {
  install.packages(c("dplyr", "redcapAPI", "keyring", "lubridate"), repos="http://cran.rstudio.com/")
  library(dplyr)
  library(redcapAPI)
  library(keyring)
  library(lubridate)
}

# Import REDCap data for Follow-Up surveys via API
api_url <- 'https://redcap.iths.org/api/'
api_token1 <- key_get('star_index_fu')
api_token2 <- key_get('star_cc1_fu')
api_token3 <- key_get('star_cc2_fu')
api_token <- key_get('star_enroll_cc')

project1 <- redcapAPI:::redcapConnection(url = api_url, token = api_token1)
project2 <- redcapAPI:::redcapConnection(url = api_url, token = api_token2)
project3 <- redcapAPI:::redcapConnection(url = api_url, token = api_token3)
project <- redcapAPI:::redcapConnection(url = api_url, token = api_token)

df_index <- exportRecords(project1,
                      factors = TRUE,
                      fields = c("fu_id", "ppt_tel_index", "ppt_init_index", "date_index_fu"),
                      forms = NULL,
                      records = NULL,
                      events = NULL,
                      labels = FALSE,
                      dates = FALSE,
                      survey = TRUE,
                      dag = TRUE,
                      checkboxLabels = FALSE)
colnames(df_index)[5] = "ppt_tel" # rename id.var to join later
df_index$ppt_tel <- as.character(df_index$ppt_tel)
df_index$tel_len <- nchar(df_index$ppt_tel)
df_index$ppt_tel2 <- ifelse(df_index$tel_len==11, paste("55", df_index$ppt_tel, sep = ""), NA)
df_index$ppt_tel2 <- ifelse(df_index$tel_len==13, df_index$ppt_tel, df_index$ppt_tel2)
df_index$tel_qa <- substring(df_index$ppt_tel, 3,3)
df_index$tel_ac <- substring(df_index$ppt_tel, 1,2)
df_index$tel_indicator <- ifelse(df_index$tel_len<=9 | df_index$tel_len==12 | df_index$tel_len>13, 1, NA)
df_index$ppt_tel2 <- ifelse(df_index$tel_len==10, paste(df_index$tel_ac, substr(df_index$ppt_tel, 3, 10), sep = ""), df_index$ppt_tel2)
df_index$ppt_tel <- df_index$ppt_tel2

df_cc1 <- exportRecords(project2,
                          factors = TRUE,
                          fields = c("fu_id", "ppt_tel_cc1", "ppt_init_cc1", "date_cc1"),
                          forms = NULL,
                          records = NULL,
                          events = NULL,
                          labels = FALSE,
                          dates = FALSE,
                          survey = TRUE,
                          dag = TRUE,
                          checkboxLabels = FALSE)
colnames(df_cc1)[5] = "ppt_tel" # rename id.var to join later
df_cc1$ppt_tel <- as.character(df_cc1$ppt_tel)
df_cc1$tel_len <- nchar(df_cc1$ppt_tel)
df_cc1$ppt_tel2 <- ifelse(df_cc1$tel_len==11, paste("55", df_cc1$ppt_tel, sep = ""), NA)
df_cc1$ppt_tel2 <- ifelse(df_cc1$tel_len==13, df_cc1$ppt_tel, df_cc1$ppt_tel2)
df_cc1$tel_qa <- substring(df_cc1$ppt_tel, 3,3)
df_cc1$tel_ac <- substring(df_cc1$ppt_tel, 1,2)
df_cc1$tel_indicator <- ifelse(df_cc1$tel_len<=9 | df_cc1$tel_len==12 | df_cc1$tel_len>13, 1, NA)
df_cc1$ppt_tel2 <- ifelse(df_cc1$tel_len==10, paste(df_cc1$tel_ac, substr(df_cc1$ppt_tel, 3, 10), sep = ""), df_cc1$ppt_tel2)
df_cc1$ppt_tel <- df_cc1$ppt_tel2

df_cc2 <- exportRecords(project3,
                        factors = TRUE,
                        fields = c("fu_id", "ppt_tel_cc2", "ppt_init_cc2", "date_cc2"),
                        forms = NULL,
                        records = NULL,
                        events = NULL,
                        labels = FALSE,
                        dates = FALSE,
                        survey = TRUE,
                        dag = TRUE,
                        checkboxLabels = FALSE)
colnames(df_cc2)[5] = "ppt_tel" # rename id.var to join later
df_cc2$ppt_tel <- as.character(df_cc2$ppt_tel)
df_cc2$tel_len <- nchar(df_cc2$ppt_tel)
df_cc2$ppt_tel2 <- ifelse(df_cc2$tel_len==11, paste("55", df_cc2$ppt_tel, sep = ""), NA)
df_cc2$ppt_tel2 <- ifelse(df_cc2$tel_len==13, df_cc2$ppt_tel, df_cc2$ppt_tel2)
df_cc2$tel_qa <- substring(df_cc2$ppt_tel, 3,3)
df_cc2$tel_ac <- substring(df_cc2$ppt_tel, 1,2)
df_cc2$tel_indicator <- ifelse(df_cc2$tel_len<=9 | df_cc2$tel_len==12 | df_cc2$tel_len>13, 1, NA)
df_cc2$ppt_tel2 <- ifelse(df_cc2$tel_len==10, paste(df_cc2$tel_ac, substr(df_cc2$ppt_tel, 3, 10), sep = ""), df_cc2$ppt_tel2)
df_cc2$ppt_tel <- df_cc2$ppt_tel2

df <- exportRecords(project,
                    factors = TRUE,
                    fields = NULL,
                    forms = NULL,
                    records = NULL,
                    events = NULL,
                    labels = FALSE,
                    dates = FALSE,
                    survey = TRUE,
                    dag = TRUE,
                    checkboxLabels = FALSE)
colnames(df)[65] = "ppt_tel" #rename id.var to join later

# Downsize df, create vars, subset to active participants
df <- df %>% select("enroll_date", "ppt_id", "ppt_tel", "ppt_init", "enroll_type", "ppt_arm")
df$ppt_tel <- as.character(df$ppt_tel)
df$ppt_init <- toupper(df$ppt_init)
df$enroll_date <- as.Date(df$enroll_date)
df$last_day <- df$enroll_date + 10
# df <- df %>%
#   group_by(ppt_id) %>%
#   mutate(max_instance = max(redcap_repeat_instance, na.rm = TRUE)) %>%
#   ungroup(ppt_id)
# df$max_instance <- ifelse(df$max_instance==-Inf, NA, df$max_instance)
df <- subset(df, enroll_date != as.Date(with_tz(Sys.Date(), tzone = "America/Sao_Paulo")))
df$message_indicator <- ifelse(df$last_day >= as.Date(with_tz(Sys.Date(), tzone = "America/Sao_Paulo")), 1, 0)
activedf <- subset(df, message_indicator==1)
activedf <- subset(activedf, !is.na(ppt_tel))

# Create list of participants who have not yet completed the survey for each arm
## Index Cases
df_index <- left_join(activedf, df_index, by = "ppt_tel")
df_index$ppt_init_index <- toupper(df_index$ppt_init_index)
df_index$date_index_fu <- as.Date(df_index$date_index_fu)
df_index <- subset(df_index, enroll_type=="Index Case")
df_index$mismatch_indicator <- ifelse(df_index$ppt_init!=df_index$ppt_init_index, 1, NA)
df_index <- df_index %>%
  group_by(ppt_id) %>%
  mutate(is_max_day = ifelse(max(date_index_fu)==date_index_fu, 1, 0)) %>%
  mutate(reminder_indicator = ifelse(date_index_fu == as.Date(with_tz(Sys.Date(), tzone = "America/Sao_Paulo")) & is_max_day==1, 0, NA)) %>%
  ungroup(ppt_id)
df_index$reminder_indicator <- ifelse(is.na(df_index$date_index_fu), 1, df_index$reminder_indicator)
df_index$reminder_indicator <- ifelse(is.na(df_index$reminder_indicator) & df_index$is_max_day==1, 1, df_index$reminder_indicator)
df_index$study_day <- as.integer(df_index$date_index_fu - df_index$enroll_date)
df_index$current_study_day <- as.integer(as.Date(with_tz(Sys.Date(), tzone = "America/Sao_Paulo")) - df_index$enroll_date)
# df_index <- df_index %>% 
#   group_by(ppt_id) %>%
#   mutate(is_max_day = ifelse(study_day==max(study_day), 1, 0)) %>%
#   mutate(max_instance = max(redcap_repeat_instance)) %>%
#   ungroup(ppt_id)
# df_index$redcap_repeat_instance <- ifelse(is.na(df_index$max_instance), 1, df_index$max_instance + 1)

mdf_index <- subset(df_index, reminder_indicator==1)
mdf_index$ppt_tel <- as.numeric(mdf_index$ppt_tel)
to_message_i <- mdf_index %>% select(ppt_tel)

## Close Contacts (Control)
df_cc1 <- left_join(activedf, df_cc1, by = "ppt_tel")
df_cc1$ppt_init_cc1 <- toupper(df_cc1$ppt_init_cc1)
df_cc1$date_cc1 <- as.Date(df_cc1$date_cc1)
df_cc1 <- subset(df_cc1, enroll_type=="Close Contact" & ppt_arm=="Arm 1: Control")
df_cc1$mismatch_indicator <- ifelse(df_cc1$ppt_init!=df_cc1$ppt_init_cc1, 1, NA)
df_cc1 <- df_cc1 %>%
  group_by(ppt_id) %>%
  mutate(is_max_day = ifelse(max(date_cc1)==date_cc1, 1, 0)) %>%
  mutate(reminder_indicator = ifelse(date_cc1 == as.Date(with_tz(Sys.Date(), tzone = "America/Sao_Paulo")) & is_max_day==1, 0, NA)) %>%
  ungroup(ppt_id)
df_cc1$reminder_indicator <- ifelse(is.na(df_cc1$date_cc1), 1, df_cc1$reminder_indicator)
df_cc1$reminder_indicator <- ifelse(is.na(df_cc1$reminder_indicator) & df_cc1$is_max_day==1, 1, df_cc1$reminder_indicator)
df_cc1$study_day <- as.integer(df_cc1$date_cc1 - df_cc1$enroll_date)
df_cc1$current_study_day <- as.integer(as.Date(with_tz(Sys.Date(), tzone = "America/Sao_Paulo")) - df_cc1$enroll_date)
# df_cc1 <- df_cc1 %>% 
#   group_by(ppt_id) %>%
#   mutate(is_max_day = ifelse(study_day==max(study_day), 1, 0)) %>%
#   mutate(max_instance = max(redcap_repeat_instance)) %>%
#   ungroup(ppt_id)
# df_cc1$redcap_repeat_instance <- ifelse(is.na(df_cc1$max_instance), 1, df_cc1$max_instance + 1)

mdf_cc1 <- subset(df_cc1, reminder_indicator==1)
mdf_cc1$ppt_tel <- as.numeric(mdf_cc1$ppt_tel)
to_message_cc1 <- mdf_cc1 %>% select(ppt_tel)

## Close Contacts (Intervention)
df_cc2 <- left_join(activedf, df_cc2, by = "ppt_tel")
df_cc2$ppt_init_cc2 <- toupper(df_cc2$ppt_init_cc2)
df_cc2$date_cc2 <- as.Date(df_cc2$date_cc2)
df_cc2 <- subset(df_cc2, enroll_type=="Close Contact" & ppt_arm=="Arm 2: Treatment")
df_cc2$mismatch_indicator <- ifelse(df_cc2$ppt_init!=df_cc2$ppt_init_cc2, 1, NA)
df_cc2 <- df_cc2 %>%
  group_by(ppt_id) %>%
  mutate(is_max_day = ifelse(max(date_cc2)==date_cc2, 1, 0)) %>%
  mutate(reminder_indicator = ifelse(date_cc2 == as.Date(with_tz(Sys.Date(), tzone = "America/Sao_Paulo")) & is_max_day==1, 0, NA)) %>%
  ungroup(ppt_id)
df_cc2$reminder_indicator <- ifelse(is.na(df_cc2$date_cc2), 1, df_cc2$reminder_indicator)
df_cc2$reminder_indicator <- ifelse(is.na(df_cc2$reminder_indicator) & df_cc2$is_max_day==1, 1, df_cc2$reminder_indicator)
df_cc2$study_day <- as.integer(df_cc2$date_cc2 - df_cc2$enroll_date)
df_cc2$current_study_day <- as.integer(as.Date(with_tz(Sys.Date(), tzone = "America/Sao_Paulo")) - df_cc2$enroll_date)
# df_cc2 <- df_cc2 %>% 
#   group_by(ppt_id) %>%
#   mutate(is_max_day = ifelse(study_day==max(study_day), 1, 0)) %>%
#   mutate(max_instance = max(redcap_repeat_instance)) %>%
#   ungroup(ppt_id)
# df_cc2$redcap_repeat_instance <- ifelse(is.na(df_cc2$max_instance), 1, df_cc2$max_instance + 1)

mdf_cc2 <- subset(df_cc2, reminder_indicator==1)
mdf_cc2$ppt_tel <- as.numeric(mdf_cc2$ppt_tel)
to_message_cc2 <- mdf_cc2 %>% select(ppt_tel)

# Write list of numbers to message today
write.table(to_message_i, file = "C:/Users/rgreen/PycharmProjects/SendWhatsApp/index_reminder_list.csv", sep=",",  row.names = FALSE, col.names=FALSE)
write.table(to_message_cc1, file = "C:/Users/rgreen/PycharmProjects/SendWhatsApp/cc1_reminder_list.csv", sep=",",  row.names = FALSE, col.names=FALSE)
write.table(to_message_cc2, file = "C:/Users/rgreen/PycharmProjects/SendWhatsApp/cc2_reminder_list.csv", sep=",",  row.names = FALSE, col.names=FALSE)

# Create df of mismatches to upload to REDCap
# df_index_mismatch <- subset(df_index, is_max_day==1 | is.na(is_max_day)) %>% select(ppt_id, tel_indicator, mismatch_indicator, redcap_repeat_instrument, redcap_repeat_instance, update_date, fu_id)
# df_cc1_mismatch <- subset(df_cc1, is_max_day==1) %>% select(ppt_id, tel_indicator, mismatch_indicator, redcap_repeat_instrument, redcap_repeat_instance, update_date, fu_id)
# df_cc2_mismatch <- subset(df_cc2, is_max_day==1) %>% select(ppt_id, tel_indicator, mismatch_indicator, redcap_repeat_instrument, redcap_repeat_instance, update_date, fu_id)
# df_mismatch <- rbind(df_index_mismatch, df_cc1_mismatch, df_cc2_mismatch)
# df_mismatch$redcap_repeat_instrument <- 'miscellaneous_participant_followup'
# df_mismatch$update_date <- Sys.Date()
# 
# 
# importRecords(project, df_mismatch, overwriteBehavior = "normal", returnContent = "count", returnData = FALSE, logfile = "C:/Users/rgreen/PycharmProjects/SendWhatsApp/redcap_import_logfile.txt", batch.size = -1)
