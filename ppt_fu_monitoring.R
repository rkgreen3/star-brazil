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

df_index1 <- exportRecords(project1,
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
#colnames(df_index1)[5] = "ppt_tel" # rename id.var to join later
df_index1$ppt_tel_index <- as.character(df_index1$ppt_tel_index)
df_index1$tel_len <- nchar(df_index1$ppt_tel_index)
df_index1$ppt_tel2 <- ifelse(df_index1$tel_len==11, paste("55", df_index1$ppt_tel_index, sep = ""), NA)
df_index1$ppt_tel2 <- ifelse(df_index1$tel_len==13, df_index1$ppt_tel_index, df_index1$ppt_tel2)
df_index1$tel_qa <- substring(df_index1$ppt_tel_index, 3,3)
df_index1$tel_ac <- substring(df_index1$ppt_tel_index, 1,2)
df_index1$tel_indicator <- ifelse(df_index1$tel_len<=9 | df_index1$tel_len==12 | df_index1$tel_len>13, 1, NA)
df_index1$ppt_tel2 <- ifelse(df_index1$tel_len==10, paste("55", df_index1$tel_ac, substr(df_index1$ppt_tel_index, 3, 10), sep = ""), df_index1$ppt_tel2)
df_index1$ppt_tel_index <- df_index1$ppt_tel2


df_cc1a <- exportRecords(project2,
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
#colnames(df_cc1a)[5] = "ppt_tel" # rename id.var to join later
df_cc1a$ppt_tel_cc1 <- as.character(df_cc1a$ppt_tel_cc1)
df_cc1a$tel_len <- nchar(df_cc1a$ppt_tel_cc1)
df_cc1a$ppt_tel2 <- ifelse(df_cc1a$tel_len==11, paste("55", df_cc1a$ppt_tel_cc1, sep = ""), NA)
df_cc1a$ppt_tel2 <- ifelse(df_cc1a$tel_len==13, df_cc1a$ppt_tel_cc1, df_cc1a$ppt_tel2)
df_cc1a$tel_qa <- substring(df_cc1a$ppt_tel_cc1, 3,3)
df_cc1a$tel_ac <- substring(df_cc1a$ppt_tel_cc1, 1,2)
df_cc1a$tel_indicator <- ifelse(df_cc1a$tel_len<=9 | df_cc1a$tel_len==12 | df_cc1a$tel_len>13, 1, NA)
df_cc1a$ppt_tel2 <- ifelse(df_cc1a$tel_len==10, paste(df_cc1a$tel_ac, substr(df_cc1a$ppt_tel_cc1, 3, 10), sep = ""), df_cc1a$ppt_tel2)
df_cc1a$ppt_tel_cc1 <- df_cc1a$ppt_tel2

df_cc2a <- exportRecords(project3,
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
#colnames(df_cc2a)[5] = "ppt_tel" # rename id.var to join later
df_cc2a$ppt_tel_cc2 <- as.character(df_cc2a$ppt_tel_cc2)
df_cc2a$tel_len <- nchar(df_cc2a$ppt_tel_cc2)
df_cc2a$ppt_tel2 <- ifelse(df_cc2a$tel_len==11, paste("55", df_cc2a$ppt_tel_cc2, sep = ""), NA)
df_cc2a$ppt_tel2 <- ifelse(df_cc2a$tel_len==13, df_cc2a$ppt_tel_cc2, df_cc2a$ppt_tel2)
df_cc2a$tel_qa <- substring(df_cc2a$ppt_tel_cc2, 3,3)
df_cc2a$tel_ac <- substring(df_cc2a$ppt_tel_cc2, 1,2)
df_cc2a$tel_indicator <- ifelse(df_cc2a$tel_len<=9 | df_cc2a$tel_len==12 | df_cc2a$tel_len>13, 1, NA)
df_cc2a$ppt_tel2 <- ifelse(df_cc2a$tel_len==10, paste(df_cc2a$tel_ac, substr(df_cc2a$ppt_tel_cc2, 3, 10), sep = ""), df_cc2a$ppt_tel2)
df_cc2a$ppt_tel_cc2 <- df_cc2a$ppt_tel2

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

# Downsize df, create vars, subset to active participants; join in f/u data
df <- df %>% select("enroll_date", "ppt_id", "ppt_tel", "dob", "enroll_type", "ppt_arm", "redcap_repeat_instrument", "redcap_repeat_instance", "update_date", "comments", "fu_min_completed_yn")
df$ppt_tel <- as.character(df$ppt_tel)
df$dob <- as.Date(df$dob)
#df$ppt_init <- toupper(df$ppt_init)
df$enroll_date <- as.Date(df$enroll_date)
df$last_day <- df$enroll_date + 10
df <- df %>%
  group_by(ppt_id) %>%
  mutate(max_instance = max(redcap_repeat_instance, na.rm = TRUE)) %>%
  ungroup(ppt_id)
df$max_instance <- ifelse(df$max_instance==-Inf, NA, df$max_instance)
activedf <- subset(df, enroll_date != as.Date(with_tz(Sys.Date(), tzone = "America/Sao_Paulo")))
activedf <- subset(activedf, last_day >= as.Date(with_tz(Sys.Date(), tzone = "America/Sao_Paulo")))

# Index
df_index1$dob_index <- as.Date(df_index1$dob_index)
df_index1$dob_index2 <- df_index1$dob_index
df_index <- left_join(activedf, df_index1, by = c("ppt_tel" = "ppt_tel_index", "dob" = "dob_index"))
df_index$is_active <- ifelse(df_index$last_day >= as.Date(with_tz(Sys.Date(), tzone = "America/Sao_Paulo")), 1, 0)
df_index <- subset(df_index, is_active==1)
#df_index$ppt_init_index <- toupper(df_index$ppt_init_index)
df_index$date_index_fu <- as.Date(df_index$date_index_fu)
df_index <- subset(df_index, enroll_type=="Index Case")
df_index$study_day <- as.integer(df_index$date_index_fu - df_index$enroll_date)
df_index$current_study_day <- as.integer(as.Date(with_tz(Sys.Date(), tzone = "America/Sao_Paulo")) - df_index$enroll_date)
df_index <- df_index %>% 
  group_by(ppt_id) %>%
  mutate(dup_indicator = duplicated(study_day)) %>%
  mutate(is_max_day = ifelse(study_day==max(study_day), 1, 0)) %>%
  ungroup(ppt_id)
df_index$dup_indicator <- ifelse(df_index$dup_indicator==TRUE, 1, NA)
df_index$lag_indicator <- ifelse(df_index$is_max_day==1 & (df_index$current_study_day-df_index$study_day>=2), 1, NA)
df_index$mismatch_indicator <- ifelse(df_index$dob!=df_index$dob_index2, 1, 0)
df_index$redcap_repeat_instance <- ifelse(is.na(df_index$max_instance), 1, df_index$max_instance + 1)
df_index$comments <- df_index$comments_index_fu
df_index$comment_indicator <- ifelse(grepl(pattern = "nÃo", x = df_index$comments, ignore.case = T)==TRUE, 0, 1)
df_index$comment_indicator <- ifelse(grepl(pattern = "nao", x = df_index$comments, ignore.case = T)==TRUE, 0, df_index$comment_indicator)
df_index$comment_indicator <- ifelse(is.na(df_index$comments), 0, df_index$comment_indicator)
df_index$comment_indicator <- ifelse(nchar(df_index$comments)<4, 0, df_index$comment_indicator)


df_index_monitor <- subset(df_index, is_max_day==1)
df_index_monitor <- df_index_monitor %>% select(ppt_id, studyteam_fu_yn_index_fu, mismatch_indicator, tel_indicator, dup_indicator, lag_indicator, comment_indicator, redcap_repeat_instrument, redcap_repeat_instance, update_date, fu_id, comments)
colnames(df_index_monitor)[2] = "fu_indicator"
df_index_monitor$update_date <- Sys.Date()

# CC1
df_cc1a$cc1_dob <- as.Date(df_cc1a$cc1_dob)
df_cc1a$cc1_dob2 <- df_cc1a$cc1_dob
df_cc1 <- left_join(activedf, df_cc1a, by = c("ppt_tel" = "ppt_tel_cc1", "dob" = "cc1_dob"))
df_cc1$is_active <- ifelse(df_cc1$last_day >= as.Date(with_tz(Sys.Date(), tzone = "America/Sao_Paulo")), 1, 0)
df_cc1 <- subset(df_cc1, is_active==1)
#df_cc1$ppt_init_cc1 <- toupper(df_cc1$ppt_init_cc1)
df_cc1$date_cc1 <- as.Date(df_cc1$date_cc1)
df_cc1 <- subset(df_cc1, enroll_type=="Close Contact" & ppt_arm=="Arm 1: Control")
df_cc1$study_day <- as.integer(df_cc1$date_cc1 - df_cc1$enroll_date)
df_cc1$current_study_day <- as.integer(as.Date(with_tz(Sys.Date(), tzone = "America/Sao_Paulo")) - df_cc1$enroll_date)
df_cc1 <- df_cc1 %>% 
  group_by(ppt_id) %>%
  mutate(dup_indicator = duplicated(study_day)) %>%
  mutate(is_max_day = ifelse(study_day==max(study_day), 1, 0)) %>%
  ungroup(ppt_id)
df_cc1$dup_indicator <- ifelse(df_cc1$dup_indicator==TRUE, 1, NA)
df_cc1$lag_indicator <- ifelse(df_cc1$is_max_day==1 & (df_cc1$current_study_day-df_cc1$study_day>=2), 1, NA)
df_cc1$mismatch_indicator <- ifelse(df_cc1$dob!=df_cc1$cc1_dob2, 1, 0)
df_cc1$redcap_repeat_instance <- ifelse(is.na(df_cc1$max_instance), 1, df_cc1$max_instance + 1)
df_cc1$comments <- df_cc1$comments_cc1
df_cc1$comment_indicator <- ifelse(grepl(pattern = "nÃo", x = df_cc1$comments, ignore.case = T)==TRUE, 0, 1)
df_cc1$comment_indicator <- ifelse(grepl(pattern = "nao", x = df_cc1$comments, ignore.case = T)==TRUE, 0, df_cc1$comment_indicator)
df_cc1$comment_indicator <- ifelse(is.na(df_cc1$comments), 0, df_cc1$comment_indicator)
df_cc1$comment_indicator <- ifelse(nchar(df_cc1$comments)<4, 0, df_cc1$comment_indicator)

df_cc1_monitor <- subset(df_cc1, is_max_day==1)
df_cc1_monitor <- df_cc1_monitor %>% select(ppt_id, studyteam_fu_yn_cc1, mismatch_indicator, tel_indicator, dup_indicator, lag_indicator, comment_indicator, redcap_repeat_instrument, redcap_repeat_instance, update_date, fu_id, comments)
colnames(df_cc1_monitor)[2] = "fu_indicator"
df_cc1_monitor$update_date <- Sys.Date()

# CC2
df_cc2a$cc2_dob <- as.Date(df_cc2a$cc2_dob)
df_cc2a$cc2_dob2 <- df_cc2a$cc2_dob
df_cc2 <- left_join(activedf, df_cc2a, by = c("ppt_tel" = "ppt_tel_cc2", "dob" = "cc2_dob"))
df_cc2$is_active <- ifelse(df_cc2$last_day >= as.Date(with_tz(Sys.Date(), tzone = "America/Sao_Paulo")), 1, 0)
df_cc2 <- subset(df_cc2, is_active==1)
#df_cc2$ppt_init_cc2 <- toupper(df_cc2$ppt_init_cc2)
df_cc2$date_cc2 <- as.Date(df_cc2$date_cc2)
df_cc2 <- subset(df_cc2, enroll_type=="Close Contact" & ppt_arm=="Arm 2: Treatment")
df_cc2$study_day <- as.integer(df_cc2$date_cc2 - df_cc2$enroll_date)
df_cc2$current_study_day <- as.integer(as.Date(with_tz(Sys.Date(), tzone = "America/Sao_Paulo")) - df_cc2$enroll_date)
df_cc2 <- df_cc2 %>% 
  group_by(ppt_id) %>%
  mutate(dup_indicator = duplicated(study_day)) %>%
  mutate(is_max_day = ifelse(study_day==max(study_day), 1, 0)) %>%
  ungroup(ppt_id)
df_cc2$dup_indicator <- ifelse(df_cc2$dup_indicator==TRUE, 1, NA)
df_cc2$lag_indicator <- ifelse(df_cc2$is_max_day==1 & (df_cc2$current_study_day-df_cc2$study_day>=2), 1, NA)
df_cc2$mismatch_indicator <- ifelse(df_cc2$dob!=df_cc2$cc2_dob2, 1, 0)
df_cc2$redcap_repeat_instance <- ifelse(is.na(df_cc2$max_instance), 1, df_cc2$max_instance + 1)
df_cc2$comments <- df_cc2$comments_cc2
df_cc2$comment_indicator <- ifelse(grepl(pattern = "nÃo", x = df_cc2$comments, ignore.case = T)==TRUE, 0, 1)
df_cc2$comment_indicator <- ifelse(grepl(pattern = "nao", x = df_cc2$comments, ignore.case = T)==TRUE, 0, df_cc2$comment_indicator)
df_cc2$comment_indicator <- ifelse(is.na(df_cc2$comments), 0, df_cc2$comment_indicator)
df_cc2$comment_indicator <- ifelse(nchar(df_cc2$comments)<4, 0, df_cc2$comment_indicator)

df_cc2_monitor <- subset(df_cc2, is_max_day==1)
df_cc2_monitor <- df_cc2_monitor %>% select(ppt_id, studyteam_fu_yn_cc2, mismatch_indicator, tel_indicator, dup_indicator, lag_indicator, comment_indicator, redcap_repeat_instrument, redcap_repeat_instance, update_date, fu_id, comments)
colnames(df_cc2_monitor)[2] = "fu_indicator"
df_cc2_monitor$update_date <- Sys.Date()

df_monitor <- rbind(df_index_monitor, df_cc1_monitor, df_cc2_monitor)
df_monitor$needs_update <- ifelse(df_monitor$mismatch_indicator==1 | df_monitor$tel_indicator==1 | df_monitor$dup_indicator==1 | df_monitor$lag_indicator==1 | df_monitor$fu_indicator==1 | df_monitor$comment_indicator==1, 1, 0)
df_monitor <- subset(df_monitor, needs_update==1)
df_monitor <- subset(df_monitor, select = -c(needs_update))
df_monitor$fu_id <- as.numeric(df_monitor$fu_id)
df_monitor <- df_monitor %>%
  group_by(ppt_id) %>%
  mutate(fu_id = max(fu_id)) %>%
  ungroup(ppt_id)
df_monitor$mismatch_indicator <- ifelse(is.na(df_monitor$mismatch_indicator), 0, df_monitor$mismatch_indicator)
df_monitor$tel_indicator <- ifelse(is.na(df_monitor$tel_indicator), 0, df_monitor$tel_indicator)
df_monitor$dup_indicator <- ifelse(is.na(df_monitor$dup_indicator), 0, df_monitor$dup_indicator)
df_monitor$lag_indicator <- ifelse(is.na(df_monitor$lag_indicator), 0, df_monitor$lag_indicator)
df_monitor$comment_indicator <- ifelse(is.na(df_monitor$comment_indicator), 0, df_monitor$comment_indicator)
df_monitor$redcap_repeat_instrument <- 'miscellaneous_participant_followup'

importRecords(project, df_monitor, overwriteBehavior = "normal", returnContent = "count", returnData = FALSE, logfile = "C:/Users/rgreen/PycharmProjects/SendWhatsApp/redcap_import_logfile.txt", batch.size = -1)

# GPS Eligibility
# inactivedf <- subset(df, last_day < Sys.Date())
# # Number of Responses Completed per Participant
# df_index1 <- left_join(inactivedf, df_index1, by = c("ppt_tel" = "ppt_tel_index", "dob" = "dob_index"))
# df_index1$is_active <- ifelse(df_index1$last_day >= as.Date(with_tz(Sys.Date(), tzone = "America/Sao_Paulo")), 1, 0)
# df_index1 <- subset(df_index1, is_active==0)
# df_index1$date_index_fu <- as.Date(df_index1$date_index_fu)
# df_index1 <- subset(df_index1, enroll_type=="Index Case")
# df_index1 <- df_index1 %>% select(ppt_id, ppt_tel, dob, enroll_date, fu_id, date_index_fu, index_case_followup_survey_timestamp)
# df_index1 <- subset(df_index1, !is.na(index_case_followup_survey_timestamp))
# names(df_index1) <- c("ppt_id", "enroll_date", "survey_date", "survey_timestamp")
# 
# df_cc1a <- left_join(inactivedf, df_cc1a, by = c("ppt_tel" = "ppt_tel_cc1", "dob" = "cc1_dob"))
# df_cc1a$is_active <- ifelse(df_cc1a$last_day >= as.Date(with_tz(Sys.Date(), tzone = "America/Sao_Paulo")), 1, 0)
# df_cc1a <- subset(df_cc1a, is_active==0)
# df_cc1a$date_cc1 <- as.Date(df_cc1a$date_cc1)
# df_cc1a <- subset(df_cc1a, enroll_type=="Close Contact" & ppt_arm=="Arm 1: Control")
# df_cc1a <- df_cc1a %>% select(ppt_id, enroll_date, date_cc1, close_contact_followup_survey_control_timestamp)
# df_cc1a <- subset(df_cc1a, !is.na(close_contact_followup_survey_control_timestamp))
# names(df_cc1a) <- c("ppt_id", "enroll_date", "survey_date", "survey_timestamp")
# 
# df_cc2a <- left_join(inactivedf, df_cc2a, by = c("ppt_tel" = "ppt_tel_cc2", "dob" = "cc2_dob"))
# df_cc2a$is_active <- ifelse(df_cc2a$last_day >= as.Date(with_tz(Sys.Date(), tzone = "America/Sao_Paulo")), 1, 0)
# df_cc2a <- subset(df_cc2a, is_active==0)
# df_cc2a$date_cc1 <- as.Date(df_cc2a$date_cc2)
# df_cc2a <- subset(df_cc2a, enroll_type=="Close Contact" & ppt_arm=="Arm 2: Treatment")
# df_cc2a <- df_cc2a %>% select(ppt_id, enroll_date, date_cc2, close_contact_followup_survey_intervention_timestamp)
# df_cc2a <- subset(df_cc2a, !is.na(close_contact_followup_survey_intervention_timestamp))
# names(df_cc2a) <- c("ppt_id", "enroll_date", "survey_date", "survey_timestamp")
# 
# df_followup <- rbind(df_index1, df_cc1a, df_cc2a)
# dff <- df_followup %>% 
#   group_by(ppt_id) %>%
#   dplyr::summarise(n = n())
# dff$fu_min_completed_yn <- ifelse(dff$n>2, 1, 0)
# dff <- dff %>% select(ppt_id, fu_min_completed_yn)
# 
# importRecords(project, dff, overwriteBehavior = "normal", returnContent = "count", returnData = FALSE, logfile = "C:/Users/rgreen/PycharmProjects/SendWhatsApp/redcap_import_logfile2.txt", batch.size = -1)
