# Goal: Apply fixdates script to the whole iNat parquet, keeping things light
# (only unique row ID, then new date, and whether ambiguous). From this,
# the whole iNat parquet can be constructed, adding back in the columns per 
# Laura's idea

# By: Ryan Hull
# Quantitative Biodiversity Lab, McGill University
# Date: July 2025

# Section 1: Loading in data
library(ggplot2)
library(dplyr)
library(arrow)
library(dbplyr, warn.conflicts = FALSE)
library(duckdb)
library(stringr)

# Load limited columns of the full inat parquet
rm(list=ls())
inat_pq <- arrow::open_dataset("C:/Users/Dell/OneDrive - McGill University/Laura's Lab_Group - Blitz the Gap/iNaturalist Canada parquet/iNat_non_sensitive_data_Jan2025.parquet")

obs <- inat_pq |>
  select(c(id, observed_on_string)) |>
  collect()

obs1 <- obs[1:1000000, ]
obs2 <- obs[1000001:2000000, ]
obs3 <- obs[2000001:3000000, ]
obs4 <- obs[3000001:4000000, ]
obs5 <- obs[4000001:5000000, ]
obs6 <- obs[5000001:6000000, ]
obs7 <- obs[6000001:7000000, ]
obs8 <- obs[7000001:8000000, ]
obs9 <- obs[8000001:9000000, ]
obs10 <- obs[9000001:10000000, ]
obs11 <- obs[10000001:11000000, ]
obs12 <- obs[11000001:12000000, ]
obs13 <- obs[12000001:13000000, ]
obs14 <- obs[13000001:14000000, ]
obs15 <- obs[14000001:15000000, ]
obs16 <- obs[15000001:16000000, ]
obs17 <- obs[16000001:17000000, ]
obs18 <- obs[17000001:nrow(obs), ]




# from other r file: do this for all 18, using search and replace, searching for \bobs\b with regex checked
# many lines commented out for speed sake



######################### 1 ##########################
# Section 2: Extracting the year
#2.1 first pass - grep any set of 4 numbers
obs1$year4num <- regmatches(obs1$observed_on_string, gregexpr("\\d{4}", obs1$observed_on_string))
obs1$year <- obs1$year4num
obs1$year <- sapply(obs1$year4num, function(x){ ifelse(length(x)==0,NA,x[1])}) # to NA the ones with no year
#length(which(is.na(obs1$year))) # about 95% of these cause theres no entry at all. 5% have an entry without a year

#2.2 second pass - correct those with multiple entries of 4 numbers
twos_indx <- which(sapply(obs1$year4num, length) == 2)

year_only <- lapply(obs1$year4num[twos_indx], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 1800 & x_num < 2026] # the plausible 4digit is used
})
obs1$year[twos_indx] <- year_only

#2.3 Any more still left? Then refine date inclusion criteria
twos_indx_still <- which(sapply(obs1$year, length) == 2)
obs1$observed_on_string[twos_indx_still] # displays the dates

# All years 2015 and 2016 with manual check, so use >2000. Change this if need be for different dfs
year_only <- lapply(obs1$year[twos_indx_still], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 2000]
})
obs1$year[twos_indx_still] <- year_only

# 2.4 Final check - any left? If so manually edit your df
#length(which(sapply(obs1$year, length) == 2)) # if empty, no further checks needed
obs1$year4num <- NULL




# Section 3 - Extracting the Month and dates when month listed as letters

# create a lookup table
month_lookup <- c(
  Jan=1, January=1, janv=1, janvier=1, jan=1, january=1,
  Feb=2, February=2, Fév=2, fév=2, fevrier=2, février=2, feb=2, february=2,
  Mar=3, March=3, mars=3, mar=3, march=3,
  Apr=4, April=4, avr=4, avril=4, apr=4, april=4,
  May=5, mai=5, may=5,
  Jun=6, June=6, juin=6, jun=6, june=6,
  Jul=7, July=7, juil=7, juillet=7, jul=7, july=7,
  Aug=8, August=8, aout=8, août=8, aug=8, august=8,
  Sep=9, Sept=9, September=9, sept=9, septembre=9, sep=9, september=9,
  Oct=10, October=10, oct=10, octobre=10, oct=10, october=10,
  Nov=11, November=11, nov=11, novembre=11, nov=11, november=11,
  Dec=12, December=12, déc=12, dec=12, décembre=12, dec=12, december=12
)

# create regex and use str_extract to take out the month
months_alphabetic_regex <- paste0("\\b(", paste(names(month_lookup), collapse="|"), ")\\b")
obs1$just_month_when_alphabetic <- str_extract(obs1$observed_on_string, months_alphabetic_regex)
#nb_alhpabetic <- length(which(!is.na(obs1$just_month_when_alphabetic))) # roughly 15% of cases

# assigning it to new column
obs1$month_numeric_when_alphabetic <- sapply(obs1$just_month_when_alphabetic, function(x){
  if (!is.na(x)){
    month_lookup[[x]]
  }
  else{
    NA
  }
})

# Finding the dates from these rows - should be the only " XX " in the string
obs1$day_when_month_alphabetical <- NA
rows_with_alphabetical_month <- !is.na(obs1$month_numeric_when_alphabetic)
positions_alpha_day <- regexec("\\s(\\d{2})\\s", obs1$observed_on_string[rows_with_alphabetical_month])
day_when_alpha <- regmatches(obs1$observed_on_string[rows_with_alphabetical_month], positions_alpha_day)
obs1$day_when_month_alphabetical[rows_with_alphabetical_month] <- 
  sapply(day_when_alpha, function(x) x[2])

#looking for errors
#length(which(sapply(obs1$day_when_month_alphabetical, length)> 1)) # any double dates? should be none
#length(which(obs1$day_when_month_alphabetical > 31))# any false dates? should be none


# Section 4: Extracting months and dates when listed as yyyy-m?d?-m?d?
# Issue: many formats (yyyy-mm-dd, yyyy-dd-mm, yyyy-d-m, yyyy-m-d)

# New column to contain only dates of this format
obs1$yyyy_md_md <- regmatches(obs1$observed_on_string, gregexpr("\\d{4}[/-]\\d{2}[/-]\\d{2}", obs1$observed_on_string))
obs1$yyyy_md_md[sapply(obs1$yyyy_md_md, length)==0] <- NA
#nb_yyyy_md_md <- length(which(!is.na(obs1$yyyy_md_md))) # the vast majority

# Extracting the numbers
obs1$yyyy_md_md_string <- sapply(obs1$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", obs1$yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
obs1$yyyy_md_md_vector <- regmatches(obs1$yyyy_md_md_string, positions_yyyy_md_md)
obs1$yyyy_md_md_string <- NULL

first_md <- as.integer(sapply(obs1$yyyy_md_md_vector, function(x) return(x[3])))
second_md <- as.integer(sapply(obs1$yyyy_md_md_vector, function(x) return(x[4])))
obs1$yyyy_md_md_vector <- NULL

# Creating function to extract date and month based on the values of first_md and second_md.
# It seems like the vast majority should be month first.
choosing_month_or_date <- function(first_number, second_number){
  
  day <- NA
  month <- NA
  likely_day <- NA
  likely_month <- NA
  
  if (is.na(first_number) && is.na(second_number)){
    # do nothing
  }
  else if (first_number > 12){
    day <- first_number
    month <- second_number
  }
  else if (second_number > 12){
    day <- second_number
    month <- first_number
  }
  else { # when ambiguous, assume first number is the month but mark as ambiguous
    likely_day <- second_number
    likely_month <- first_number
  }
  return(list(day=day,month=month,likely_day=likely_day,likely_month=likely_month))
}

# Applying the function to our data and assigning results to columns 
yyyy_md_md_results <- mapply(choosing_month_or_date,first_md, second_md, SIMPLIFY=FALSE)

obs1$month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$month)
obs1$day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$day)
obs1$likely_month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_month)
obs1$likely_day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_day)



# Section 5: Extracting months and dates when listed as md-md-yyyy - there are very few
# Issue:  many formats (mm-dd-yyyy, dd-mm-yyyy, d-m-yyyy, m-d-yyyy)

# New column to contain only dates of this format
obs1$md_md_yyyy <- regmatches(obs1$observed_on_string, gregexpr("\\d{1,2}[-/]\\d{1,2}[-/]\\d{4}", obs1$observed_on_string))
obs1$md_md_yyyy[sapply(obs1$md_md_yyyy, length)==0] <- NA
#nb_md_md_yyyy <- length(which(!is.na(obs1$md_md_yyyy))) # very few

# Extract the numbers
obs1$md_md_yyyy_string <- sapply(obs1$md_md_yyyy, function(x) if(length(x)==0) NA else x[1])
positions_md_md_yyyy <- regexec("(\\d{1,2})[-/](\\d{1,2})[-/](\\d{4})", obs1$md_md_yyyy_string)
obs1$md_md_yyyy_vector <- NA
obs1$md_md_yyyy_vector <- regmatches(obs1$md_md_yyyy_string, positions_md_md_yyyy)
obs1$md_md_yyyy_string <- NULL

first_md_2 <- as.integer(sapply(obs1$md_md_yyyy_vector, function(x) return(x[3])))
second_md_2 <- as.integer(sapply(obs1$md_md_yyyy_vector, function(x) return(x[4])))
obs1$md_md_yyyy_vector <- NULL

# Applying the function to our data and assigning results to columns 
md_md_yyyy_results <- mapply(choosing_month_or_date, first_md_2, second_md_2, SIMPLIFY=FALSE)

obs1$month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$month)
obs1$day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$day)
obs1$likely_month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_month)
obs1$likely_day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_day)

#confirm they were all assigned
#length(which(!is.na(obs1$day_when_md_md_yyyy))) 



# Section 5: Dealing with exceptions
#nb_no_date <- length(obs1$observed_on_string[is.na(obs1$observed_on_string)]) # when no date is listed
# very few (0.02%) have some non-NA entry other than the formats we've handled. deal with them here if one wishes to
#nb_entries_not_caught <- nrow(obs1) - sum(nb_alhpabetic, nb_md_md_yyyy, nb_yyyy_md_md, nb_no_date)



# Section 6: Assembling uniform date column 
# 6.1 assembling together different parts of the date
obs1$certain_dd <-
  ifelse(!is.na(obs1$day_when_month_alphabetical), obs1$day_when_month_alphabetical,
         ifelse(!is.na(obs1$day_when_yyyy_md_md), obs1$day_when_yyyy_md_md,
                ifelse(!is.na(obs1$day_when_md_md_yyyy), obs1$day_when_md_md_yyyy, NA)))

obs1$probable_dd <- 
  ifelse(!is.na(obs1$likely_day_when_yyyy_md_md), obs1$likely_day_when_yyyy_md_md,
         ifelse(!is.na(obs1$likely_day_when_md_md_yyyy), obs1$likely_day_when_md_md_yyyy, NA))

obs1$certain_mm <-
  ifelse(!is.na(obs1$month_numeric_when_alphabetic), obs1$month_numeric_when_alphabetic,
         ifelse(!is.na(obs1$month_when_yyyy_md_md), obs1$month_when_yyyy_md_md,
                ifelse(!is.na(obs1$month_when_md_md_yyyy), obs1$month_when_md_md_yyyy, NA)))

obs1$probable_mm <-
  ifelse(!is.na(obs1$likely_month_when_yyyy_md_md), obs1$likely_month_when_yyyy_md_md,
         ifelse(!is.na(obs1$likely_month_when_md_md_yyyy), obs1$likely_month_when_md_md_yyyy, NA))

# 6.2 assembling final columns
certain_dd <- ifelse(!is.na(obs1$certain_dd), obs1$certain_dd, "??")
certain_mm <- ifelse(!is.na(obs1$certain_mm), obs1$certain_mm, "??")
certain_yyyy <- ifelse(!is.na(obs1$year), obs1$year, "????")

obs1$certain_ddmmyyyy <-
  paste(
    ifelse(!is.na(obs1$certain_dd), obs1$certain_dd, "??"),
    ifelse(!is.na(obs1$certain_mm), obs1$certain_mm, "??"),
    ifelse(!is.na(obs1$year), obs1$year, "??"),
    sep="/")

obs1$probable_ddmmyyyy <-
  ifelse(
    (!is.na(obs1$probable_dd) & !is.na(obs1$probable_mm) & !is.na(obs1$year)),
    paste(obs1$probable_dd, obs1$probable_mm, obs1$year, sep="/"),
    NA)

obs1$standardized_ddmmyyyy <-
  ifelse((!is.na(obs1$probable_ddmmyyyy)), obs1$probable_ddmmyyyy, obs1$certain_ddmmyyyy)


# # Section 7: Assessing the quality of the "assumed" dates of ambiguous cases
# # when there is indeed a day > 12 (clear case), what percentage of them are in the first position? 
# # this serves as an estimate for the non-clear cases, to see how accurate our month-first assumption is
# nb_total_obs <- nrow(obs1)
# test_yyyy_md_md_string <- sapply(obs1$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
# positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", test_yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
# test_yyyy_md_md_vector <- regmatches(test_yyyy_md_md_string, positions_yyyy_md_md)
# test_first_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[3])))
# test_second_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[4])))
# 
# counting_error_rate <- function(first_number, second_number){
#   
#   both_na <- is.na(test_first_md) & is.na(second_number)
#   total_yyyy_md_md <- sum(!is.na(first_number))
#   first_md_greater_than_12 <- first_number > 12
#   second_md_greater_than_12 <- second_number > 12
#   ambiguous_cases <- !(both_na | first_md_greater_than_12 | second_md_greater_than_12)
#   
#   nb_first_md_gt12 <- sum(first_md_greater_than_12, na.rm = TRUE)
#   nb_second_md_gt12 <- sum(second_md_greater_than_12, na.rm = TRUE)
#   total_clear_cases <- nb_first_md_gt12 + nb_second_md_gt12
#   nb_ambiguous <- sum(ambiguous_cases, na.rm=TRUE)
#   nb_both_na <- sum(both_na)
#   
#   percent_of_clear_cases_with_day_first <- (nb_first_md_gt12/total_clear_cases)*100
#   percent_successful_guess_of_month_first <- round((100-percent_of_clear_cases_with_day_first), 8)
#   
#   return( list(
#     summary = paste(
#       c(
#         paste("total cases of yyyy-md-md: ", total_yyyy_md_md),
#         paste("ambiguous cases:", nb_ambiguous),
#         paste("clear cases:", total_clear_cases),
#         paste0("percent of all the observations caught under this search: ", (total_yyyy_md_md/nb_total_obs)*100,"%"),
#         paste0("percent of yyyy-md-md cases that are ambiguous: ", (nb_ambiguous/total_yyyy_md_md)*100,"%"),
#         paste0("percent of clear cases with day first: ", percent_of_clear_cases_with_day_first,"%")
#       ),
#       collapse="\n"
#     ),
#     percent_successful_guess_of_month_first = percent_successful_guess_of_month_first
#   ))
# }
# 
# 
# test_result <- counting_error_rate(test_first_md, test_second_md)
# cat(test_result$summary) # so 99.9996% of cases, the first number is the day
# percent_successful_guess_of_month_first <- test_result$percent_successful_guess_of_month_first
# 
# # Indicating this error rate in the df
# probable_date_string <- paste0("probable_ddmmyyyy_to_approx_", percent_successful_guess_of_month_first,"_percent_certainty")
# colnames(obs1)[colnames(obs1) == "probable_ddmmyyyy"] <- probable_date_string

# Creating easy column to identify certain dates over probable dates
obs1$is_ambiguous <- 
  ifelse(is.na(obs1$probable_ddmmyyyy),
         0,
         1)

# # Screen for months that should not be
# improper_months_regex <- paste0("^\\d{1,2}/(", paste0(13:99, collapse = "|"), ")/\\d{4}$")
# erroneous_months <- which(!is.na(str_extract(obs1$standardized_ddmmyyyy, improper_months_regex)))
# erroneous_months_df <- obs1[erroneous_months,]

# Section 8: Writing out data with unified columns to desired location
obs1$year <- NULL
obs1$month_alphabetic <- NULL
obs1$day_when_month_alphabetical <- NULL
obs1$yyyy_md_md <- NULL
obs1$month_when_yyyy_md_md <- NULL
obs1$day_when_yyyy_md_md <- NULL
obs1$likely_month_when_yyyy_md_md <- NULL
obs1$likely_day_when_yyyy_md_md <- NULL
obs1$md_md_yyyy <- NULL
obs1$month_when_md_md_yyyy <- NULL
obs1$day_when_md_md_yyyy <- NULL
obs1$likely_month_when_md_md_yyyy <- NULL
obs1$likely_day_when_md_md_yyyy <- NULL
obs1$certain_dd <- NULL
obs1$probable_dd <- NULL
obs1$certain_mm <- NULL
obs1$probable_mm <- NULL
obs1$certain_ddmmyyyy <- NULL
obs1$probable_ddmmyyyy <- NULL
obs1$just_month_when_alphabetic <- NULL
obs1$month_numeric_when_alphabetic <- NULL


############## 2 #####################

# Section 2: Extracting the year
#2.1 first pass - grep any set of 4 numbers
obs2$year4num <- regmatches(obs2$observed_on_string, gregexpr("\\d{4}", obs2$observed_on_string))
obs2$year <- obs2$year4num
obs2$year <- sapply(obs2$year4num, function(x){ ifelse(length(x)==0,NA,x[1])}) # to NA the ones with no year
#length(which(is.na(obs2$year))) # about 95% of these cause theres no entry at all. 5% have an entry without a year

#2.2 second pass - correct those with multiple entries of 4 numbers
twos_indx <- which(sapply(obs2$year4num, length) == 2)

year_only <- lapply(obs2$year4num[twos_indx], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 1800 & x_num < 2026] # the plausible 4digit is used
})
obs2$year[twos_indx] <- year_only

#2.3 Any more still left? Then refine date inclusion criteria
twos_indx_still <- which(sapply(obs2$year, length) == 2)
obs2$observed_on_string[twos_indx_still] # displays the dates

# All years 2015 and 2016 with manual check, so use >2000. Change this if need be for different dfs
year_only <- lapply(obs2$year[twos_indx_still], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 2000]
})
obs2$year[twos_indx_still] <- year_only

# 2.4 Final check - any left? If so manually edit your df
#length(which(sapply(obs2$year, length) == 2)) # if empty, no further checks needed
obs2$year4num <- NULL




# Section 3 - Extracting the Month and dates when month listed as letters

# create a lookup table
month_lookup <- c(
  Jan=1, January=1, janv=1, janvier=1, jan=1, january=1,
  Feb=2, February=2, Fév=2, fév=2, fevrier=2, février=2, feb=2, february=2,
  Mar=3, March=3, mars=3, mar=3, march=3,
  Apr=4, April=4, avr=4, avril=4, apr=4, april=4,
  May=5, mai=5, may=5,
  Jun=6, June=6, juin=6, jun=6, june=6,
  Jul=7, July=7, juil=7, juillet=7, jul=7, july=7,
  Aug=8, August=8, aout=8, août=8, aug=8, august=8,
  Sep=9, Sept=9, September=9, sept=9, septembre=9, sep=9, september=9,
  Oct=10, October=10, oct=10, octobre=10, oct=10, october=10,
  Nov=11, November=11, nov=11, novembre=11, nov=11, november=11,
  Dec=12, December=12, déc=12, dec=12, décembre=12, dec=12, december=12
)

# create regex and use str_extract to take out the month
months_alphabetic_regex <- paste0("\\b(", paste(names(month_lookup), collapse="|"), ")\\b")
obs2$just_month_when_alphabetic <- str_extract(obs2$observed_on_string, months_alphabetic_regex)
#nb_alhpabetic <- length(which(!is.na(obs2$just_month_when_alphabetic))) # roughly 15% of cases

# assigning it to new column
obs2$month_numeric_when_alphabetic <- sapply(obs2$just_month_when_alphabetic, function(x){
  if (!is.na(x)){
    month_lookup[[x]]
  }
  else{
    NA
  }
})

# Finding the dates from these rows - should be the only " XX " in the string
obs2$day_when_month_alphabetical <- NA
rows_with_alphabetical_month <- !is.na(obs2$month_numeric_when_alphabetic)
positions_alpha_day <- regexec("\\s(\\d{2})\\s", obs2$observed_on_string[rows_with_alphabetical_month])
day_when_alpha <- regmatches(obs2$observed_on_string[rows_with_alphabetical_month], positions_alpha_day)
obs2$day_when_month_alphabetical[rows_with_alphabetical_month] <- 
  sapply(day_when_alpha, function(x) x[2])

#looking for errors
#length(which(sapply(obs2$day_when_month_alphabetical, length)> 1)) # any double dates? should be none
#length(which(obs2$day_when_month_alphabetical > 31))# any false dates? should be none


# Section 4: Extracting months and dates when listed as yyyy-m?d?-m?d?
# Issue: many formats (yyyy-mm-dd, yyyy-dd-mm, yyyy-d-m, yyyy-m-d)

# New column to contain only dates of this format
obs2$yyyy_md_md <- regmatches(obs2$observed_on_string, gregexpr("\\d{4}[/-]\\d{2}[/-]\\d{2}", obs2$observed_on_string))
obs2$yyyy_md_md[sapply(obs2$yyyy_md_md, length)==0] <- NA
#nb_yyyy_md_md <- length(which(!is.na(obs2$yyyy_md_md))) # the vast majority

# Extracting the numbers
obs2$yyyy_md_md_string <- sapply(obs2$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", obs2$yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
obs2$yyyy_md_md_vector <- regmatches(obs2$yyyy_md_md_string, positions_yyyy_md_md)
obs2$yyyy_md_md_string <- NULL

first_md <- as.integer(sapply(obs2$yyyy_md_md_vector, function(x) return(x[3])))
second_md <- as.integer(sapply(obs2$yyyy_md_md_vector, function(x) return(x[4])))
obs2$yyyy_md_md_vector <- NULL

# Creating function to extract date and month based on the values of first_md and second_md.
# It seems like the vast majority should be month first.
choosing_month_or_date <- function(first_number, second_number){
  
  day <- NA
  month <- NA
  likely_day <- NA
  likely_month <- NA
  
  if (is.na(first_number) && is.na(second_number)){
    # do nothing
  }
  else if (first_number > 12){
    day <- first_number
    month <- second_number
  }
  else if (second_number > 12){
    day <- second_number
    month <- first_number
  }
  else { # when ambiguous, assume first number is the month but mark as ambiguous
    likely_day <- second_number
    likely_month <- first_number
  }
  return(list(day=day,month=month,likely_day=likely_day,likely_month=likely_month))
}

# Applying the function to our data and assigning results to columns 
yyyy_md_md_results <- mapply(choosing_month_or_date,first_md, second_md, SIMPLIFY=FALSE)

obs2$month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$month)
obs2$day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$day)
obs2$likely_month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_month)
obs2$likely_day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_day)



# Section 5: Extracting months and dates when listed as md-md-yyyy - there are very few
# Issue:  many formats (mm-dd-yyyy, dd-mm-yyyy, d-m-yyyy, m-d-yyyy)

# New column to contain only dates of this format
obs2$md_md_yyyy <- regmatches(obs2$observed_on_string, gregexpr("\\d{1,2}[-/]\\d{1,2}[-/]\\d{4}", obs2$observed_on_string))
obs2$md_md_yyyy[sapply(obs2$md_md_yyyy, length)==0] <- NA
#nb_md_md_yyyy <- length(which(!is.na(obs2$md_md_yyyy))) # very few

# Extract the numbers
obs2$md_md_yyyy_string <- sapply(obs2$md_md_yyyy, function(x) if(length(x)==0) NA else x[1])
positions_md_md_yyyy <- regexec("(\\d{1,2})[-/](\\d{1,2})[-/](\\d{4})", obs2$md_md_yyyy_string)
obs2$md_md_yyyy_vector <- NA
obs2$md_md_yyyy_vector <- regmatches(obs2$md_md_yyyy_string, positions_md_md_yyyy)
obs2$md_md_yyyy_string <- NULL

first_md_2 <- as.integer(sapply(obs2$md_md_yyyy_vector, function(x) return(x[3])))
second_md_2 <- as.integer(sapply(obs2$md_md_yyyy_vector, function(x) return(x[4])))
obs2$md_md_yyyy_vector <- NULL

# Applying the function to our data and assigning results to columns 
md_md_yyyy_results <- mapply(choosing_month_or_date, first_md_2, second_md_2, SIMPLIFY=FALSE)

obs2$month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$month)
obs2$day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$day)
obs2$likely_month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_month)
obs2$likely_day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_day)

#confirm they were all assigned
#length(which(!is.na(obs2$day_when_md_md_yyyy))) 



# Section 5: Dealing with exceptions
#nb_no_date <- length(obs2$observed_on_string[is.na(obs2$observed_on_string)]) # when no date is listed
# very few (0.02%) have some non-NA entry other than the formats we've handled. deal with them here if one wishes to
#nb_entries_not_caught <- nrow(obs2) - sum(nb_alhpabetic, nb_md_md_yyyy, nb_yyyy_md_md, nb_no_date)



# Section 6: Assembling uniform date column 
# 6.1 assembling together different parts of the date
obs2$certain_dd <-
  ifelse(!is.na(obs2$day_when_month_alphabetical), obs2$day_when_month_alphabetical,
         ifelse(!is.na(obs2$day_when_yyyy_md_md), obs2$day_when_yyyy_md_md,
                ifelse(!is.na(obs2$day_when_md_md_yyyy), obs2$day_when_md_md_yyyy, NA)))

obs2$probable_dd <- 
  ifelse(!is.na(obs2$likely_day_when_yyyy_md_md), obs2$likely_day_when_yyyy_md_md,
         ifelse(!is.na(obs2$likely_day_when_md_md_yyyy), obs2$likely_day_when_md_md_yyyy, NA))

obs2$certain_mm <-
  ifelse(!is.na(obs2$month_numeric_when_alphabetic), obs2$month_numeric_when_alphabetic,
         ifelse(!is.na(obs2$month_when_yyyy_md_md), obs2$month_when_yyyy_md_md,
                ifelse(!is.na(obs2$month_when_md_md_yyyy), obs2$month_when_md_md_yyyy, NA)))

obs2$probable_mm <-
  ifelse(!is.na(obs2$likely_month_when_yyyy_md_md), obs2$likely_month_when_yyyy_md_md,
         ifelse(!is.na(obs2$likely_month_when_md_md_yyyy), obs2$likely_month_when_md_md_yyyy, NA))

# 6.2 assembling final columns
certain_dd <- ifelse(!is.na(obs2$certain_dd), obs2$certain_dd, "??")
certain_mm <- ifelse(!is.na(obs2$certain_mm), obs2$certain_mm, "??")
certain_yyyy <- ifelse(!is.na(obs2$year), obs2$year, "????")

obs2$certain_ddmmyyyy <-
  paste(
    ifelse(!is.na(obs2$certain_dd), obs2$certain_dd, "??"),
    ifelse(!is.na(obs2$certain_mm), obs2$certain_mm, "??"),
    ifelse(!is.na(obs2$year), obs2$year, "??"),
    sep="/")

obs2$probable_ddmmyyyy <-
  ifelse(
    (!is.na(obs2$probable_dd) & !is.na(obs2$probable_mm) & !is.na(obs2$year)),
    paste(obs2$probable_dd, obs2$probable_mm, obs2$year, sep="/"),
    NA)

obs2$standardized_ddmmyyyy <-
  ifelse((!is.na(obs2$probable_ddmmyyyy)), obs2$probable_ddmmyyyy, obs2$certain_ddmmyyyy)


# # Section 7: Assessing the quality of the "assumed" dates of ambiguous cases
# # when there is indeed a day > 12 (clear case), what percentage of them are in the first position? 
# # this serves as an estimate for the non-clear cases, to see how accurate our month-first assumption is
# nb_total_obs <- nrow(obs2)
# test_yyyy_md_md_string <- sapply(obs2$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
# positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", test_yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
# test_yyyy_md_md_vector <- regmatches(test_yyyy_md_md_string, positions_yyyy_md_md)
# test_first_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[3])))
# test_second_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[4])))
# 
# counting_error_rate <- function(first_number, second_number){
#   
#   both_na <- is.na(test_first_md) & is.na(second_number)
#   total_yyyy_md_md <- sum(!is.na(first_number))
#   first_md_greater_than_12 <- first_number > 12
#   second_md_greater_than_12 <- second_number > 12
#   ambiguous_cases <- !(both_na | first_md_greater_than_12 | second_md_greater_than_12)
#   
#   nb_first_md_gt12 <- sum(first_md_greater_than_12, na.rm = TRUE)
#   nb_second_md_gt12 <- sum(second_md_greater_than_12, na.rm = TRUE)
#   total_clear_cases <- nb_first_md_gt12 + nb_second_md_gt12
#   nb_ambiguous <- sum(ambiguous_cases, na.rm=TRUE)
#   nb_both_na <- sum(both_na)
#   
#   percent_of_clear_cases_with_day_first <- (nb_first_md_gt12/total_clear_cases)*100
#   percent_successful_guess_of_month_first <- round((100-percent_of_clear_cases_with_day_first), 8)
#   
#   return( list(
#     summary = paste(
#       c(
#         paste("total cases of yyyy-md-md: ", total_yyyy_md_md),
#         paste("ambiguous cases:", nb_ambiguous),
#         paste("clear cases:", total_clear_cases),
#         paste0("percent of all the observations caught under this search: ", (total_yyyy_md_md/nb_total_obs)*100,"%"),
#         paste0("percent of yyyy-md-md cases that are ambiguous: ", (nb_ambiguous/total_yyyy_md_md)*100,"%"),
#         paste0("percent of clear cases with day first: ", percent_of_clear_cases_with_day_first,"%")
#       ),
#       collapse="\n"
#     ),
#     percent_successful_guess_of_month_first = percent_successful_guess_of_month_first
#   ))
# }
# 
# 
# test_result <- counting_error_rate(test_first_md, test_second_md)
# cat(test_result$summary) # so 99.9996% of cases, the first number is the day
# percent_successful_guess_of_month_first <- test_result$percent_successful_guess_of_month_first
# 
# # Indicating this error rate in the df
# probable_date_string <- paste0("probable_ddmmyyyy_to_approx_", percent_successful_guess_of_month_first,"_percent_certainty")
# colnames(obs2)[colnames(obs2) == "probable_ddmmyyyy"] <- probable_date_string

# Creating easy column to identify certain dates over probable dates
obs2$is_ambiguous <- 
  ifelse(is.na(obs2$probable_ddmmyyyy),
         0,
         1)

# # Screen for months that should not be
# improper_months_regex <- paste0("^\\d{1,2}/(", paste0(13:99, collapse = "|"), ")/\\d{4}$")
# erroneous_months <- which(!is.na(str_extract(obs2$standardized_ddmmyyyy, improper_months_regex)))
# erroneous_months_df <- obs2[erroneous_months,]

# Section 8: Writing out data with unified columns to desired location
obs2$year <- NULL
obs2$month_alphabetic <- NULL
obs2$day_when_month_alphabetical <- NULL
obs2$yyyy_md_md <- NULL
obs2$month_when_yyyy_md_md <- NULL
obs2$day_when_yyyy_md_md <- NULL
obs2$likely_month_when_yyyy_md_md <- NULL
obs2$likely_day_when_yyyy_md_md <- NULL
obs2$md_md_yyyy <- NULL
obs2$month_when_md_md_yyyy <- NULL
obs2$day_when_md_md_yyyy <- NULL
obs2$likely_month_when_md_md_yyyy <- NULL
obs2$likely_day_when_md_md_yyyy <- NULL
obs2$certain_dd <- NULL
obs2$probable_dd <- NULL
obs2$certain_mm <- NULL
obs2$probable_mm <- NULL
obs2$certain_ddmmyyyy <- NULL
obs2$probable_ddmmyyyy <- NULL
obs2$just_month_when_alphabetic <- NULL
obs2$month_numeric_when_alphabetic <- NULL









######################## 3##########################

# Section 2: Extracting the year
#2.1 first pass - grep any set of 4 numbers
obs3$year4num <- regmatches(obs3$observed_on_string, gregexpr("\\d{4}", obs3$observed_on_string))
obs3$year <- obs3$year4num
obs3$year <- sapply(obs3$year4num, function(x){ ifelse(length(x)==0,NA,x[1])}) # to NA the ones with no year
#length(which(is.na(obs3$year))) # about 95% of these cause theres no entry at all. 5% have an entry without a year

#2.2 second pass - correct those with multiple entries of 4 numbers
twos_indx <- which(sapply(obs3$year4num, length) == 2)

year_only <- lapply(obs3$year4num[twos_indx], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 1800 & x_num < 2026] # the plausible 4digit is used
})
obs3$year[twos_indx] <- year_only

#2.3 Any more still left? Then refine date inclusion criteria
twos_indx_still <- which(sapply(obs3$year, length) == 2)
obs3$observed_on_string[twos_indx_still] # displays the dates

# All years 2015 and 2016 with manual check, so use >2000. Change this if need be for different dfs
year_only <- lapply(obs3$year[twos_indx_still], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 2000]
})
obs3$year[twos_indx_still] <- year_only

# 2.4 Final check - any left? If so manually edit your df
#length(which(sapply(obs3$year, length) == 2)) # if empty, no further checks needed
obs3$year4num <- NULL




# Section 3 - Extracting the Month and dates when month listed as letters

# create a lookup table
month_lookup <- c(
  Jan=1, January=1, janv=1, janvier=1, jan=1, january=1,
  Feb=2, February=2, Fév=2, fév=2, fevrier=2, février=2, feb=2, february=2,
  Mar=3, March=3, mars=3, mar=3, march=3,
  Apr=4, April=4, avr=4, avril=4, apr=4, april=4,
  May=5, mai=5, may=5,
  Jun=6, June=6, juin=6, jun=6, june=6,
  Jul=7, July=7, juil=7, juillet=7, jul=7, july=7,
  Aug=8, August=8, aout=8, août=8, aug=8, august=8,
  Sep=9, Sept=9, September=9, sept=9, septembre=9, sep=9, september=9,
  Oct=10, October=10, oct=10, octobre=10, oct=10, october=10,
  Nov=11, November=11, nov=11, novembre=11, nov=11, november=11,
  Dec=12, December=12, déc=12, dec=12, décembre=12, dec=12, december=12
)

# create regex and use str_extract to take out the month
months_alphabetic_regex <- paste0("\\b(", paste(names(month_lookup), collapse="|"), ")\\b")
obs3$just_month_when_alphabetic <- str_extract(obs3$observed_on_string, months_alphabetic_regex)
#nb_alhpabetic <- length(which(!is.na(obs3$just_month_when_alphabetic))) # roughly 15% of cases

# assigning it to new column
obs3$month_numeric_when_alphabetic <- sapply(obs3$just_month_when_alphabetic, function(x){
  if (!is.na(x)){
    month_lookup[[x]]
  }
  else{
    NA
  }
})

# Finding the dates from these rows - should be the only " XX " in the string
obs3$day_when_month_alphabetical <- NA
rows_with_alphabetical_month <- !is.na(obs3$month_numeric_when_alphabetic)
positions_alpha_day <- regexec("\\s(\\d{2})\\s", obs3$observed_on_string[rows_with_alphabetical_month])
day_when_alpha <- regmatches(obs3$observed_on_string[rows_with_alphabetical_month], positions_alpha_day)
obs3$day_when_month_alphabetical[rows_with_alphabetical_month] <- 
  sapply(day_when_alpha, function(x) x[2])

#looking for errors
#length(which(sapply(obs3$day_when_month_alphabetical, length)> 1)) # any double dates? should be none
#length(which(obs3$day_when_month_alphabetical > 31))# any false dates? should be none


# Section 4: Extracting months and dates when listed as yyyy-m?d?-m?d?
# Issue: many formats (yyyy-mm-dd, yyyy-dd-mm, yyyy-d-m, yyyy-m-d)

# New column to contain only dates of this format
obs3$yyyy_md_md <- regmatches(obs3$observed_on_string, gregexpr("\\d{4}[/-]\\d{2}[/-]\\d{2}", obs3$observed_on_string))
obs3$yyyy_md_md[sapply(obs3$yyyy_md_md, length)==0] <- NA
#nb_yyyy_md_md <- length(which(!is.na(obs3$yyyy_md_md))) # the vast majority

# Extracting the numbers
obs3$yyyy_md_md_string <- sapply(obs3$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", obs3$yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
obs3$yyyy_md_md_vector <- regmatches(obs3$yyyy_md_md_string, positions_yyyy_md_md)
obs3$yyyy_md_md_string <- NULL

first_md <- as.integer(sapply(obs3$yyyy_md_md_vector, function(x) return(x[3])))
second_md <- as.integer(sapply(obs3$yyyy_md_md_vector, function(x) return(x[4])))
obs3$yyyy_md_md_vector <- NULL

# Creating function to extract date and month based on the values of first_md and second_md.
# It seems like the vast majority should be month first.
choosing_month_or_date <- function(first_number, second_number){
  
  day <- NA
  month <- NA
  likely_day <- NA
  likely_month <- NA
  
  if (is.na(first_number) && is.na(second_number)){
    # do nothing
  }
  else if (first_number > 12){
    day <- first_number
    month <- second_number
  }
  else if (second_number > 12){
    day <- second_number
    month <- first_number
  }
  else { # when ambiguous, assume first number is the month but mark as ambiguous
    likely_day <- second_number
    likely_month <- first_number
  }
  return(list(day=day,month=month,likely_day=likely_day,likely_month=likely_month))
}

# Applying the function to our data and assigning results to columns 
yyyy_md_md_results <- mapply(choosing_month_or_date,first_md, second_md, SIMPLIFY=FALSE)

obs3$month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$month)
obs3$day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$day)
obs3$likely_month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_month)
obs3$likely_day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_day)



# Section 5: Extracting months and dates when listed as md-md-yyyy - there are very few
# Issue:  many formats (mm-dd-yyyy, dd-mm-yyyy, d-m-yyyy, m-d-yyyy)

# New column to contain only dates of this format
obs3$md_md_yyyy <- regmatches(obs3$observed_on_string, gregexpr("\\d{1,2}[-/]\\d{1,2}[-/]\\d{4}", obs3$observed_on_string))
obs3$md_md_yyyy[sapply(obs3$md_md_yyyy, length)==0] <- NA
#nb_md_md_yyyy <- length(which(!is.na(obs3$md_md_yyyy))) # very few

# Extract the numbers
obs3$md_md_yyyy_string <- sapply(obs3$md_md_yyyy, function(x) if(length(x)==0) NA else x[1])
positions_md_md_yyyy <- regexec("(\\d{1,2})[-/](\\d{1,2})[-/](\\d{4})", obs3$md_md_yyyy_string)
obs3$md_md_yyyy_vector <- NA
obs3$md_md_yyyy_vector <- regmatches(obs3$md_md_yyyy_string, positions_md_md_yyyy)
obs3$md_md_yyyy_string <- NULL

first_md_2 <- as.integer(sapply(obs3$md_md_yyyy_vector, function(x) return(x[3])))
second_md_2 <- as.integer(sapply(obs3$md_md_yyyy_vector, function(x) return(x[4])))
obs3$md_md_yyyy_vector <- NULL

# Applying the function to our data and assigning results to columns 
md_md_yyyy_results <- mapply(choosing_month_or_date, first_md_2, second_md_2, SIMPLIFY=FALSE)

obs3$month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$month)
obs3$day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$day)
obs3$likely_month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_month)
obs3$likely_day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_day)

#confirm they were all assigned
#length(which(!is.na(obs3$day_when_md_md_yyyy))) 



# Section 5: Dealing with exceptions
#nb_no_date <- length(obs3$observed_on_string[is.na(obs3$observed_on_string)]) # when no date is listed
# very few (0.02%) have some non-NA entry other than the formats we've handled. deal with them here if one wishes to
#nb_entries_not_caught <- nrow(obs3) - sum(nb_alhpabetic, nb_md_md_yyyy, nb_yyyy_md_md, nb_no_date)



# Section 6: Assembling uniform date column 
# 6.1 assembling together different parts of the date
obs3$certain_dd <-
  ifelse(!is.na(obs3$day_when_month_alphabetical), obs3$day_when_month_alphabetical,
         ifelse(!is.na(obs3$day_when_yyyy_md_md), obs3$day_when_yyyy_md_md,
                ifelse(!is.na(obs3$day_when_md_md_yyyy), obs3$day_when_md_md_yyyy, NA)))

obs3$probable_dd <- 
  ifelse(!is.na(obs3$likely_day_when_yyyy_md_md), obs3$likely_day_when_yyyy_md_md,
         ifelse(!is.na(obs3$likely_day_when_md_md_yyyy), obs3$likely_day_when_md_md_yyyy, NA))

obs3$certain_mm <-
  ifelse(!is.na(obs3$month_numeric_when_alphabetic), obs3$month_numeric_when_alphabetic,
         ifelse(!is.na(obs3$month_when_yyyy_md_md), obs3$month_when_yyyy_md_md,
                ifelse(!is.na(obs3$month_when_md_md_yyyy), obs3$month_when_md_md_yyyy, NA)))

obs3$probable_mm <-
  ifelse(!is.na(obs3$likely_month_when_yyyy_md_md), obs3$likely_month_when_yyyy_md_md,
         ifelse(!is.na(obs3$likely_month_when_md_md_yyyy), obs3$likely_month_when_md_md_yyyy, NA))

# 6.2 assembling final columns
certain_dd <- ifelse(!is.na(obs3$certain_dd), obs3$certain_dd, "??")
certain_mm <- ifelse(!is.na(obs3$certain_mm), obs3$certain_mm, "??")
certain_yyyy <- ifelse(!is.na(obs3$year), obs3$year, "????")

obs3$certain_ddmmyyyy <-
  paste(
    ifelse(!is.na(obs3$certain_dd), obs3$certain_dd, "??"),
    ifelse(!is.na(obs3$certain_mm), obs3$certain_mm, "??"),
    ifelse(!is.na(obs3$year), obs3$year, "??"),
    sep="/")

obs3$probable_ddmmyyyy <-
  ifelse(
    (!is.na(obs3$probable_dd) & !is.na(obs3$probable_mm) & !is.na(obs3$year)),
    paste(obs3$probable_dd, obs3$probable_mm, obs3$year, sep="/"),
    NA)

obs3$standardized_ddmmyyyy <-
  ifelse((!is.na(obs3$probable_ddmmyyyy)), obs3$probable_ddmmyyyy, obs3$certain_ddmmyyyy)


# # Section 7: Assessing the quality of the "assumed" dates of ambiguous cases
# # when there is indeed a day > 12 (clear case), what percentage of them are in the first position? 
# # this serves as an estimate for the non-clear cases, to see how accurate our month-first assumption is
# nb_total_obs <- nrow(obs3)
# test_yyyy_md_md_string <- sapply(obs3$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
# positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", test_yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
# test_yyyy_md_md_vector <- regmatches(test_yyyy_md_md_string, positions_yyyy_md_md)
# test_first_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[3])))
# test_second_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[4])))
# 
# counting_error_rate <- function(first_number, second_number){
#   
#   both_na <- is.na(test_first_md) & is.na(second_number)
#   total_yyyy_md_md <- sum(!is.na(first_number))
#   first_md_greater_than_12 <- first_number > 12
#   second_md_greater_than_12 <- second_number > 12
#   ambiguous_cases <- !(both_na | first_md_greater_than_12 | second_md_greater_than_12)
#   
#   nb_first_md_gt12 <- sum(first_md_greater_than_12, na.rm = TRUE)
#   nb_second_md_gt12 <- sum(second_md_greater_than_12, na.rm = TRUE)
#   total_clear_cases <- nb_first_md_gt12 + nb_second_md_gt12
#   nb_ambiguous <- sum(ambiguous_cases, na.rm=TRUE)
#   nb_both_na <- sum(both_na)
#   
#   percent_of_clear_cases_with_day_first <- (nb_first_md_gt12/total_clear_cases)*100
#   percent_successful_guess_of_month_first <- round((100-percent_of_clear_cases_with_day_first), 8)
#   
#   return( list(
#     summary = paste(
#       c(
#         paste("total cases of yyyy-md-md: ", total_yyyy_md_md),
#         paste("ambiguous cases:", nb_ambiguous),
#         paste("clear cases:", total_clear_cases),
#         paste0("percent of all the observations caught under this search: ", (total_yyyy_md_md/nb_total_obs)*100,"%"),
#         paste0("percent of yyyy-md-md cases that are ambiguous: ", (nb_ambiguous/total_yyyy_md_md)*100,"%"),
#         paste0("percent of clear cases with day first: ", percent_of_clear_cases_with_day_first,"%")
#       ),
#       collapse="\n"
#     ),
#     percent_successful_guess_of_month_first = percent_successful_guess_of_month_first
#   ))
# }
# 
# 
# test_result <- counting_error_rate(test_first_md, test_second_md)
# cat(test_result$summary) # so 99.9996% of cases, the first number is the day
# percent_successful_guess_of_month_first <- test_result$percent_successful_guess_of_month_first
# 
# # Indicating this error rate in the df
# probable_date_string <- paste0("probable_ddmmyyyy_to_approx_", percent_successful_guess_of_month_first,"_percent_certainty")
# colnames(obs3)[colnames(obs3) == "probable_ddmmyyyy"] <- probable_date_string

# Creating easy column to identify certain dates over probable dates
obs3$is_ambiguous <- 
  ifelse(is.na(obs3$probable_ddmmyyyy),
         0,
         1)

# # Screen for months that should not be
# improper_months_regex <- paste0("^\\d{1,2}/(", paste0(13:99, collapse = "|"), ")/\\d{4}$")
# erroneous_months <- which(!is.na(str_extract(obs3$standardized_ddmmyyyy, improper_months_regex)))
# erroneous_months_df <- obs3[erroneous_months,]

# Section 8: Writing out data with unified columns to desired location
obs3$year <- NULL
obs3$month_alphabetic <- NULL
obs3$day_when_month_alphabetical <- NULL
obs3$yyyy_md_md <- NULL
obs3$month_when_yyyy_md_md <- NULL
obs3$day_when_yyyy_md_md <- NULL
obs3$likely_month_when_yyyy_md_md <- NULL
obs3$likely_day_when_yyyy_md_md <- NULL
obs3$md_md_yyyy <- NULL
obs3$month_when_md_md_yyyy <- NULL
obs3$day_when_md_md_yyyy <- NULL
obs3$likely_month_when_md_md_yyyy <- NULL
obs3$likely_day_when_md_md_yyyy <- NULL
obs3$certain_dd <- NULL
obs3$probable_dd <- NULL
obs3$certain_mm <- NULL
obs3$probable_mm <- NULL
obs3$certain_ddmmyyyy <- NULL
obs3$probable_ddmmyyyy <- NULL
obs3$just_month_when_alphabetic <- NULL
obs3$month_numeric_when_alphabetic <- NULL









############################## 4 ###############################


# Section 2: Extracting the year
#2.1 first pass - grep any set of 4 numbers
obs4$year4num <- regmatches(obs4$observed_on_string, gregexpr("\\d{4}", obs4$observed_on_string))
obs4$year <- obs4$year4num
obs4$year <- sapply(obs4$year4num, function(x){ ifelse(length(x)==0,NA,x[1])}) # to NA the ones with no year
#length(which(is.na(obs4$year))) # about 95% of these cause theres no entry at all. 5% have an entry without a year

#2.2 second pass - correct those with multiple entries of 4 numbers
twos_indx <- which(sapply(obs4$year4num, length) == 2)

year_only <- lapply(obs4$year4num[twos_indx], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 1800 & x_num < 2026] # the plausible 4digit is used
})
obs4$year[twos_indx] <- year_only

#2.3 Any more still left? Then refine date inclusion criteria
twos_indx_still <- which(sapply(obs4$year, length) == 2)
obs4$observed_on_string[twos_indx_still] # displays the dates

# All years 2015 and 2016 with manual check, so use >2000. Change this if need be for different dfs
year_only <- lapply(obs4$year[twos_indx_still], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 2000]
})
obs4$year[twos_indx_still] <- year_only

# 2.4 Final check - any left? If so manually edit your df
#length(which(sapply(obs4$year, length) == 2)) # if empty, no further checks needed
obs4$year4num <- NULL




# Section 3 - Extracting the Month and dates when month listed as letters

# create a lookup table
month_lookup <- c(
  Jan=1, January=1, janv=1, janvier=1, jan=1, january=1,
  Feb=2, February=2, Fév=2, fév=2, fevrier=2, février=2, feb=2, february=2,
  Mar=3, March=3, mars=3, mar=3, march=3,
  Apr=4, April=4, avr=4, avril=4, apr=4, april=4,
  May=5, mai=5, may=5,
  Jun=6, June=6, juin=6, jun=6, june=6,
  Jul=7, July=7, juil=7, juillet=7, jul=7, july=7,
  Aug=8, August=8, aout=8, août=8, aug=8, august=8,
  Sep=9, Sept=9, September=9, sept=9, septembre=9, sep=9, september=9,
  Oct=10, October=10, oct=10, octobre=10, oct=10, october=10,
  Nov=11, November=11, nov=11, novembre=11, nov=11, november=11,
  Dec=12, December=12, déc=12, dec=12, décembre=12, dec=12, december=12
)

# create regex and use str_extract to take out the month
months_alphabetic_regex <- paste0("\\b(", paste(names(month_lookup), collapse="|"), ")\\b")
obs4$just_month_when_alphabetic <- str_extract(obs4$observed_on_string, months_alphabetic_regex)
#nb_alhpabetic <- length(which(!is.na(obs4$just_month_when_alphabetic))) # roughly 15% of cases

# assigning it to new column
obs4$month_numeric_when_alphabetic <- sapply(obs4$just_month_when_alphabetic, function(x){
  if (!is.na(x)){
    month_lookup[[x]]
  }
  else{
    NA
  }
})

# Finding the dates from these rows - should be the only " XX " in the string
obs4$day_when_month_alphabetical <- NA
rows_with_alphabetical_month <- !is.na(obs4$month_numeric_when_alphabetic)
positions_alpha_day <- regexec("\\s(\\d{2})\\s", obs4$observed_on_string[rows_with_alphabetical_month])
day_when_alpha <- regmatches(obs4$observed_on_string[rows_with_alphabetical_month], positions_alpha_day)
obs4$day_when_month_alphabetical[rows_with_alphabetical_month] <- 
  sapply(day_when_alpha, function(x) x[2])

#looking for errors
#length(which(sapply(obs4$day_when_month_alphabetical, length)> 1)) # any double dates? should be none
#length(which(obs4$day_when_month_alphabetical > 31))# any false dates? should be none


# Section 4: Extracting months and dates when listed as yyyy-m?d?-m?d?
# Issue: many formats (yyyy-mm-dd, yyyy-dd-mm, yyyy-d-m, yyyy-m-d)

# New column to contain only dates of this format
obs4$yyyy_md_md <- regmatches(obs4$observed_on_string, gregexpr("\\d{4}[/-]\\d{2}[/-]\\d{2}", obs4$observed_on_string))
obs4$yyyy_md_md[sapply(obs4$yyyy_md_md, length)==0] <- NA
#nb_yyyy_md_md <- length(which(!is.na(obs4$yyyy_md_md))) # the vast majority

# Extracting the numbers
obs4$yyyy_md_md_string <- sapply(obs4$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", obs4$yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
obs4$yyyy_md_md_vector <- regmatches(obs4$yyyy_md_md_string, positions_yyyy_md_md)
obs4$yyyy_md_md_string <- NULL

first_md <- as.integer(sapply(obs4$yyyy_md_md_vector, function(x) return(x[3])))
second_md <- as.integer(sapply(obs4$yyyy_md_md_vector, function(x) return(x[4])))
obs4$yyyy_md_md_vector <- NULL

# Creating function to extract date and month based on the values of first_md and second_md.
# It seems like the vast majority should be month first.
choosing_month_or_date <- function(first_number, second_number){
  
  day <- NA
  month <- NA
  likely_day <- NA
  likely_month <- NA
  
  if (is.na(first_number) && is.na(second_number)){
    # do nothing
  }
  else if (first_number > 12){
    day <- first_number
    month <- second_number
  }
  else if (second_number > 12){
    day <- second_number
    month <- first_number
  }
  else { # when ambiguous, assume first number is the month but mark as ambiguous
    likely_day <- second_number
    likely_month <- first_number
  }
  return(list(day=day,month=month,likely_day=likely_day,likely_month=likely_month))
}

# Applying the function to our data and assigning results to columns 
yyyy_md_md_results <- mapply(choosing_month_or_date,first_md, second_md, SIMPLIFY=FALSE)

obs4$month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$month)
obs4$day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$day)
obs4$likely_month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_month)
obs4$likely_day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_day)



# Section 5: Extracting months and dates when listed as md-md-yyyy - there are very few
# Issue:  many formats (mm-dd-yyyy, dd-mm-yyyy, d-m-yyyy, m-d-yyyy)

# New column to contain only dates of this format
obs4$md_md_yyyy <- regmatches(obs4$observed_on_string, gregexpr("\\d{1,2}[-/]\\d{1,2}[-/]\\d{4}", obs4$observed_on_string))
obs4$md_md_yyyy[sapply(obs4$md_md_yyyy, length)==0] <- NA
#nb_md_md_yyyy <- length(which(!is.na(obs4$md_md_yyyy))) # very few

# Extract the numbers
obs4$md_md_yyyy_string <- sapply(obs4$md_md_yyyy, function(x) if(length(x)==0) NA else x[1])
positions_md_md_yyyy <- regexec("(\\d{1,2})[-/](\\d{1,2})[-/](\\d{4})", obs4$md_md_yyyy_string)
obs4$md_md_yyyy_vector <- NA
obs4$md_md_yyyy_vector <- regmatches(obs4$md_md_yyyy_string, positions_md_md_yyyy)
obs4$md_md_yyyy_string <- NULL

first_md_2 <- as.integer(sapply(obs4$md_md_yyyy_vector, function(x) return(x[3])))
second_md_2 <- as.integer(sapply(obs4$md_md_yyyy_vector, function(x) return(x[4])))
obs4$md_md_yyyy_vector <- NULL

# Applying the function to our data and assigning results to columns 
md_md_yyyy_results <- mapply(choosing_month_or_date, first_md_2, second_md_2, SIMPLIFY=FALSE)

obs4$month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$month)
obs4$day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$day)
obs4$likely_month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_month)
obs4$likely_day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_day)

#confirm they were all assigned
#length(which(!is.na(obs4$day_when_md_md_yyyy))) 



# Section 5: Dealing with exceptions
#nb_no_date <- length(obs4$observed_on_string[is.na(obs4$observed_on_string)]) # when no date is listed
# very few (0.02%) have some non-NA entry other than the formats we've handled. deal with them here if one wishes to
#nb_entries_not_caught <- nrow(obs4) - sum(nb_alhpabetic, nb_md_md_yyyy, nb_yyyy_md_md, nb_no_date)



# Section 6: Assembling uniform date column 
# 6.1 assembling together different parts of the date
obs4$certain_dd <-
  ifelse(!is.na(obs4$day_when_month_alphabetical), obs4$day_when_month_alphabetical,
         ifelse(!is.na(obs4$day_when_yyyy_md_md), obs4$day_when_yyyy_md_md,
                ifelse(!is.na(obs4$day_when_md_md_yyyy), obs4$day_when_md_md_yyyy, NA)))

obs4$probable_dd <- 
  ifelse(!is.na(obs4$likely_day_when_yyyy_md_md), obs4$likely_day_when_yyyy_md_md,
         ifelse(!is.na(obs4$likely_day_when_md_md_yyyy), obs4$likely_day_when_md_md_yyyy, NA))

obs4$certain_mm <-
  ifelse(!is.na(obs4$month_numeric_when_alphabetic), obs4$month_numeric_when_alphabetic,
         ifelse(!is.na(obs4$month_when_yyyy_md_md), obs4$month_when_yyyy_md_md,
                ifelse(!is.na(obs4$month_when_md_md_yyyy), obs4$month_when_md_md_yyyy, NA)))

obs4$probable_mm <-
  ifelse(!is.na(obs4$likely_month_when_yyyy_md_md), obs4$likely_month_when_yyyy_md_md,
         ifelse(!is.na(obs4$likely_month_when_md_md_yyyy), obs4$likely_month_when_md_md_yyyy, NA))

# 6.2 assembling final columns
certain_dd <- ifelse(!is.na(obs4$certain_dd), obs4$certain_dd, "??")
certain_mm <- ifelse(!is.na(obs4$certain_mm), obs4$certain_mm, "??")
certain_yyyy <- ifelse(!is.na(obs4$year), obs4$year, "????")

obs4$certain_ddmmyyyy <-
  paste(
    ifelse(!is.na(obs4$certain_dd), obs4$certain_dd, "??"),
    ifelse(!is.na(obs4$certain_mm), obs4$certain_mm, "??"),
    ifelse(!is.na(obs4$year), obs4$year, "??"),
    sep="/")

obs4$probable_ddmmyyyy <-
  ifelse(
    (!is.na(obs4$probable_dd) & !is.na(obs4$probable_mm) & !is.na(obs4$year)),
    paste(obs4$probable_dd, obs4$probable_mm, obs4$year, sep="/"),
    NA)

obs4$standardized_ddmmyyyy <-
  ifelse((!is.na(obs4$probable_ddmmyyyy)), obs4$probable_ddmmyyyy, obs4$certain_ddmmyyyy)


# # Section 7: Assessing the quality of the "assumed" dates of ambiguous cases
# # when there is indeed a day > 12 (clear case), what percentage of them are in the first position? 
# # this serves as an estimate for the non-clear cases, to see how accurate our month-first assumption is
# nb_total_obs <- nrow(obs4)
# test_yyyy_md_md_string <- sapply(obs4$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
# positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", test_yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
# test_yyyy_md_md_vector <- regmatches(test_yyyy_md_md_string, positions_yyyy_md_md)
# test_first_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[3])))
# test_second_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[4])))
# 
# counting_error_rate <- function(first_number, second_number){
#   
#   both_na <- is.na(test_first_md) & is.na(second_number)
#   total_yyyy_md_md <- sum(!is.na(first_number))
#   first_md_greater_than_12 <- first_number > 12
#   second_md_greater_than_12 <- second_number > 12
#   ambiguous_cases <- !(both_na | first_md_greater_than_12 | second_md_greater_than_12)
#   
#   nb_first_md_gt12 <- sum(first_md_greater_than_12, na.rm = TRUE)
#   nb_second_md_gt12 <- sum(second_md_greater_than_12, na.rm = TRUE)
#   total_clear_cases <- nb_first_md_gt12 + nb_second_md_gt12
#   nb_ambiguous <- sum(ambiguous_cases, na.rm=TRUE)
#   nb_both_na <- sum(both_na)
#   
#   percent_of_clear_cases_with_day_first <- (nb_first_md_gt12/total_clear_cases)*100
#   percent_successful_guess_of_month_first <- round((100-percent_of_clear_cases_with_day_first), 8)
#   
#   return( list(
#     summary = paste(
#       c(
#         paste("total cases of yyyy-md-md: ", total_yyyy_md_md),
#         paste("ambiguous cases:", nb_ambiguous),
#         paste("clear cases:", total_clear_cases),
#         paste0("percent of all the observations caught under this search: ", (total_yyyy_md_md/nb_total_obs)*100,"%"),
#         paste0("percent of yyyy-md-md cases that are ambiguous: ", (nb_ambiguous/total_yyyy_md_md)*100,"%"),
#         paste0("percent of clear cases with day first: ", percent_of_clear_cases_with_day_first,"%")
#       ),
#       collapse="\n"
#     ),
#     percent_successful_guess_of_month_first = percent_successful_guess_of_month_first
#   ))
# }
# 
# 
# test_result <- counting_error_rate(test_first_md, test_second_md)
# cat(test_result$summary) # so 99.9996% of cases, the first number is the day
# percent_successful_guess_of_month_first <- test_result$percent_successful_guess_of_month_first
# 
# # Indicating this error rate in the df
# probable_date_string <- paste0("probable_ddmmyyyy_to_approx_", percent_successful_guess_of_month_first,"_percent_certainty")
# colnames(obs4)[colnames(obs4) == "probable_ddmmyyyy"] <- probable_date_string

# Creating easy column to identify certain dates over probable dates
obs4$is_ambiguous <- 
  ifelse(is.na(obs4$probable_ddmmyyyy),
         0,
         1)

# # Screen for months that should not be
# improper_months_regex <- paste0("^\\d{1,2}/(", paste0(13:99, collapse = "|"), ")/\\d{4}$")
# erroneous_months <- which(!is.na(str_extract(obs4$standardized_ddmmyyyy, improper_months_regex)))
# erroneous_months_df <- obs4[erroneous_months,]

# Section 8: Writing out data with unified columns to desired location
obs4$year <- NULL
obs4$month_alphabetic <- NULL
obs4$day_when_month_alphabetical <- NULL
obs4$yyyy_md_md <- NULL
obs4$month_when_yyyy_md_md <- NULL
obs4$day_when_yyyy_md_md <- NULL
obs4$likely_month_when_yyyy_md_md <- NULL
obs4$likely_day_when_yyyy_md_md <- NULL
obs4$md_md_yyyy <- NULL
obs4$month_when_md_md_yyyy <- NULL
obs4$day_when_md_md_yyyy <- NULL
obs4$likely_month_when_md_md_yyyy <- NULL
obs4$likely_day_when_md_md_yyyy <- NULL
obs4$certain_dd <- NULL
obs4$probable_dd <- NULL
obs4$certain_mm <- NULL
obs4$probable_mm <- NULL
obs4$certain_ddmmyyyy <- NULL
obs4$probable_ddmmyyyy <- NULL
obs4$just_month_when_alphabetic <- NULL
obs4$month_numeric_when_alphabetic <- NULL





#################################5##########################

# Section 2: Extracting the year
#2.1 first pass - grep any set of 4 numbers
obs5$year4num <- regmatches(obs5$observed_on_string, gregexpr("\\d{4}", obs5$observed_on_string))
obs5$year <- obs5$year4num
obs5$year <- sapply(obs5$year4num, function(x){ ifelse(length(x)==0,NA,x[1])}) # to NA the ones with no year
#length(which(is.na(obs5$year))) # about 95% of these cause theres no entry at all. 5% have an entry without a year

#2.2 second pass - correct those with multiple entries of 4 numbers
twos_indx <- which(sapply(obs5$year4num, length) == 2)

year_only <- lapply(obs5$year4num[twos_indx], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 1800 & x_num < 2026] # the plausible 4digit is used
})
obs5$year[twos_indx] <- year_only

#2.3 Any more still left? Then refine date inclusion criteria
twos_indx_still <- which(sapply(obs5$year, length) == 2)
obs5$observed_on_string[twos_indx_still] # displays the dates

# All years 2015 and 2016 with manual check, so use >2000. Change this if need be for different dfs
year_only <- lapply(obs5$year[twos_indx_still], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 2000]
})
obs5$year[twos_indx_still] <- year_only

# 2.4 Final check - any left? If so manually edit your df
#length(which(sapply(obs5$year, length) == 2)) # if empty, no further checks needed
obs5$year4num <- NULL




# Section 3 - Extracting the Month and dates when month listed as letters

# create a lookup table
month_lookup <- c(
  Jan=1, January=1, janv=1, janvier=1, jan=1, january=1,
  Feb=2, February=2, Fév=2, fév=2, fevrier=2, février=2, feb=2, february=2,
  Mar=3, March=3, mars=3, mar=3, march=3,
  Apr=4, April=4, avr=4, avril=4, apr=4, april=4,
  May=5, mai=5, may=5,
  Jun=6, June=6, juin=6, jun=6, june=6,
  Jul=7, July=7, juil=7, juillet=7, jul=7, july=7,
  Aug=8, August=8, aout=8, août=8, aug=8, august=8,
  Sep=9, Sept=9, September=9, sept=9, septembre=9, sep=9, september=9,
  Oct=10, October=10, oct=10, octobre=10, oct=10, october=10,
  Nov=11, November=11, nov=11, novembre=11, nov=11, november=11,
  Dec=12, December=12, déc=12, dec=12, décembre=12, dec=12, december=12
)

# create regex and use str_extract to take out the month
months_alphabetic_regex <- paste0("\\b(", paste(names(month_lookup), collapse="|"), ")\\b")
obs5$just_month_when_alphabetic <- str_extract(obs5$observed_on_string, months_alphabetic_regex)
#nb_alhpabetic <- length(which(!is.na(obs5$just_month_when_alphabetic))) # roughly 15% of cases

# assigning it to new column
obs5$month_numeric_when_alphabetic <- sapply(obs5$just_month_when_alphabetic, function(x){
  if (!is.na(x)){
    month_lookup[[x]]
  }
  else{
    NA
  }
})

# Finding the dates from these rows - should be the only " XX " in the string
obs5$day_when_month_alphabetical <- NA
rows_with_alphabetical_month <- !is.na(obs5$month_numeric_when_alphabetic)
positions_alpha_day <- regexec("\\s(\\d{2})\\s", obs5$observed_on_string[rows_with_alphabetical_month])
day_when_alpha <- regmatches(obs5$observed_on_string[rows_with_alphabetical_month], positions_alpha_day)
obs5$day_when_month_alphabetical[rows_with_alphabetical_month] <- 
  sapply(day_when_alpha, function(x) x[2])

#looking for errors
#length(which(sapply(obs5$day_when_month_alphabetical, length)> 1)) # any double dates? should be none
#length(which(obs5$day_when_month_alphabetical > 31))# any false dates? should be none


# Section 4: Extracting months and dates when listed as yyyy-m?d?-m?d?
# Issue: many formats (yyyy-mm-dd, yyyy-dd-mm, yyyy-d-m, yyyy-m-d)

# New column to contain only dates of this format
obs5$yyyy_md_md <- regmatches(obs5$observed_on_string, gregexpr("\\d{4}[/-]\\d{2}[/-]\\d{2}", obs5$observed_on_string))
obs5$yyyy_md_md[sapply(obs5$yyyy_md_md, length)==0] <- NA
#nb_yyyy_md_md <- length(which(!is.na(obs5$yyyy_md_md))) # the vast majority

# Extracting the numbers
obs5$yyyy_md_md_string <- sapply(obs5$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", obs5$yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
obs5$yyyy_md_md_vector <- regmatches(obs5$yyyy_md_md_string, positions_yyyy_md_md)
obs5$yyyy_md_md_string <- NULL

first_md <- as.integer(sapply(obs5$yyyy_md_md_vector, function(x) return(x[3])))
second_md <- as.integer(sapply(obs5$yyyy_md_md_vector, function(x) return(x[4])))
obs5$yyyy_md_md_vector <- NULL

# Creating function to extract date and month based on the values of first_md and second_md.
# It seems like the vast majority should be month first.
choosing_month_or_date <- function(first_number, second_number){
  
  day <- NA
  month <- NA
  likely_day <- NA
  likely_month <- NA
  
  if (is.na(first_number) && is.na(second_number)){
    # do nothing
  }
  else if (first_number > 12){
    day <- first_number
    month <- second_number
  }
  else if (second_number > 12){
    day <- second_number
    month <- first_number
  }
  else { # when ambiguous, assume first number is the month but mark as ambiguous
    likely_day <- second_number
    likely_month <- first_number
  }
  return(list(day=day,month=month,likely_day=likely_day,likely_month=likely_month))
}

# Applying the function to our data and assigning results to columns 
yyyy_md_md_results <- mapply(choosing_month_or_date,first_md, second_md, SIMPLIFY=FALSE)

obs5$month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$month)
obs5$day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$day)
obs5$likely_month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_month)
obs5$likely_day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_day)



# Section 5: Extracting months and dates when listed as md-md-yyyy - there are very few
# Issue:  many formats (mm-dd-yyyy, dd-mm-yyyy, d-m-yyyy, m-d-yyyy)

# New column to contain only dates of this format
obs5$md_md_yyyy <- regmatches(obs5$observed_on_string, gregexpr("\\d{1,2}[-/]\\d{1,2}[-/]\\d{4}", obs5$observed_on_string))
obs5$md_md_yyyy[sapply(obs5$md_md_yyyy, length)==0] <- NA
#nb_md_md_yyyy <- length(which(!is.na(obs5$md_md_yyyy))) # very few

# Extract the numbers
obs5$md_md_yyyy_string <- sapply(obs5$md_md_yyyy, function(x) if(length(x)==0) NA else x[1])
positions_md_md_yyyy <- regexec("(\\d{1,2})[-/](\\d{1,2})[-/](\\d{4})", obs5$md_md_yyyy_string)
obs5$md_md_yyyy_vector <- NA
obs5$md_md_yyyy_vector <- regmatches(obs5$md_md_yyyy_string, positions_md_md_yyyy)
obs5$md_md_yyyy_string <- NULL

first_md_2 <- as.integer(sapply(obs5$md_md_yyyy_vector, function(x) return(x[3])))
second_md_2 <- as.integer(sapply(obs5$md_md_yyyy_vector, function(x) return(x[4])))
obs5$md_md_yyyy_vector <- NULL

# Applying the function to our data and assigning results to columns 
md_md_yyyy_results <- mapply(choosing_month_or_date, first_md_2, second_md_2, SIMPLIFY=FALSE)

obs5$month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$month)
obs5$day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$day)
obs5$likely_month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_month)
obs5$likely_day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_day)

#confirm they were all assigned
#length(which(!is.na(obs5$day_when_md_md_yyyy))) 



# Section 5: Dealing with exceptions
#nb_no_date <- length(obs5$observed_on_string[is.na(obs5$observed_on_string)]) # when no date is listed
# very few (0.02%) have some non-NA entry other than the formats we've handled. deal with them here if one wishes to
#nb_entries_not_caught <- nrow(obs5) - sum(nb_alhpabetic, nb_md_md_yyyy, nb_yyyy_md_md, nb_no_date)



# Section 6: Assembling uniform date column 
# 6.1 assembling together different parts of the date
obs5$certain_dd <-
  ifelse(!is.na(obs5$day_when_month_alphabetical), obs5$day_when_month_alphabetical,
         ifelse(!is.na(obs5$day_when_yyyy_md_md), obs5$day_when_yyyy_md_md,
                ifelse(!is.na(obs5$day_when_md_md_yyyy), obs5$day_when_md_md_yyyy, NA)))

obs5$probable_dd <- 
  ifelse(!is.na(obs5$likely_day_when_yyyy_md_md), obs5$likely_day_when_yyyy_md_md,
         ifelse(!is.na(obs5$likely_day_when_md_md_yyyy), obs5$likely_day_when_md_md_yyyy, NA))

obs5$certain_mm <-
  ifelse(!is.na(obs5$month_numeric_when_alphabetic), obs5$month_numeric_when_alphabetic,
         ifelse(!is.na(obs5$month_when_yyyy_md_md), obs5$month_when_yyyy_md_md,
                ifelse(!is.na(obs5$month_when_md_md_yyyy), obs5$month_when_md_md_yyyy, NA)))

obs5$probable_mm <-
  ifelse(!is.na(obs5$likely_month_when_yyyy_md_md), obs5$likely_month_when_yyyy_md_md,
         ifelse(!is.na(obs5$likely_month_when_md_md_yyyy), obs5$likely_month_when_md_md_yyyy, NA))

# 6.2 assembling final columns
certain_dd <- ifelse(!is.na(obs5$certain_dd), obs5$certain_dd, "??")
certain_mm <- ifelse(!is.na(obs5$certain_mm), obs5$certain_mm, "??")
certain_yyyy <- ifelse(!is.na(obs5$year), obs5$year, "????")

obs5$certain_ddmmyyyy <-
  paste(
    ifelse(!is.na(obs5$certain_dd), obs5$certain_dd, "??"),
    ifelse(!is.na(obs5$certain_mm), obs5$certain_mm, "??"),
    ifelse(!is.na(obs5$year), obs5$year, "??"),
    sep="/")

obs5$probable_ddmmyyyy <-
  ifelse(
    (!is.na(obs5$probable_dd) & !is.na(obs5$probable_mm) & !is.na(obs5$year)),
    paste(obs5$probable_dd, obs5$probable_mm, obs5$year, sep="/"),
    NA)

obs5$standardized_ddmmyyyy <-
  ifelse((!is.na(obs5$probable_ddmmyyyy)), obs5$probable_ddmmyyyy, obs5$certain_ddmmyyyy)


# # Section 7: Assessing the quality of the "assumed" dates of ambiguous cases
# # when there is indeed a day > 12 (clear case), what percentage of them are in the first position? 
# # this serves as an estimate for the non-clear cases, to see how accurate our month-first assumption is
# nb_total_obs <- nrow(obs5)
# test_yyyy_md_md_string <- sapply(obs5$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
# positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", test_yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
# test_yyyy_md_md_vector <- regmatches(test_yyyy_md_md_string, positions_yyyy_md_md)
# test_first_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[3])))
# test_second_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[4])))
# 
# counting_error_rate <- function(first_number, second_number){
#   
#   both_na <- is.na(test_first_md) & is.na(second_number)
#   total_yyyy_md_md <- sum(!is.na(first_number))
#   first_md_greater_than_12 <- first_number > 12
#   second_md_greater_than_12 <- second_number > 12
#   ambiguous_cases <- !(both_na | first_md_greater_than_12 | second_md_greater_than_12)
#   
#   nb_first_md_gt12 <- sum(first_md_greater_than_12, na.rm = TRUE)
#   nb_second_md_gt12 <- sum(second_md_greater_than_12, na.rm = TRUE)
#   total_clear_cases <- nb_first_md_gt12 + nb_second_md_gt12
#   nb_ambiguous <- sum(ambiguous_cases, na.rm=TRUE)
#   nb_both_na <- sum(both_na)
#   
#   percent_of_clear_cases_with_day_first <- (nb_first_md_gt12/total_clear_cases)*100
#   percent_successful_guess_of_month_first <- round((100-percent_of_clear_cases_with_day_first), 8)
#   
#   return( list(
#     summary = paste(
#       c(
#         paste("total cases of yyyy-md-md: ", total_yyyy_md_md),
#         paste("ambiguous cases:", nb_ambiguous),
#         paste("clear cases:", total_clear_cases),
#         paste0("percent of all the observations caught under this search: ", (total_yyyy_md_md/nb_total_obs)*100,"%"),
#         paste0("percent of yyyy-md-md cases that are ambiguous: ", (nb_ambiguous/total_yyyy_md_md)*100,"%"),
#         paste0("percent of clear cases with day first: ", percent_of_clear_cases_with_day_first,"%")
#       ),
#       collapse="\n"
#     ),
#     percent_successful_guess_of_month_first = percent_successful_guess_of_month_first
#   ))
# }
# 
# 
# test_result <- counting_error_rate(test_first_md, test_second_md)
# cat(test_result$summary) # so 99.9996% of cases, the first number is the day
# percent_successful_guess_of_month_first <- test_result$percent_successful_guess_of_month_first
# 
# # Indicating this error rate in the df
# probable_date_string <- paste0("probable_ddmmyyyy_to_approx_", percent_successful_guess_of_month_first,"_percent_certainty")
# colnames(obs5)[colnames(obs5) == "probable_ddmmyyyy"] <- probable_date_string

# Creating easy column to identify certain dates over probable dates
obs5$is_ambiguous <- 
  ifelse(is.na(obs5$probable_ddmmyyyy),
         0,
         1)

# # Screen for months that should not be
# improper_months_regex <- paste0("^\\d{1,2}/(", paste0(13:99, collapse = "|"), ")/\\d{4}$")
# erroneous_months <- which(!is.na(str_extract(obs5$standardized_ddmmyyyy, improper_months_regex)))
# erroneous_months_df <- obs5[erroneous_months,]

# Section 8: Writing out data with unified columns to desired location
obs5$year <- NULL
obs5$month_alphabetic <- NULL
obs5$day_when_month_alphabetical <- NULL
obs5$yyyy_md_md <- NULL
obs5$month_when_yyyy_md_md <- NULL
obs5$day_when_yyyy_md_md <- NULL
obs5$likely_month_when_yyyy_md_md <- NULL
obs5$likely_day_when_yyyy_md_md <- NULL
obs5$md_md_yyyy <- NULL
obs5$month_when_md_md_yyyy <- NULL
obs5$day_when_md_md_yyyy <- NULL
obs5$likely_month_when_md_md_yyyy <- NULL
obs5$likely_day_when_md_md_yyyy <- NULL
obs5$certain_dd <- NULL
obs5$probable_dd <- NULL
obs5$certain_mm <- NULL
obs5$probable_mm <- NULL
obs5$certain_ddmmyyyy <- NULL
obs5$probable_ddmmyyyy <- NULL
obs5$just_month_when_alphabetic <- NULL
obs5$month_numeric_when_alphabetic <- NULL







#################################### 6 #########################


# Section 2: Extracting the year
#2.1 first pass - grep any set of 4 numbers
obs6$year4num <- regmatches(obs6$observed_on_string, gregexpr("\\d{4}", obs6$observed_on_string))
obs6$year <- obs6$year4num
obs6$year <- sapply(obs6$year4num, function(x){ ifelse(length(x)==0,NA,x[1])}) # to NA the ones with no year
#length(which(is.na(obs6$year))) # about 95% of these cause theres no entry at all. 5% have an entry without a year

#2.2 second pass - correct those with multiple entries of 4 numbers
twos_indx <- which(sapply(obs6$year4num, length) == 2)

year_only <- lapply(obs6$year4num[twos_indx], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 1800 & x_num < 2026] # the plausible 4digit is used
})
obs6$year[twos_indx] <- year_only

#2.3 Any more still left? Then refine date inclusion criteria
twos_indx_still <- which(sapply(obs6$year, length) == 2)
obs6$observed_on_string[twos_indx_still] # displays the dates

# All years 2015 and 2016 with manual check, so use >2000. Change this if need be for different dfs
year_only <- lapply(obs6$year[twos_indx_still], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 2000]
})
obs6$year[twos_indx_still] <- year_only

# 2.4 Final check - any left? If so manually edit your df
#length(which(sapply(obs6$year, length) == 2)) # if empty, no further checks needed
obs6$year4num <- NULL




# Section 3 - Extracting the Month and dates when month listed as letters

# create a lookup table
month_lookup <- c(
  Jan=1, January=1, janv=1, janvier=1, jan=1, january=1,
  Feb=2, February=2, Fév=2, fév=2, fevrier=2, février=2, feb=2, february=2,
  Mar=3, March=3, mars=3, mar=3, march=3,
  Apr=4, April=4, avr=4, avril=4, apr=4, april=4,
  May=5, mai=5, may=5,
  Jun=6, June=6, juin=6, jun=6, june=6,
  Jul=7, July=7, juil=7, juillet=7, jul=7, july=7,
  Aug=8, August=8, aout=8, août=8, aug=8, august=8,
  Sep=9, Sept=9, September=9, sept=9, septembre=9, sep=9, september=9,
  Oct=10, October=10, oct=10, octobre=10, oct=10, october=10,
  Nov=11, November=11, nov=11, novembre=11, nov=11, november=11,
  Dec=12, December=12, déc=12, dec=12, décembre=12, dec=12, december=12
)

# create regex and use str_extract to take out the month
months_alphabetic_regex <- paste0("\\b(", paste(names(month_lookup), collapse="|"), ")\\b")
obs6$just_month_when_alphabetic <- str_extract(obs6$observed_on_string, months_alphabetic_regex)
#nb_alhpabetic <- length(which(!is.na(obs6$just_month_when_alphabetic))) # roughly 15% of cases

# assigning it to new column
obs6$month_numeric_when_alphabetic <- sapply(obs6$just_month_when_alphabetic, function(x){
  if (!is.na(x)){
    month_lookup[[x]]
  }
  else{
    NA
  }
})

# Finding the dates from these rows - should be the only " XX " in the string
obs6$day_when_month_alphabetical <- NA
rows_with_alphabetical_month <- !is.na(obs6$month_numeric_when_alphabetic)
positions_alpha_day <- regexec("\\s(\\d{2})\\s", obs6$observed_on_string[rows_with_alphabetical_month])
day_when_alpha <- regmatches(obs6$observed_on_string[rows_with_alphabetical_month], positions_alpha_day)
obs6$day_when_month_alphabetical[rows_with_alphabetical_month] <- 
  sapply(day_when_alpha, function(x) x[2])

#looking for errors
#length(which(sapply(obs6$day_when_month_alphabetical, length)> 1)) # any double dates? should be none
#length(which(obs6$day_when_month_alphabetical > 31))# any false dates? should be none


# Section 4: Extracting months and dates when listed as yyyy-m?d?-m?d?
# Issue: many formats (yyyy-mm-dd, yyyy-dd-mm, yyyy-d-m, yyyy-m-d)

# New column to contain only dates of this format
obs6$yyyy_md_md <- regmatches(obs6$observed_on_string, gregexpr("\\d{4}[/-]\\d{2}[/-]\\d{2}", obs6$observed_on_string))
obs6$yyyy_md_md[sapply(obs6$yyyy_md_md, length)==0] <- NA
#nb_yyyy_md_md <- length(which(!is.na(obs6$yyyy_md_md))) # the vast majority

# Extracting the numbers
obs6$yyyy_md_md_string <- sapply(obs6$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", obs6$yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
obs6$yyyy_md_md_vector <- regmatches(obs6$yyyy_md_md_string, positions_yyyy_md_md)
obs6$yyyy_md_md_string <- NULL

first_md <- as.integer(sapply(obs6$yyyy_md_md_vector, function(x) return(x[3])))
second_md <- as.integer(sapply(obs6$yyyy_md_md_vector, function(x) return(x[4])))
obs6$yyyy_md_md_vector <- NULL

# Creating function to extract date and month based on the values of first_md and second_md.
# It seems like the vast majority should be month first.
choosing_month_or_date <- function(first_number, second_number){
  
  day <- NA
  month <- NA
  likely_day <- NA
  likely_month <- NA
  
  if (is.na(first_number) && is.na(second_number)){
    # do nothing
  }
  else if (first_number > 12){
    day <- first_number
    month <- second_number
  }
  else if (second_number > 12){
    day <- second_number
    month <- first_number
  }
  else { # when ambiguous, assume first number is the month but mark as ambiguous
    likely_day <- second_number
    likely_month <- first_number
  }
  return(list(day=day,month=month,likely_day=likely_day,likely_month=likely_month))
}

# Applying the function to our data and assigning results to columns 
yyyy_md_md_results <- mapply(choosing_month_or_date,first_md, second_md, SIMPLIFY=FALSE)

obs6$month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$month)
obs6$day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$day)
obs6$likely_month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_month)
obs6$likely_day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_day)



# Section 5: Extracting months and dates when listed as md-md-yyyy - there are very few
# Issue:  many formats (mm-dd-yyyy, dd-mm-yyyy, d-m-yyyy, m-d-yyyy)

# New column to contain only dates of this format
obs6$md_md_yyyy <- regmatches(obs6$observed_on_string, gregexpr("\\d{1,2}[-/]\\d{1,2}[-/]\\d{4}", obs6$observed_on_string))
obs6$md_md_yyyy[sapply(obs6$md_md_yyyy, length)==0] <- NA
#nb_md_md_yyyy <- length(which(!is.na(obs6$md_md_yyyy))) # very few

# Extract the numbers
obs6$md_md_yyyy_string <- sapply(obs6$md_md_yyyy, function(x) if(length(x)==0) NA else x[1])
positions_md_md_yyyy <- regexec("(\\d{1,2})[-/](\\d{1,2})[-/](\\d{4})", obs6$md_md_yyyy_string)
obs6$md_md_yyyy_vector <- NA
obs6$md_md_yyyy_vector <- regmatches(obs6$md_md_yyyy_string, positions_md_md_yyyy)
obs6$md_md_yyyy_string <- NULL

first_md_2 <- as.integer(sapply(obs6$md_md_yyyy_vector, function(x) return(x[3])))
second_md_2 <- as.integer(sapply(obs6$md_md_yyyy_vector, function(x) return(x[4])))
obs6$md_md_yyyy_vector <- NULL

# Applying the function to our data and assigning results to columns 
md_md_yyyy_results <- mapply(choosing_month_or_date, first_md_2, second_md_2, SIMPLIFY=FALSE)

obs6$month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$month)
obs6$day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$day)
obs6$likely_month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_month)
obs6$likely_day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_day)

#confirm they were all assigned
#length(which(!is.na(obs6$day_when_md_md_yyyy))) 



# Section 5: Dealing with exceptions
#nb_no_date <- length(obs6$observed_on_string[is.na(obs6$observed_on_string)]) # when no date is listed
# very few (0.02%) have some non-NA entry other than the formats we've handled. deal with them here if one wishes to
#nb_entries_not_caught <- nrow(obs6) - sum(nb_alhpabetic, nb_md_md_yyyy, nb_yyyy_md_md, nb_no_date)



# Section 6: Assembling uniform date column 
# 6.1 assembling together different parts of the date
obs6$certain_dd <-
  ifelse(!is.na(obs6$day_when_month_alphabetical), obs6$day_when_month_alphabetical,
         ifelse(!is.na(obs6$day_when_yyyy_md_md), obs6$day_when_yyyy_md_md,
                ifelse(!is.na(obs6$day_when_md_md_yyyy), obs6$day_when_md_md_yyyy, NA)))

obs6$probable_dd <- 
  ifelse(!is.na(obs6$likely_day_when_yyyy_md_md), obs6$likely_day_when_yyyy_md_md,
         ifelse(!is.na(obs6$likely_day_when_md_md_yyyy), obs6$likely_day_when_md_md_yyyy, NA))

obs6$certain_mm <-
  ifelse(!is.na(obs6$month_numeric_when_alphabetic), obs6$month_numeric_when_alphabetic,
         ifelse(!is.na(obs6$month_when_yyyy_md_md), obs6$month_when_yyyy_md_md,
                ifelse(!is.na(obs6$month_when_md_md_yyyy), obs6$month_when_md_md_yyyy, NA)))

obs6$probable_mm <-
  ifelse(!is.na(obs6$likely_month_when_yyyy_md_md), obs6$likely_month_when_yyyy_md_md,
         ifelse(!is.na(obs6$likely_month_when_md_md_yyyy), obs6$likely_month_when_md_md_yyyy, NA))

# 6.2 assembling final columns
certain_dd <- ifelse(!is.na(obs6$certain_dd), obs6$certain_dd, "??")
certain_mm <- ifelse(!is.na(obs6$certain_mm), obs6$certain_mm, "??")
certain_yyyy <- ifelse(!is.na(obs6$year), obs6$year, "????")

obs6$certain_ddmmyyyy <-
  paste(
    ifelse(!is.na(obs6$certain_dd), obs6$certain_dd, "??"),
    ifelse(!is.na(obs6$certain_mm), obs6$certain_mm, "??"),
    ifelse(!is.na(obs6$year), obs6$year, "??"),
    sep="/")

obs6$probable_ddmmyyyy <-
  ifelse(
    (!is.na(obs6$probable_dd) & !is.na(obs6$probable_mm) & !is.na(obs6$year)),
    paste(obs6$probable_dd, obs6$probable_mm, obs6$year, sep="/"),
    NA)

obs6$standardized_ddmmyyyy <-
  ifelse((!is.na(obs6$probable_ddmmyyyy)), obs6$probable_ddmmyyyy, obs6$certain_ddmmyyyy)


# # Section 7: Assessing the quality of the "assumed" dates of ambiguous cases
# # when there is indeed a day > 12 (clear case), what percentage of them are in the first position? 
# # this serves as an estimate for the non-clear cases, to see how accurate our month-first assumption is
# nb_total_obs <- nrow(obs6)
# test_yyyy_md_md_string <- sapply(obs6$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
# positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", test_yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
# test_yyyy_md_md_vector <- regmatches(test_yyyy_md_md_string, positions_yyyy_md_md)
# test_first_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[3])))
# test_second_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[4])))
# 
# counting_error_rate <- function(first_number, second_number){
#   
#   both_na <- is.na(test_first_md) & is.na(second_number)
#   total_yyyy_md_md <- sum(!is.na(first_number))
#   first_md_greater_than_12 <- first_number > 12
#   second_md_greater_than_12 <- second_number > 12
#   ambiguous_cases <- !(both_na | first_md_greater_than_12 | second_md_greater_than_12)
#   
#   nb_first_md_gt12 <- sum(first_md_greater_than_12, na.rm = TRUE)
#   nb_second_md_gt12 <- sum(second_md_greater_than_12, na.rm = TRUE)
#   total_clear_cases <- nb_first_md_gt12 + nb_second_md_gt12
#   nb_ambiguous <- sum(ambiguous_cases, na.rm=TRUE)
#   nb_both_na <- sum(both_na)
#   
#   percent_of_clear_cases_with_day_first <- (nb_first_md_gt12/total_clear_cases)*100
#   percent_successful_guess_of_month_first <- round((100-percent_of_clear_cases_with_day_first), 8)
#   
#   return( list(
#     summary = paste(
#       c(
#         paste("total cases of yyyy-md-md: ", total_yyyy_md_md),
#         paste("ambiguous cases:", nb_ambiguous),
#         paste("clear cases:", total_clear_cases),
#         paste0("percent of all the observations caught under this search: ", (total_yyyy_md_md/nb_total_obs)*100,"%"),
#         paste0("percent of yyyy-md-md cases that are ambiguous: ", (nb_ambiguous/total_yyyy_md_md)*100,"%"),
#         paste0("percent of clear cases with day first: ", percent_of_clear_cases_with_day_first,"%")
#       ),
#       collapse="\n"
#     ),
#     percent_successful_guess_of_month_first = percent_successful_guess_of_month_first
#   ))
# }
# 
# 
# test_result <- counting_error_rate(test_first_md, test_second_md)
# cat(test_result$summary) # so 99.9996% of cases, the first number is the day
# percent_successful_guess_of_month_first <- test_result$percent_successful_guess_of_month_first
# 
# # Indicating this error rate in the df
# probable_date_string <- paste0("probable_ddmmyyyy_to_approx_", percent_successful_guess_of_month_first,"_percent_certainty")
# colnames(obs6)[colnames(obs6) == "probable_ddmmyyyy"] <- probable_date_string

# Creating easy column to identify certain dates over probable dates
obs6$is_ambiguous <- 
  ifelse(is.na(obs6$probable_ddmmyyyy),
         0,
         1)

# # Screen for months that should not be
# improper_months_regex <- paste0("^\\d{1,2}/(", paste0(13:99, collapse = "|"), ")/\\d{4}$")
# erroneous_months <- which(!is.na(str_extract(obs6$standardized_ddmmyyyy, improper_months_regex)))
# erroneous_months_df <- obs6[erroneous_months,]

# Section 8: Writing out data with unified columns to desired location
obs6$year <- NULL
obs6$month_alphabetic <- NULL
obs6$day_when_month_alphabetical <- NULL
obs6$yyyy_md_md <- NULL
obs6$month_when_yyyy_md_md <- NULL
obs6$day_when_yyyy_md_md <- NULL
obs6$likely_month_when_yyyy_md_md <- NULL
obs6$likely_day_when_yyyy_md_md <- NULL
obs6$md_md_yyyy <- NULL
obs6$month_when_md_md_yyyy <- NULL
obs6$day_when_md_md_yyyy <- NULL
obs6$likely_month_when_md_md_yyyy <- NULL
obs6$likely_day_when_md_md_yyyy <- NULL
obs6$certain_dd <- NULL
obs6$probable_dd <- NULL
obs6$certain_mm <- NULL
obs6$probable_mm <- NULL
obs6$certain_ddmmyyyy <- NULL
obs6$probable_ddmmyyyy <- NULL
obs6$just_month_when_alphabetic <- NULL
obs6$month_numeric_when_alphabetic <- NULL









####################################### 7 ################################

# Section 2: Extracting the year
#2.1 first pass - grep any set of 4 numbers
obs7$year4num <- regmatches(obs7$observed_on_string, gregexpr("\\d{4}", obs7$observed_on_string))
obs7$year <- obs7$year4num
obs7$year <- sapply(obs7$year4num, function(x){ ifelse(length(x)==0,NA,x[1])}) # to NA the ones with no year
#length(which(is.na(obs7$year))) # about 95% of these cause theres no entry at all. 5% have an entry without a year

#2.2 second pass - correct those with multiple entries of 4 numbers
twos_indx <- which(sapply(obs7$year4num, length) == 2)

year_only <- lapply(obs7$year4num[twos_indx], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 1800 & x_num < 2026] # the plausible 4digit is used
})
obs7$year[twos_indx] <- year_only

#2.3 Any more still left? Then refine date inclusion criteria
twos_indx_still <- which(sapply(obs7$year, length) == 2)
obs7$observed_on_string[twos_indx_still] # displays the dates

# All years 2015 and 2016 with manual check, so use >2000. Change this if need be for different dfs
year_only <- lapply(obs7$year[twos_indx_still], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 2000]
})
obs7$year[twos_indx_still] <- year_only

# 2.4 Final check - any left? If so manually edit your df
#length(which(sapply(obs7$year, length) == 2)) # if empty, no further checks needed
obs7$year4num <- NULL




# Section 3 - Extracting the Month and dates when month listed as letters

# create a lookup table
month_lookup <- c(
  Jan=1, January=1, janv=1, janvier=1, jan=1, january=1,
  Feb=2, February=2, Fév=2, fév=2, fevrier=2, février=2, feb=2, february=2,
  Mar=3, March=3, mars=3, mar=3, march=3,
  Apr=4, April=4, avr=4, avril=4, apr=4, april=4,
  May=5, mai=5, may=5,
  Jun=6, June=6, juin=6, jun=6, june=6,
  Jul=7, July=7, juil=7, juillet=7, jul=7, july=7,
  Aug=8, August=8, aout=8, août=8, aug=8, august=8,
  Sep=9, Sept=9, September=9, sept=9, septembre=9, sep=9, september=9,
  Oct=10, October=10, oct=10, octobre=10, oct=10, october=10,
  Nov=11, November=11, nov=11, novembre=11, nov=11, november=11,
  Dec=12, December=12, déc=12, dec=12, décembre=12, dec=12, december=12
)

# create regex and use str_extract to take out the month
months_alphabetic_regex <- paste0("\\b(", paste(names(month_lookup), collapse="|"), ")\\b")
obs7$just_month_when_alphabetic <- str_extract(obs7$observed_on_string, months_alphabetic_regex)
#nb_alhpabetic <- length(which(!is.na(obs7$just_month_when_alphabetic))) # roughly 15% of cases

# assigning it to new column
obs7$month_numeric_when_alphabetic <- sapply(obs7$just_month_when_alphabetic, function(x){
  if (!is.na(x)){
    month_lookup[[x]]
  }
  else{
    NA
  }
})

# Finding the dates from these rows - should be the only " XX " in the string
obs7$day_when_month_alphabetical <- NA
rows_with_alphabetical_month <- !is.na(obs7$month_numeric_when_alphabetic)
positions_alpha_day <- regexec("\\s(\\d{2})\\s", obs7$observed_on_string[rows_with_alphabetical_month])
day_when_alpha <- regmatches(obs7$observed_on_string[rows_with_alphabetical_month], positions_alpha_day)
obs7$day_when_month_alphabetical[rows_with_alphabetical_month] <- 
  sapply(day_when_alpha, function(x) x[2])

#looking for errors
#length(which(sapply(obs7$day_when_month_alphabetical, length)> 1)) # any double dates? should be none
#length(which(obs7$day_when_month_alphabetical > 31))# any false dates? should be none


# Section 4: Extracting months and dates when listed as yyyy-m?d?-m?d?
# Issue: many formats (yyyy-mm-dd, yyyy-dd-mm, yyyy-d-m, yyyy-m-d)

# New column to contain only dates of this format
obs7$yyyy_md_md <- regmatches(obs7$observed_on_string, gregexpr("\\d{4}[/-]\\d{2}[/-]\\d{2}", obs7$observed_on_string))
obs7$yyyy_md_md[sapply(obs7$yyyy_md_md, length)==0] <- NA
#nb_yyyy_md_md <- length(which(!is.na(obs7$yyyy_md_md))) # the vast majority

# Extracting the numbers
obs7$yyyy_md_md_string <- sapply(obs7$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", obs7$yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
obs7$yyyy_md_md_vector <- regmatches(obs7$yyyy_md_md_string, positions_yyyy_md_md)
obs7$yyyy_md_md_string <- NULL

first_md <- as.integer(sapply(obs7$yyyy_md_md_vector, function(x) return(x[3])))
second_md <- as.integer(sapply(obs7$yyyy_md_md_vector, function(x) return(x[4])))
obs7$yyyy_md_md_vector <- NULL

# Creating function to extract date and month based on the values of first_md and second_md.
# It seems like the vast majority should be month first.
choosing_month_or_date <- function(first_number, second_number){
  
  day <- NA
  month <- NA
  likely_day <- NA
  likely_month <- NA
  
  if (is.na(first_number) && is.na(second_number)){
    # do nothing
  }
  else if (first_number > 12){
    day <- first_number
    month <- second_number
  }
  else if (second_number > 12){
    day <- second_number
    month <- first_number
  }
  else { # when ambiguous, assume first number is the month but mark as ambiguous
    likely_day <- second_number
    likely_month <- first_number
  }
  return(list(day=day,month=month,likely_day=likely_day,likely_month=likely_month))
}

# Applying the function to our data and assigning results to columns 
yyyy_md_md_results <- mapply(choosing_month_or_date,first_md, second_md, SIMPLIFY=FALSE)

obs7$month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$month)
obs7$day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$day)
obs7$likely_month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_month)
obs7$likely_day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_day)



# Section 5: Extracting months and dates when listed as md-md-yyyy - there are very few
# Issue:  many formats (mm-dd-yyyy, dd-mm-yyyy, d-m-yyyy, m-d-yyyy)

# New column to contain only dates of this format
obs7$md_md_yyyy <- regmatches(obs7$observed_on_string, gregexpr("\\d{1,2}[-/]\\d{1,2}[-/]\\d{4}", obs7$observed_on_string))
obs7$md_md_yyyy[sapply(obs7$md_md_yyyy, length)==0] <- NA
#nb_md_md_yyyy <- length(which(!is.na(obs7$md_md_yyyy))) # very few

# Extract the numbers
obs7$md_md_yyyy_string <- sapply(obs7$md_md_yyyy, function(x) if(length(x)==0) NA else x[1])
positions_md_md_yyyy <- regexec("(\\d{1,2})[-/](\\d{1,2})[-/](\\d{4})", obs7$md_md_yyyy_string)
obs7$md_md_yyyy_vector <- NA
obs7$md_md_yyyy_vector <- regmatches(obs7$md_md_yyyy_string, positions_md_md_yyyy)
obs7$md_md_yyyy_string <- NULL

first_md_2 <- as.integer(sapply(obs7$md_md_yyyy_vector, function(x) return(x[3])))
second_md_2 <- as.integer(sapply(obs7$md_md_yyyy_vector, function(x) return(x[4])))
obs7$md_md_yyyy_vector <- NULL

# Applying the function to our data and assigning results to columns 
md_md_yyyy_results <- mapply(choosing_month_or_date, first_md_2, second_md_2, SIMPLIFY=FALSE)

obs7$month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$month)
obs7$day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$day)
obs7$likely_month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_month)
obs7$likely_day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_day)

#confirm they were all assigned
#length(which(!is.na(obs7$day_when_md_md_yyyy))) 



# Section 5: Dealing with exceptions
#nb_no_date <- length(obs7$observed_on_string[is.na(obs7$observed_on_string)]) # when no date is listed
# very few (0.02%) have some non-NA entry other than the formats we've handled. deal with them here if one wishes to
#nb_entries_not_caught <- nrow(obs7) - sum(nb_alhpabetic, nb_md_md_yyyy, nb_yyyy_md_md, nb_no_date)



# Section 6: Assembling uniform date column 
# 6.1 assembling together different parts of the date
obs7$certain_dd <-
  ifelse(!is.na(obs7$day_when_month_alphabetical), obs7$day_when_month_alphabetical,
         ifelse(!is.na(obs7$day_when_yyyy_md_md), obs7$day_when_yyyy_md_md,
                ifelse(!is.na(obs7$day_when_md_md_yyyy), obs7$day_when_md_md_yyyy, NA)))

obs7$probable_dd <- 
  ifelse(!is.na(obs7$likely_day_when_yyyy_md_md), obs7$likely_day_when_yyyy_md_md,
         ifelse(!is.na(obs7$likely_day_when_md_md_yyyy), obs7$likely_day_when_md_md_yyyy, NA))

obs7$certain_mm <-
  ifelse(!is.na(obs7$month_numeric_when_alphabetic), obs7$month_numeric_when_alphabetic,
         ifelse(!is.na(obs7$month_when_yyyy_md_md), obs7$month_when_yyyy_md_md,
                ifelse(!is.na(obs7$month_when_md_md_yyyy), obs7$month_when_md_md_yyyy, NA)))

obs7$probable_mm <-
  ifelse(!is.na(obs7$likely_month_when_yyyy_md_md), obs7$likely_month_when_yyyy_md_md,
         ifelse(!is.na(obs7$likely_month_when_md_md_yyyy), obs7$likely_month_when_md_md_yyyy, NA))

# 6.2 assembling final columns
certain_dd <- ifelse(!is.na(obs7$certain_dd), obs7$certain_dd, "??")
certain_mm <- ifelse(!is.na(obs7$certain_mm), obs7$certain_mm, "??")
certain_yyyy <- ifelse(!is.na(obs7$year), obs7$year, "????")

obs7$certain_ddmmyyyy <-
  paste(
    ifelse(!is.na(obs7$certain_dd), obs7$certain_dd, "??"),
    ifelse(!is.na(obs7$certain_mm), obs7$certain_mm, "??"),
    ifelse(!is.na(obs7$year), obs7$year, "??"),
    sep="/")

obs7$probable_ddmmyyyy <-
  ifelse(
    (!is.na(obs7$probable_dd) & !is.na(obs7$probable_mm) & !is.na(obs7$year)),
    paste(obs7$probable_dd, obs7$probable_mm, obs7$year, sep="/"),
    NA)

obs7$standardized_ddmmyyyy <-
  ifelse((!is.na(obs7$probable_ddmmyyyy)), obs7$probable_ddmmyyyy, obs7$certain_ddmmyyyy)


# # Section 7: Assessing the quality of the "assumed" dates of ambiguous cases
# # when there is indeed a day > 12 (clear case), what percentage of them are in the first position? 
# # this serves as an estimate for the non-clear cases, to see how accurate our month-first assumption is
# nb_total_obs <- nrow(obs7)
# test_yyyy_md_md_string <- sapply(obs7$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
# positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", test_yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
# test_yyyy_md_md_vector <- regmatches(test_yyyy_md_md_string, positions_yyyy_md_md)
# test_first_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[3])))
# test_second_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[4])))
# 
# counting_error_rate <- function(first_number, second_number){
#   
#   both_na <- is.na(test_first_md) & is.na(second_number)
#   total_yyyy_md_md <- sum(!is.na(first_number))
#   first_md_greater_than_12 <- first_number > 12
#   second_md_greater_than_12 <- second_number > 12
#   ambiguous_cases <- !(both_na | first_md_greater_than_12 | second_md_greater_than_12)
#   
#   nb_first_md_gt12 <- sum(first_md_greater_than_12, na.rm = TRUE)
#   nb_second_md_gt12 <- sum(second_md_greater_than_12, na.rm = TRUE)
#   total_clear_cases <- nb_first_md_gt12 + nb_second_md_gt12
#   nb_ambiguous <- sum(ambiguous_cases, na.rm=TRUE)
#   nb_both_na <- sum(both_na)
#   
#   percent_of_clear_cases_with_day_first <- (nb_first_md_gt12/total_clear_cases)*100
#   percent_successful_guess_of_month_first <- round((100-percent_of_clear_cases_with_day_first), 8)
#   
#   return( list(
#     summary = paste(
#       c(
#         paste("total cases of yyyy-md-md: ", total_yyyy_md_md),
#         paste("ambiguous cases:", nb_ambiguous),
#         paste("clear cases:", total_clear_cases),
#         paste0("percent of all the observations caught under this search: ", (total_yyyy_md_md/nb_total_obs)*100,"%"),
#         paste0("percent of yyyy-md-md cases that are ambiguous: ", (nb_ambiguous/total_yyyy_md_md)*100,"%"),
#         paste0("percent of clear cases with day first: ", percent_of_clear_cases_with_day_first,"%")
#       ),
#       collapse="\n"
#     ),
#     percent_successful_guess_of_month_first = percent_successful_guess_of_month_first
#   ))
# }
# 
# 
# test_result <- counting_error_rate(test_first_md, test_second_md)
# cat(test_result$summary) # so 99.9996% of cases, the first number is the day
# percent_successful_guess_of_month_first <- test_result$percent_successful_guess_of_month_first
# 
# # Indicating this error rate in the df
# probable_date_string <- paste0("probable_ddmmyyyy_to_approx_", percent_successful_guess_of_month_first,"_percent_certainty")
# colnames(obs7)[colnames(obs7) == "probable_ddmmyyyy"] <- probable_date_string

# Creating easy column to identify certain dates over probable dates
obs7$is_ambiguous <- 
  ifelse(is.na(obs7$probable_ddmmyyyy),
         0,
         1)

# # Screen for months that should not be
# improper_months_regex <- paste0("^\\d{1,2}/(", paste0(13:99, collapse = "|"), ")/\\d{4}$")
# erroneous_months <- which(!is.na(str_extract(obs7$standardized_ddmmyyyy, improper_months_regex)))
# erroneous_months_df <- obs7[erroneous_months,]

# Section 8: Writing out data with unified columns to desired location
obs7$year <- NULL
obs7$month_alphabetic <- NULL
obs7$day_when_month_alphabetical <- NULL
obs7$yyyy_md_md <- NULL
obs7$month_when_yyyy_md_md <- NULL
obs7$day_when_yyyy_md_md <- NULL
obs7$likely_month_when_yyyy_md_md <- NULL
obs7$likely_day_when_yyyy_md_md <- NULL
obs7$md_md_yyyy <- NULL
obs7$month_when_md_md_yyyy <- NULL
obs7$day_when_md_md_yyyy <- NULL
obs7$likely_month_when_md_md_yyyy <- NULL
obs7$likely_day_when_md_md_yyyy <- NULL
obs7$certain_dd <- NULL
obs7$probable_dd <- NULL
obs7$certain_mm <- NULL
obs7$probable_mm <- NULL
obs7$certain_ddmmyyyy <- NULL
obs7$probable_ddmmyyyy <- NULL
obs7$just_month_when_alphabetic <- NULL
obs7$month_numeric_when_alphabetic <- NULL






#############################8####################################

# Section 2: Extracting the year
#2.1 first pass - grep any set of 4 numbers
obs8$year4num <- regmatches(obs8$observed_on_string, gregexpr("\\d{4}", obs8$observed_on_string))
obs8$year <- obs8$year4num
obs8$year <- sapply(obs8$year4num, function(x){ ifelse(length(x)==0,NA,x[1])}) # to NA the ones with no year
#length(which(is.na(obs8$year))) # about 95% of these cause theres no entry at all. 5% have an entry without a year

#2.2 second pass - correct those with multiple entries of 4 numbers
twos_indx <- which(sapply(obs8$year4num, length) == 2)

year_only <- lapply(obs8$year4num[twos_indx], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 1800 & x_num < 2026] # the plausible 4digit is used
})
obs8$year[twos_indx] <- year_only

#2.3 Any more still left? Then refine date inclusion criteria
twos_indx_still <- which(sapply(obs8$year, length) == 2)
obs8$observed_on_string[twos_indx_still] # displays the dates

# All years 2015 and 2016 with manual check, so use >2000. Change this if need be for different dfs
year_only <- lapply(obs8$year[twos_indx_still], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 2000]
})
obs8$year[twos_indx_still] <- year_only

# 2.4 Final check - any left? If so manually edit your df
#length(which(sapply(obs8$year, length) == 2)) # if empty, no further checks needed
obs8$year4num <- NULL




# Section 3 - Extracting the Month and dates when month listed as letters

# create a lookup table
month_lookup <- c(
  Jan=1, January=1, janv=1, janvier=1, jan=1, january=1,
  Feb=2, February=2, Fév=2, fév=2, fevrier=2, février=2, feb=2, february=2,
  Mar=3, March=3, mars=3, mar=3, march=3,
  Apr=4, April=4, avr=4, avril=4, apr=4, april=4,
  May=5, mai=5, may=5,
  Jun=6, June=6, juin=6, jun=6, june=6,
  Jul=7, July=7, juil=7, juillet=7, jul=7, july=7,
  Aug=8, August=8, aout=8, août=8, aug=8, august=8,
  Sep=9, Sept=9, September=9, sept=9, septembre=9, sep=9, september=9,
  Oct=10, October=10, oct=10, octobre=10, oct=10, october=10,
  Nov=11, November=11, nov=11, novembre=11, nov=11, november=11,
  Dec=12, December=12, déc=12, dec=12, décembre=12, dec=12, december=12
)

# create regex and use str_extract to take out the month
months_alphabetic_regex <- paste0("\\b(", paste(names(month_lookup), collapse="|"), ")\\b")
obs8$just_month_when_alphabetic <- str_extract(obs8$observed_on_string, months_alphabetic_regex)
#nb_alhpabetic <- length(which(!is.na(obs8$just_month_when_alphabetic))) # roughly 15% of cases

# assigning it to new column
obs8$month_numeric_when_alphabetic <- sapply(obs8$just_month_when_alphabetic, function(x){
  if (!is.na(x)){
    month_lookup[[x]]
  }
  else{
    NA
  }
})

# Finding the dates from these rows - should be the only " XX " in the string
obs8$day_when_month_alphabetical <- NA
rows_with_alphabetical_month <- !is.na(obs8$month_numeric_when_alphabetic)
positions_alpha_day <- regexec("\\s(\\d{2})\\s", obs8$observed_on_string[rows_with_alphabetical_month])
day_when_alpha <- regmatches(obs8$observed_on_string[rows_with_alphabetical_month], positions_alpha_day)
obs8$day_when_month_alphabetical[rows_with_alphabetical_month] <- 
  sapply(day_when_alpha, function(x) x[2])

#looking for errors
#length(which(sapply(obs8$day_when_month_alphabetical, length)> 1)) # any double dates? should be none
#length(which(obs8$day_when_month_alphabetical > 31))# any false dates? should be none


# Section 4: Extracting months and dates when listed as yyyy-m?d?-m?d?
# Issue: many formats (yyyy-mm-dd, yyyy-dd-mm, yyyy-d-m, yyyy-m-d)

# New column to contain only dates of this format
obs8$yyyy_md_md <- regmatches(obs8$observed_on_string, gregexpr("\\d{4}[/-]\\d{2}[/-]\\d{2}", obs8$observed_on_string))
obs8$yyyy_md_md[sapply(obs8$yyyy_md_md, length)==0] <- NA
#nb_yyyy_md_md <- length(which(!is.na(obs8$yyyy_md_md))) # the vast majority

# Extracting the numbers
obs8$yyyy_md_md_string <- sapply(obs8$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", obs8$yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
obs8$yyyy_md_md_vector <- regmatches(obs8$yyyy_md_md_string, positions_yyyy_md_md)
obs8$yyyy_md_md_string <- NULL

first_md <- as.integer(sapply(obs8$yyyy_md_md_vector, function(x) return(x[3])))
second_md <- as.integer(sapply(obs8$yyyy_md_md_vector, function(x) return(x[4])))
obs8$yyyy_md_md_vector <- NULL

# Creating function to extract date and month based on the values of first_md and second_md.
# It seems like the vast majority should be month first.
choosing_month_or_date <- function(first_number, second_number){
  
  day <- NA
  month <- NA
  likely_day <- NA
  likely_month <- NA
  
  if (is.na(first_number) && is.na(second_number)){
    # do nothing
  }
  else if (first_number > 12){
    day <- first_number
    month <- second_number
  }
  else if (second_number > 12){
    day <- second_number
    month <- first_number
  }
  else { # when ambiguous, assume first number is the month but mark as ambiguous
    likely_day <- second_number
    likely_month <- first_number
  }
  return(list(day=day,month=month,likely_day=likely_day,likely_month=likely_month))
}

# Applying the function to our data and assigning results to columns 
yyyy_md_md_results <- mapply(choosing_month_or_date,first_md, second_md, SIMPLIFY=FALSE)

obs8$month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$month)
obs8$day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$day)
obs8$likely_month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_month)
obs8$likely_day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_day)



# Section 5: Extracting months and dates when listed as md-md-yyyy - there are very few
# Issue:  many formats (mm-dd-yyyy, dd-mm-yyyy, d-m-yyyy, m-d-yyyy)

# New column to contain only dates of this format
obs8$md_md_yyyy <- regmatches(obs8$observed_on_string, gregexpr("\\d{1,2}[-/]\\d{1,2}[-/]\\d{4}", obs8$observed_on_string))
obs8$md_md_yyyy[sapply(obs8$md_md_yyyy, length)==0] <- NA
#nb_md_md_yyyy <- length(which(!is.na(obs8$md_md_yyyy))) # very few

# Extract the numbers
obs8$md_md_yyyy_string <- sapply(obs8$md_md_yyyy, function(x) if(length(x)==0) NA else x[1])
positions_md_md_yyyy <- regexec("(\\d{1,2})[-/](\\d{1,2})[-/](\\d{4})", obs8$md_md_yyyy_string)
obs8$md_md_yyyy_vector <- NA
obs8$md_md_yyyy_vector <- regmatches(obs8$md_md_yyyy_string, positions_md_md_yyyy)
obs8$md_md_yyyy_string <- NULL

first_md_2 <- as.integer(sapply(obs8$md_md_yyyy_vector, function(x) return(x[3])))
second_md_2 <- as.integer(sapply(obs8$md_md_yyyy_vector, function(x) return(x[4])))
obs8$md_md_yyyy_vector <- NULL

# Applying the function to our data and assigning results to columns 
md_md_yyyy_results <- mapply(choosing_month_or_date, first_md_2, second_md_2, SIMPLIFY=FALSE)

obs8$month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$month)
obs8$day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$day)
obs8$likely_month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_month)
obs8$likely_day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_day)

#confirm they were all assigned
#length(which(!is.na(obs8$day_when_md_md_yyyy))) 



# Section 5: Dealing with exceptions
#nb_no_date <- length(obs8$observed_on_string[is.na(obs8$observed_on_string)]) # when no date is listed
# very few (0.02%) have some non-NA entry other than the formats we've handled. deal with them here if one wishes to
#nb_entries_not_caught <- nrow(obs8) - sum(nb_alhpabetic, nb_md_md_yyyy, nb_yyyy_md_md, nb_no_date)



# Section 6: Assembling uniform date column 
# 6.1 assembling together different parts of the date
obs8$certain_dd <-
  ifelse(!is.na(obs8$day_when_month_alphabetical), obs8$day_when_month_alphabetical,
         ifelse(!is.na(obs8$day_when_yyyy_md_md), obs8$day_when_yyyy_md_md,
                ifelse(!is.na(obs8$day_when_md_md_yyyy), obs8$day_when_md_md_yyyy, NA)))

obs8$probable_dd <- 
  ifelse(!is.na(obs8$likely_day_when_yyyy_md_md), obs8$likely_day_when_yyyy_md_md,
         ifelse(!is.na(obs8$likely_day_when_md_md_yyyy), obs8$likely_day_when_md_md_yyyy, NA))

obs8$certain_mm <-
  ifelse(!is.na(obs8$month_numeric_when_alphabetic), obs8$month_numeric_when_alphabetic,
         ifelse(!is.na(obs8$month_when_yyyy_md_md), obs8$month_when_yyyy_md_md,
                ifelse(!is.na(obs8$month_when_md_md_yyyy), obs8$month_when_md_md_yyyy, NA)))

obs8$probable_mm <-
  ifelse(!is.na(obs8$likely_month_when_yyyy_md_md), obs8$likely_month_when_yyyy_md_md,
         ifelse(!is.na(obs8$likely_month_when_md_md_yyyy), obs8$likely_month_when_md_md_yyyy, NA))

# 6.2 assembling final columns
certain_dd <- ifelse(!is.na(obs8$certain_dd), obs8$certain_dd, "??")
certain_mm <- ifelse(!is.na(obs8$certain_mm), obs8$certain_mm, "??")
certain_yyyy <- ifelse(!is.na(obs8$year), obs8$year, "????")

obs8$certain_ddmmyyyy <-
  paste(
    ifelse(!is.na(obs8$certain_dd), obs8$certain_dd, "??"),
    ifelse(!is.na(obs8$certain_mm), obs8$certain_mm, "??"),
    ifelse(!is.na(obs8$year), obs8$year, "??"),
    sep="/")

obs8$probable_ddmmyyyy <-
  ifelse(
    (!is.na(obs8$probable_dd) & !is.na(obs8$probable_mm) & !is.na(obs8$year)),
    paste(obs8$probable_dd, obs8$probable_mm, obs8$year, sep="/"),
    NA)

obs8$standardized_ddmmyyyy <-
  ifelse((!is.na(obs8$probable_ddmmyyyy)), obs8$probable_ddmmyyyy, obs8$certain_ddmmyyyy)


# # Section 7: Assessing the quality of the "assumed" dates of ambiguous cases
# # when there is indeed a day > 12 (clear case), what percentage of them are in the first position? 
# # this serves as an estimate for the non-clear cases, to see how accurate our month-first assumption is
# nb_total_obs <- nrow(obs8)
# test_yyyy_md_md_string <- sapply(obs8$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
# positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", test_yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
# test_yyyy_md_md_vector <- regmatches(test_yyyy_md_md_string, positions_yyyy_md_md)
# test_first_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[3])))
# test_second_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[4])))
# 
# counting_error_rate <- function(first_number, second_number){
#   
#   both_na <- is.na(test_first_md) & is.na(second_number)
#   total_yyyy_md_md <- sum(!is.na(first_number))
#   first_md_greater_than_12 <- first_number > 12
#   second_md_greater_than_12 <- second_number > 12
#   ambiguous_cases <- !(both_na | first_md_greater_than_12 | second_md_greater_than_12)
#   
#   nb_first_md_gt12 <- sum(first_md_greater_than_12, na.rm = TRUE)
#   nb_second_md_gt12 <- sum(second_md_greater_than_12, na.rm = TRUE)
#   total_clear_cases <- nb_first_md_gt12 + nb_second_md_gt12
#   nb_ambiguous <- sum(ambiguous_cases, na.rm=TRUE)
#   nb_both_na <- sum(both_na)
#   
#   percent_of_clear_cases_with_day_first <- (nb_first_md_gt12/total_clear_cases)*100
#   percent_successful_guess_of_month_first <- round((100-percent_of_clear_cases_with_day_first), 8)
#   
#   return( list(
#     summary = paste(
#       c(
#         paste("total cases of yyyy-md-md: ", total_yyyy_md_md),
#         paste("ambiguous cases:", nb_ambiguous),
#         paste("clear cases:", total_clear_cases),
#         paste0("percent of all the observations caught under this search: ", (total_yyyy_md_md/nb_total_obs)*100,"%"),
#         paste0("percent of yyyy-md-md cases that are ambiguous: ", (nb_ambiguous/total_yyyy_md_md)*100,"%"),
#         paste0("percent of clear cases with day first: ", percent_of_clear_cases_with_day_first,"%")
#       ),
#       collapse="\n"
#     ),
#     percent_successful_guess_of_month_first = percent_successful_guess_of_month_first
#   ))
# }
# 
# 
# test_result <- counting_error_rate(test_first_md, test_second_md)
# cat(test_result$summary) # so 99.9996% of cases, the first number is the day
# percent_successful_guess_of_month_first <- test_result$percent_successful_guess_of_month_first
# 
# # Indicating this error rate in the df
# probable_date_string <- paste0("probable_ddmmyyyy_to_approx_", percent_successful_guess_of_month_first,"_percent_certainty")
# colnames(obs8)[colnames(obs8) == "probable_ddmmyyyy"] <- probable_date_string

# Creating easy column to identify certain dates over probable dates
obs8$is_ambiguous <- 
  ifelse(is.na(obs8$probable_ddmmyyyy),
         0,
         1)

# # Screen for months that should not be
# improper_months_regex <- paste0("^\\d{1,2}/(", paste0(13:99, collapse = "|"), ")/\\d{4}$")
# erroneous_months <- which(!is.na(str_extract(obs8$standardized_ddmmyyyy, improper_months_regex)))
# erroneous_months_df <- obs8[erroneous_months,]

# Section 8: Writing out data with unified columns to desired location
obs8$year <- NULL
obs8$month_alphabetic <- NULL
obs8$day_when_month_alphabetical <- NULL
obs8$yyyy_md_md <- NULL
obs8$month_when_yyyy_md_md <- NULL
obs8$day_when_yyyy_md_md <- NULL
obs8$likely_month_when_yyyy_md_md <- NULL
obs8$likely_day_when_yyyy_md_md <- NULL
obs8$md_md_yyyy <- NULL
obs8$month_when_md_md_yyyy <- NULL
obs8$day_when_md_md_yyyy <- NULL
obs8$likely_month_when_md_md_yyyy <- NULL
obs8$likely_day_when_md_md_yyyy <- NULL
obs8$certain_dd <- NULL
obs8$probable_dd <- NULL
obs8$certain_mm <- NULL
obs8$probable_mm <- NULL
obs8$certain_ddmmyyyy <- NULL
obs8$probable_ddmmyyyy <- NULL
obs8$just_month_when_alphabetic <- NULL
obs8$month_numeric_when_alphabetic <- NULL








##################################9###############################

# Section 2: Extracting the year
#2.1 first pass - grep any set of 4 numbers
obs9$year4num <- regmatches(obs9$observed_on_string, gregexpr("\\d{4}", obs9$observed_on_string))
obs9$year <- obs9$year4num
obs9$year <- sapply(obs9$year4num, function(x){ ifelse(length(x)==0,NA,x[1])}) # to NA the ones with no year
#length(which(is.na(obs9$year))) # about 95% of these cause theres no entry at all. 5% have an entry without a year

#2.2 second pass - correct those with multiple entries of 4 numbers
twos_indx <- which(sapply(obs9$year4num, length) == 2)

year_only <- lapply(obs9$year4num[twos_indx], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 1800 & x_num < 2026] # the plausible 4digit is used
})
obs9$year[twos_indx] <- year_only

#2.3 Any more still left? Then refine date inclusion criteria
twos_indx_still <- which(sapply(obs9$year, length) == 2)
obs9$observed_on_string[twos_indx_still] # displays the dates

# All years 2015 and 2016 with manual check, so use >2000. Change this if need be for different dfs
year_only <- lapply(obs9$year[twos_indx_still], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 2000]
})
obs9$year[twos_indx_still] <- year_only

# 2.4 Final check - any left? If so manually edit your df
#length(which(sapply(obs9$year, length) == 2)) # if empty, no further checks needed
obs9$year4num <- NULL




# Section 3 - Extracting the Month and dates when month listed as letters

# create a lookup table
month_lookup <- c(
  Jan=1, January=1, janv=1, janvier=1, jan=1, january=1,
  Feb=2, February=2, Fév=2, fév=2, fevrier=2, février=2, feb=2, february=2,
  Mar=3, March=3, mars=3, mar=3, march=3,
  Apr=4, April=4, avr=4, avril=4, apr=4, april=4,
  May=5, mai=5, may=5,
  Jun=6, June=6, juin=6, jun=6, june=6,
  Jul=7, July=7, juil=7, juillet=7, jul=7, july=7,
  Aug=8, August=8, aout=8, août=8, aug=8, august=8,
  Sep=9, Sept=9, September=9, sept=9, septembre=9, sep=9, september=9,
  Oct=10, October=10, oct=10, octobre=10, oct=10, october=10,
  Nov=11, November=11, nov=11, novembre=11, nov=11, november=11,
  Dec=12, December=12, déc=12, dec=12, décembre=12, dec=12, december=12
)

# create regex and use str_extract to take out the month
months_alphabetic_regex <- paste0("\\b(", paste(names(month_lookup), collapse="|"), ")\\b")
obs9$just_month_when_alphabetic <- str_extract(obs9$observed_on_string, months_alphabetic_regex)
#nb_alhpabetic <- length(which(!is.na(obs9$just_month_when_alphabetic))) # roughly 15% of cases

# assigning it to new column
obs9$month_numeric_when_alphabetic <- sapply(obs9$just_month_when_alphabetic, function(x){
  if (!is.na(x)){
    month_lookup[[x]]
  }
  else{
    NA
  }
})

# Finding the dates from these rows - should be the only " XX " in the string
obs9$day_when_month_alphabetical <- NA
rows_with_alphabetical_month <- !is.na(obs9$month_numeric_when_alphabetic)
positions_alpha_day <- regexec("\\s(\\d{2})\\s", obs9$observed_on_string[rows_with_alphabetical_month])
day_when_alpha <- regmatches(obs9$observed_on_string[rows_with_alphabetical_month], positions_alpha_day)
obs9$day_when_month_alphabetical[rows_with_alphabetical_month] <- 
  sapply(day_when_alpha, function(x) x[2])

#looking for errors
#length(which(sapply(obs9$day_when_month_alphabetical, length)> 1)) # any double dates? should be none
#length(which(obs9$day_when_month_alphabetical > 31))# any false dates? should be none


# Section 4: Extracting months and dates when listed as yyyy-m?d?-m?d?
# Issue: many formats (yyyy-mm-dd, yyyy-dd-mm, yyyy-d-m, yyyy-m-d)

# New column to contain only dates of this format
obs9$yyyy_md_md <- regmatches(obs9$observed_on_string, gregexpr("\\d{4}[/-]\\d{2}[/-]\\d{2}", obs9$observed_on_string))
obs9$yyyy_md_md[sapply(obs9$yyyy_md_md, length)==0] <- NA
#nb_yyyy_md_md <- length(which(!is.na(obs9$yyyy_md_md))) # the vast majority

# Extracting the numbers
obs9$yyyy_md_md_string <- sapply(obs9$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", obs9$yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
obs9$yyyy_md_md_vector <- regmatches(obs9$yyyy_md_md_string, positions_yyyy_md_md)
obs9$yyyy_md_md_string <- NULL

first_md <- as.integer(sapply(obs9$yyyy_md_md_vector, function(x) return(x[3])))
second_md <- as.integer(sapply(obs9$yyyy_md_md_vector, function(x) return(x[4])))
obs9$yyyy_md_md_vector <- NULL

# Creating function to extract date and month based on the values of first_md and second_md.
# It seems like the vast majority should be month first.
choosing_month_or_date <- function(first_number, second_number){
  
  day <- NA
  month <- NA
  likely_day <- NA
  likely_month <- NA
  
  if (is.na(first_number) && is.na(second_number)){
    # do nothing
  }
  else if (first_number > 12){
    day <- first_number
    month <- second_number
  }
  else if (second_number > 12){
    day <- second_number
    month <- first_number
  }
  else { # when ambiguous, assume first number is the month but mark as ambiguous
    likely_day <- second_number
    likely_month <- first_number
  }
  return(list(day=day,month=month,likely_day=likely_day,likely_month=likely_month))
}

# Applying the function to our data and assigning results to columns 
yyyy_md_md_results <- mapply(choosing_month_or_date,first_md, second_md, SIMPLIFY=FALSE)

obs9$month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$month)
obs9$day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$day)
obs9$likely_month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_month)
obs9$likely_day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_day)



# Section 5: Extracting months and dates when listed as md-md-yyyy - there are very few
# Issue:  many formats (mm-dd-yyyy, dd-mm-yyyy, d-m-yyyy, m-d-yyyy)

# New column to contain only dates of this format
obs9$md_md_yyyy <- regmatches(obs9$observed_on_string, gregexpr("\\d{1,2}[-/]\\d{1,2}[-/]\\d{4}", obs9$observed_on_string))
obs9$md_md_yyyy[sapply(obs9$md_md_yyyy, length)==0] <- NA
#nb_md_md_yyyy <- length(which(!is.na(obs9$md_md_yyyy))) # very few

# Extract the numbers
obs9$md_md_yyyy_string <- sapply(obs9$md_md_yyyy, function(x) if(length(x)==0) NA else x[1])
positions_md_md_yyyy <- regexec("(\\d{1,2})[-/](\\d{1,2})[-/](\\d{4})", obs9$md_md_yyyy_string)
obs9$md_md_yyyy_vector <- NA
obs9$md_md_yyyy_vector <- regmatches(obs9$md_md_yyyy_string, positions_md_md_yyyy)
obs9$md_md_yyyy_string <- NULL

first_md_2 <- as.integer(sapply(obs9$md_md_yyyy_vector, function(x) return(x[3])))
second_md_2 <- as.integer(sapply(obs9$md_md_yyyy_vector, function(x) return(x[4])))
obs9$md_md_yyyy_vector <- NULL

# Applying the function to our data and assigning results to columns 
md_md_yyyy_results <- mapply(choosing_month_or_date, first_md_2, second_md_2, SIMPLIFY=FALSE)

obs9$month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$month)
obs9$day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$day)
obs9$likely_month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_month)
obs9$likely_day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_day)

#confirm they were all assigned
#length(which(!is.na(obs9$day_when_md_md_yyyy))) 



# Section 5: Dealing with exceptions
#nb_no_date <- length(obs9$observed_on_string[is.na(obs9$observed_on_string)]) # when no date is listed
# very few (0.02%) have some non-NA entry other than the formats we've handled. deal with them here if one wishes to
#nb_entries_not_caught <- nrow(obs9) - sum(nb_alhpabetic, nb_md_md_yyyy, nb_yyyy_md_md, nb_no_date)



# Section 6: Assembling uniform date column 
# 6.1 assembling together different parts of the date
obs9$certain_dd <-
  ifelse(!is.na(obs9$day_when_month_alphabetical), obs9$day_when_month_alphabetical,
         ifelse(!is.na(obs9$day_when_yyyy_md_md), obs9$day_when_yyyy_md_md,
                ifelse(!is.na(obs9$day_when_md_md_yyyy), obs9$day_when_md_md_yyyy, NA)))

obs9$probable_dd <- 
  ifelse(!is.na(obs9$likely_day_when_yyyy_md_md), obs9$likely_day_when_yyyy_md_md,
         ifelse(!is.na(obs9$likely_day_when_md_md_yyyy), obs9$likely_day_when_md_md_yyyy, NA))

obs9$certain_mm <-
  ifelse(!is.na(obs9$month_numeric_when_alphabetic), obs9$month_numeric_when_alphabetic,
         ifelse(!is.na(obs9$month_when_yyyy_md_md), obs9$month_when_yyyy_md_md,
                ifelse(!is.na(obs9$month_when_md_md_yyyy), obs9$month_when_md_md_yyyy, NA)))

obs9$probable_mm <-
  ifelse(!is.na(obs9$likely_month_when_yyyy_md_md), obs9$likely_month_when_yyyy_md_md,
         ifelse(!is.na(obs9$likely_month_when_md_md_yyyy), obs9$likely_month_when_md_md_yyyy, NA))

# 6.2 assembling final columns
certain_dd <- ifelse(!is.na(obs9$certain_dd), obs9$certain_dd, "??")
certain_mm <- ifelse(!is.na(obs9$certain_mm), obs9$certain_mm, "??")
certain_yyyy <- ifelse(!is.na(obs9$year), obs9$year, "????")

obs9$certain_ddmmyyyy <-
  paste(
    ifelse(!is.na(obs9$certain_dd), obs9$certain_dd, "??"),
    ifelse(!is.na(obs9$certain_mm), obs9$certain_mm, "??"),
    ifelse(!is.na(obs9$year), obs9$year, "??"),
    sep="/")

obs9$probable_ddmmyyyy <-
  ifelse(
    (!is.na(obs9$probable_dd) & !is.na(obs9$probable_mm) & !is.na(obs9$year)),
    paste(obs9$probable_dd, obs9$probable_mm, obs9$year, sep="/"),
    NA)

obs9$standardized_ddmmyyyy <-
  ifelse((!is.na(obs9$probable_ddmmyyyy)), obs9$probable_ddmmyyyy, obs9$certain_ddmmyyyy)


# # Section 7: Assessing the quality of the "assumed" dates of ambiguous cases
# # when there is indeed a day > 12 (clear case), what percentage of them are in the first position? 
# # this serves as an estimate for the non-clear cases, to see how accurate our month-first assumption is
# nb_total_obs <- nrow(obs9)
# test_yyyy_md_md_string <- sapply(obs9$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
# positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", test_yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
# test_yyyy_md_md_vector <- regmatches(test_yyyy_md_md_string, positions_yyyy_md_md)
# test_first_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[3])))
# test_second_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[4])))
# 
# counting_error_rate <- function(first_number, second_number){
#   
#   both_na <- is.na(test_first_md) & is.na(second_number)
#   total_yyyy_md_md <- sum(!is.na(first_number))
#   first_md_greater_than_12 <- first_number > 12
#   second_md_greater_than_12 <- second_number > 12
#   ambiguous_cases <- !(both_na | first_md_greater_than_12 | second_md_greater_than_12)
#   
#   nb_first_md_gt12 <- sum(first_md_greater_than_12, na.rm = TRUE)
#   nb_second_md_gt12 <- sum(second_md_greater_than_12, na.rm = TRUE)
#   total_clear_cases <- nb_first_md_gt12 + nb_second_md_gt12
#   nb_ambiguous <- sum(ambiguous_cases, na.rm=TRUE)
#   nb_both_na <- sum(both_na)
#   
#   percent_of_clear_cases_with_day_first <- (nb_first_md_gt12/total_clear_cases)*100
#   percent_successful_guess_of_month_first <- round((100-percent_of_clear_cases_with_day_first), 8)
#   
#   return( list(
#     summary = paste(
#       c(
#         paste("total cases of yyyy-md-md: ", total_yyyy_md_md),
#         paste("ambiguous cases:", nb_ambiguous),
#         paste("clear cases:", total_clear_cases),
#         paste0("percent of all the observations caught under this search: ", (total_yyyy_md_md/nb_total_obs)*100,"%"),
#         paste0("percent of yyyy-md-md cases that are ambiguous: ", (nb_ambiguous/total_yyyy_md_md)*100,"%"),
#         paste0("percent of clear cases with day first: ", percent_of_clear_cases_with_day_first,"%")
#       ),
#       collapse="\n"
#     ),
#     percent_successful_guess_of_month_first = percent_successful_guess_of_month_first
#   ))
# }
# 
# 
# test_result <- counting_error_rate(test_first_md, test_second_md)
# cat(test_result$summary) # so 99.9996% of cases, the first number is the day
# percent_successful_guess_of_month_first <- test_result$percent_successful_guess_of_month_first
# 
# # Indicating this error rate in the df
# probable_date_string <- paste0("probable_ddmmyyyy_to_approx_", percent_successful_guess_of_month_first,"_percent_certainty")
# colnames(obs9)[colnames(obs9) == "probable_ddmmyyyy"] <- probable_date_string

# Creating easy column to identify certain dates over probable dates
obs9$is_ambiguous <- 
  ifelse(is.na(obs9$probable_ddmmyyyy),
         0,
         1)

# # Screen for months that should not be
# improper_months_regex <- paste0("^\\d{1,2}/(", paste0(13:99, collapse = "|"), ")/\\d{4}$")
# erroneous_months <- which(!is.na(str_extract(obs9$standardized_ddmmyyyy, improper_months_regex)))
# erroneous_months_df <- obs9[erroneous_months,]

# Section 8: Writing out data with unified columns to desired location
obs9$year <- NULL
obs9$month_alphabetic <- NULL
obs9$day_when_month_alphabetical <- NULL
obs9$yyyy_md_md <- NULL
obs9$month_when_yyyy_md_md <- NULL
obs9$day_when_yyyy_md_md <- NULL
obs9$likely_month_when_yyyy_md_md <- NULL
obs9$likely_day_when_yyyy_md_md <- NULL
obs9$md_md_yyyy <- NULL
obs9$month_when_md_md_yyyy <- NULL
obs9$day_when_md_md_yyyy <- NULL
obs9$likely_month_when_md_md_yyyy <- NULL
obs9$likely_day_when_md_md_yyyy <- NULL
obs9$certain_dd <- NULL
obs9$probable_dd <- NULL
obs9$certain_mm <- NULL
obs9$probable_mm <- NULL
obs9$certain_ddmmyyyy <- NULL
obs9$probable_ddmmyyyy <- NULL
obs9$just_month_when_alphabetic <- NULL
obs9$month_numeric_when_alphabetic <- NULL












####################################10#########################


# Section 2: Extracting the year
#2.1 first pass - grep any set of 4 numbers
obs10$year4num <- regmatches(obs10$observed_on_string, gregexpr("\\d{4}", obs10$observed_on_string))
obs10$year <- obs10$year4num
obs10$year <- sapply(obs10$year4num, function(x){ ifelse(length(x)==0,NA,x[1])}) # to NA the ones with no year
#length(which(is.na(obs10$year))) # about 95% of these cause theres no entry at all. 5% have an entry without a year

#2.2 second pass - correct those with multiple entries of 4 numbers
twos_indx <- which(sapply(obs10$year4num, length) == 2)

year_only <- lapply(obs10$year4num[twos_indx], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 1800 & x_num < 2026] # the plausible 4digit is used
})
obs10$year[twos_indx] <- year_only

#2.3 Any more still left? Then refine date inclusion criteria
twos_indx_still <- which(sapply(obs10$year, length) == 2)
obs10$observed_on_string[twos_indx_still] # displays the dates

# All years 2015 and 2016 with manual check, so use >2000. Change this if need be for different dfs
year_only <- lapply(obs10$year[twos_indx_still], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 2000]
})
obs10$year[twos_indx_still] <- year_only

# 2.4 Final check - any left? If so manually edit your df
#length(which(sapply(obs10$year, length) == 2)) # if empty, no further checks needed
obs10$year4num <- NULL




# Section 3 - Extracting the Month and dates when month listed as letters

# create a lookup table
month_lookup <- c(
  Jan=1, January=1, janv=1, janvier=1, jan=1, january=1,
  Feb=2, February=2, Fév=2, fév=2, fevrier=2, février=2, feb=2, february=2,
  Mar=3, March=3, mars=3, mar=3, march=3,
  Apr=4, April=4, avr=4, avril=4, apr=4, april=4,
  May=5, mai=5, may=5,
  Jun=6, June=6, juin=6, jun=6, june=6,
  Jul=7, July=7, juil=7, juillet=7, jul=7, july=7,
  Aug=8, August=8, aout=8, août=8, aug=8, august=8,
  Sep=9, Sept=9, September=9, sept=9, septembre=9, sep=9, september=9,
  Oct=10, October=10, oct=10, octobre=10, oct=10, october=10,
  Nov=11, November=11, nov=11, novembre=11, nov=11, november=11,
  Dec=12, December=12, déc=12, dec=12, décembre=12, dec=12, december=12
)

# create regex and use str_extract to take out the month
months_alphabetic_regex <- paste0("\\b(", paste(names(month_lookup), collapse="|"), ")\\b")
obs10$just_month_when_alphabetic <- str_extract(obs10$observed_on_string, months_alphabetic_regex)
#nb_alhpabetic <- length(which(!is.na(obs10$just_month_when_alphabetic))) # roughly 15% of cases

# assigning it to new column
obs10$month_numeric_when_alphabetic <- sapply(obs10$just_month_when_alphabetic, function(x){
  if (!is.na(x)){
    month_lookup[[x]]
  }
  else{
    NA
  }
})

# Finding the dates from these rows - should be the only " XX " in the string
obs10$day_when_month_alphabetical <- NA
rows_with_alphabetical_month <- !is.na(obs10$month_numeric_when_alphabetic)
positions_alpha_day <- regexec("\\s(\\d{2})\\s", obs10$observed_on_string[rows_with_alphabetical_month])
day_when_alpha <- regmatches(obs10$observed_on_string[rows_with_alphabetical_month], positions_alpha_day)
obs10$day_when_month_alphabetical[rows_with_alphabetical_month] <- 
  sapply(day_when_alpha, function(x) x[2])

#looking for errors
#length(which(sapply(obs10$day_when_month_alphabetical, length)> 1)) # any double dates? should be none
#length(which(obs10$day_when_month_alphabetical > 31))# any false dates? should be none


# Section 4: Extracting months and dates when listed as yyyy-m?d?-m?d?
# Issue: many formats (yyyy-mm-dd, yyyy-dd-mm, yyyy-d-m, yyyy-m-d)

# New column to contain only dates of this format
obs10$yyyy_md_md <- regmatches(obs10$observed_on_string, gregexpr("\\d{4}[/-]\\d{2}[/-]\\d{2}", obs10$observed_on_string))
obs10$yyyy_md_md[sapply(obs10$yyyy_md_md, length)==0] <- NA
#nb_yyyy_md_md <- length(which(!is.na(obs10$yyyy_md_md))) # the vast majority

# Extracting the numbers
obs10$yyyy_md_md_string <- sapply(obs10$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", obs10$yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
obs10$yyyy_md_md_vector <- regmatches(obs10$yyyy_md_md_string, positions_yyyy_md_md)
obs10$yyyy_md_md_string <- NULL

first_md <- as.integer(sapply(obs10$yyyy_md_md_vector, function(x) return(x[3])))
second_md <- as.integer(sapply(obs10$yyyy_md_md_vector, function(x) return(x[4])))
obs10$yyyy_md_md_vector <- NULL

# Creating function to extract date and month based on the values of first_md and second_md.
# It seems like the vast majority should be month first.
choosing_month_or_date <- function(first_number, second_number){
  
  day <- NA
  month <- NA
  likely_day <- NA
  likely_month <- NA
  
  if (is.na(first_number) && is.na(second_number)){
    # do nothing
  }
  else if (first_number > 12){
    day <- first_number
    month <- second_number
  }
  else if (second_number > 12){
    day <- second_number
    month <- first_number
  }
  else { # when ambiguous, assume first number is the month but mark as ambiguous
    likely_day <- second_number
    likely_month <- first_number
  }
  return(list(day=day,month=month,likely_day=likely_day,likely_month=likely_month))
}

# Applying the function to our data and assigning results to columns 
yyyy_md_md_results <- mapply(choosing_month_or_date,first_md, second_md, SIMPLIFY=FALSE)

obs10$month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$month)
obs10$day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$day)
obs10$likely_month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_month)
obs10$likely_day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_day)



# Section 5: Extracting months and dates when listed as md-md-yyyy - there are very few
# Issue:  many formats (mm-dd-yyyy, dd-mm-yyyy, d-m-yyyy, m-d-yyyy)

# New column to contain only dates of this format
obs10$md_md_yyyy <- regmatches(obs10$observed_on_string, gregexpr("\\d{1,2}[-/]\\d{1,2}[-/]\\d{4}", obs10$observed_on_string))
obs10$md_md_yyyy[sapply(obs10$md_md_yyyy, length)==0] <- NA
#nb_md_md_yyyy <- length(which(!is.na(obs10$md_md_yyyy))) # very few

# Extract the numbers
obs10$md_md_yyyy_string <- sapply(obs10$md_md_yyyy, function(x) if(length(x)==0) NA else x[1])
positions_md_md_yyyy <- regexec("(\\d{1,2})[-/](\\d{1,2})[-/](\\d{4})", obs10$md_md_yyyy_string)
obs10$md_md_yyyy_vector <- NA
obs10$md_md_yyyy_vector <- regmatches(obs10$md_md_yyyy_string, positions_md_md_yyyy)
obs10$md_md_yyyy_string <- NULL

first_md_2 <- as.integer(sapply(obs10$md_md_yyyy_vector, function(x) return(x[3])))
second_md_2 <- as.integer(sapply(obs10$md_md_yyyy_vector, function(x) return(x[4])))
obs10$md_md_yyyy_vector <- NULL

# Applying the function to our data and assigning results to columns 
md_md_yyyy_results <- mapply(choosing_month_or_date, first_md_2, second_md_2, SIMPLIFY=FALSE)

obs10$month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$month)
obs10$day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$day)
obs10$likely_month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_month)
obs10$likely_day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_day)

#confirm they were all assigned
#length(which(!is.na(obs10$day_when_md_md_yyyy))) 



# Section 5: Dealing with exceptions
#nb_no_date <- length(obs10$observed_on_string[is.na(obs10$observed_on_string)]) # when no date is listed
# very few (0.02%) have some non-NA entry other than the formats we've handled. deal with them here if one wishes to
#nb_entries_not_caught <- nrow(obs10) - sum(nb_alhpabetic, nb_md_md_yyyy, nb_yyyy_md_md, nb_no_date)



# Section 6: Assembling uniform date column 
# 6.1 assembling together different parts of the date
obs10$certain_dd <-
  ifelse(!is.na(obs10$day_when_month_alphabetical), obs10$day_when_month_alphabetical,
         ifelse(!is.na(obs10$day_when_yyyy_md_md), obs10$day_when_yyyy_md_md,
                ifelse(!is.na(obs10$day_when_md_md_yyyy), obs10$day_when_md_md_yyyy, NA)))

obs10$probable_dd <- 
  ifelse(!is.na(obs10$likely_day_when_yyyy_md_md), obs10$likely_day_when_yyyy_md_md,
         ifelse(!is.na(obs10$likely_day_when_md_md_yyyy), obs10$likely_day_when_md_md_yyyy, NA))

obs10$certain_mm <-
  ifelse(!is.na(obs10$month_numeric_when_alphabetic), obs10$month_numeric_when_alphabetic,
         ifelse(!is.na(obs10$month_when_yyyy_md_md), obs10$month_when_yyyy_md_md,
                ifelse(!is.na(obs10$month_when_md_md_yyyy), obs10$month_when_md_md_yyyy, NA)))

obs10$probable_mm <-
  ifelse(!is.na(obs10$likely_month_when_yyyy_md_md), obs10$likely_month_when_yyyy_md_md,
         ifelse(!is.na(obs10$likely_month_when_md_md_yyyy), obs10$likely_month_when_md_md_yyyy, NA))

# 6.2 assembling final columns
certain_dd <- ifelse(!is.na(obs10$certain_dd), obs10$certain_dd, "??")
certain_mm <- ifelse(!is.na(obs10$certain_mm), obs10$certain_mm, "??")
certain_yyyy <- ifelse(!is.na(obs10$year), obs10$year, "????")

obs10$certain_ddmmyyyy <-
  paste(
    ifelse(!is.na(obs10$certain_dd), obs10$certain_dd, "??"),
    ifelse(!is.na(obs10$certain_mm), obs10$certain_mm, "??"),
    ifelse(!is.na(obs10$year), obs10$year, "??"),
    sep="/")

obs10$probable_ddmmyyyy <-
  ifelse(
    (!is.na(obs10$probable_dd) & !is.na(obs10$probable_mm) & !is.na(obs10$year)),
    paste(obs10$probable_dd, obs10$probable_mm, obs10$year, sep="/"),
    NA)

obs10$standardized_ddmmyyyy <-
  ifelse((!is.na(obs10$probable_ddmmyyyy)), obs10$probable_ddmmyyyy, obs10$certain_ddmmyyyy)


# # Section 7: Assessing the quality of the "assumed" dates of ambiguous cases
# # when there is indeed a day > 12 (clear case), what percentage of them are in the first position? 
# # this serves as an estimate for the non-clear cases, to see how accurate our month-first assumption is
# nb_total_obs <- nrow(obs10)
# test_yyyy_md_md_string <- sapply(obs10$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
# positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", test_yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
# test_yyyy_md_md_vector <- regmatches(test_yyyy_md_md_string, positions_yyyy_md_md)
# test_first_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[3])))
# test_second_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[4])))
# 
# counting_error_rate <- function(first_number, second_number){
#   
#   both_na <- is.na(test_first_md) & is.na(second_number)
#   total_yyyy_md_md <- sum(!is.na(first_number))
#   first_md_greater_than_12 <- first_number > 12
#   second_md_greater_than_12 <- second_number > 12
#   ambiguous_cases <- !(both_na | first_md_greater_than_12 | second_md_greater_than_12)
#   
#   nb_first_md_gt12 <- sum(first_md_greater_than_12, na.rm = TRUE)
#   nb_second_md_gt12 <- sum(second_md_greater_than_12, na.rm = TRUE)
#   total_clear_cases <- nb_first_md_gt12 + nb_second_md_gt12
#   nb_ambiguous <- sum(ambiguous_cases, na.rm=TRUE)
#   nb_both_na <- sum(both_na)
#   
#   percent_of_clear_cases_with_day_first <- (nb_first_md_gt12/total_clear_cases)*100
#   percent_successful_guess_of_month_first <- round((100-percent_of_clear_cases_with_day_first), 8)
#   
#   return( list(
#     summary = paste(
#       c(
#         paste("total cases of yyyy-md-md: ", total_yyyy_md_md),
#         paste("ambiguous cases:", nb_ambiguous),
#         paste("clear cases:", total_clear_cases),
#         paste0("percent of all the observations caught under this search: ", (total_yyyy_md_md/nb_total_obs)*100,"%"),
#         paste0("percent of yyyy-md-md cases that are ambiguous: ", (nb_ambiguous/total_yyyy_md_md)*100,"%"),
#         paste0("percent of clear cases with day first: ", percent_of_clear_cases_with_day_first,"%")
#       ),
#       collapse="\n"
#     ),
#     percent_successful_guess_of_month_first = percent_successful_guess_of_month_first
#   ))
# }
# 
# 
# test_result <- counting_error_rate(test_first_md, test_second_md)
# cat(test_result$summary) # so 99.9996% of cases, the first number is the day
# percent_successful_guess_of_month_first <- test_result$percent_successful_guess_of_month_first
# 
# # Indicating this error rate in the df
# probable_date_string <- paste0("probable_ddmmyyyy_to_approx_", percent_successful_guess_of_month_first,"_percent_certainty")
# colnames(obs10)[colnames(obs10) == "probable_ddmmyyyy"] <- probable_date_string

# Creating easy column to identify certain dates over probable dates
obs10$is_ambiguous <- 
  ifelse(is.na(obs10$probable_ddmmyyyy),
         0,
         1)

# # Screen for months that should not be
# improper_months_regex <- paste0("^\\d{1,2}/(", paste0(13:99, collapse = "|"), ")/\\d{4}$")
# erroneous_months <- which(!is.na(str_extract(obs10$standardized_ddmmyyyy, improper_months_regex)))
# erroneous_months_df <- obs10[erroneous_months,]

# Section 8: Writing out data with unified columns to desired location
obs10$year <- NULL
obs10$month_alphabetic <- NULL
obs10$day_when_month_alphabetical <- NULL
obs10$yyyy_md_md <- NULL
obs10$month_when_yyyy_md_md <- NULL
obs10$day_when_yyyy_md_md <- NULL
obs10$likely_month_when_yyyy_md_md <- NULL
obs10$likely_day_when_yyyy_md_md <- NULL
obs10$md_md_yyyy <- NULL
obs10$month_when_md_md_yyyy <- NULL
obs10$day_when_md_md_yyyy <- NULL
obs10$likely_month_when_md_md_yyyy <- NULL
obs10$likely_day_when_md_md_yyyy <- NULL
obs10$certain_dd <- NULL
obs10$probable_dd <- NULL
obs10$certain_mm <- NULL
obs10$probable_mm <- NULL
obs10$certain_ddmmyyyy <- NULL
obs10$probable_ddmmyyyy <- NULL
obs10$just_month_when_alphabetic <- NULL
obs10$month_numeric_when_alphabetic <- NULL








################################# 11 #########################


# Section 2: Extracting the year
#2.1 first pass - grep any set of 4 numbers
obs11$year4num <- regmatches(obs11$observed_on_string, gregexpr("\\d{4}", obs11$observed_on_string))
obs11$year <- obs11$year4num
obs11$year <- sapply(obs11$year4num, function(x){ ifelse(length(x)==0,NA,x[1])}) # to NA the ones with no year
#length(which(is.na(obs11$year))) # about 95% of these cause theres no entry at all. 5% have an entry without a year

#2.2 second pass - correct those with multiple entries of 4 numbers
twos_indx <- which(sapply(obs11$year4num, length) == 2)

year_only <- lapply(obs11$year4num[twos_indx], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 1800 & x_num < 2026] # the plausible 4digit is used
})
obs11$year[twos_indx] <- year_only

#2.3 Any more still left? Then refine date inclusion criteria
twos_indx_still <- which(sapply(obs11$year, length) == 2)
obs11$observed_on_string[twos_indx_still] # displays the dates

# All years 2015 and 2016 with manual check, so use >2000. Change this if need be for different dfs
year_only <- lapply(obs11$year[twos_indx_still], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 2000]
})
obs11$year[twos_indx_still] <- year_only

# 2.4 Final check - any left? If so manually edit your df
#length(which(sapply(obs11$year, length) == 2)) # if empty, no further checks needed
obs11$year4num <- NULL




# Section 3 - Extracting the Month and dates when month listed as letters

# create a lookup table
month_lookup <- c(
  Jan=1, January=1, janv=1, janvier=1, jan=1, january=1,
  Feb=2, February=2, Fév=2, fév=2, fevrier=2, février=2, feb=2, february=2,
  Mar=3, March=3, mars=3, mar=3, march=3,
  Apr=4, April=4, avr=4, avril=4, apr=4, april=4,
  May=5, mai=5, may=5,
  Jun=6, June=6, juin=6, jun=6, june=6,
  Jul=7, July=7, juil=7, juillet=7, jul=7, july=7,
  Aug=8, August=8, aout=8, août=8, aug=8, august=8,
  Sep=9, Sept=9, September=9, sept=9, septembre=9, sep=9, september=9,
  Oct=10, October=10, oct=10, octobre=10, oct=10, october=10,
  Nov=11, November=11, nov=11, novembre=11, nov=11, november=11,
  Dec=12, December=12, déc=12, dec=12, décembre=12, dec=12, december=12
)

# create regex and use str_extract to take out the month
months_alphabetic_regex <- paste0("\\b(", paste(names(month_lookup), collapse="|"), ")\\b")
obs11$just_month_when_alphabetic <- str_extract(obs11$observed_on_string, months_alphabetic_regex)
#nb_alhpabetic <- length(which(!is.na(obs11$just_month_when_alphabetic))) # roughly 15% of cases

# assigning it to new column
obs11$month_numeric_when_alphabetic <- sapply(obs11$just_month_when_alphabetic, function(x){
  if (!is.na(x)){
    month_lookup[[x]]
  }
  else{
    NA
  }
})

# Finding the dates from these rows - should be the only " XX " in the string
obs11$day_when_month_alphabetical <- NA
rows_with_alphabetical_month <- !is.na(obs11$month_numeric_when_alphabetic)
positions_alpha_day <- regexec("\\s(\\d{2})\\s", obs11$observed_on_string[rows_with_alphabetical_month])
day_when_alpha <- regmatches(obs11$observed_on_string[rows_with_alphabetical_month], positions_alpha_day)
obs11$day_when_month_alphabetical[rows_with_alphabetical_month] <- 
  sapply(day_when_alpha, function(x) x[2])

#looking for errors
#length(which(sapply(obs11$day_when_month_alphabetical, length)> 1)) # any double dates? should be none
#length(which(obs11$day_when_month_alphabetical > 31))# any false dates? should be none


# Section 4: Extracting months and dates when listed as yyyy-m?d?-m?d?
# Issue: many formats (yyyy-mm-dd, yyyy-dd-mm, yyyy-d-m, yyyy-m-d)

# New column to contain only dates of this format
obs11$yyyy_md_md <- regmatches(obs11$observed_on_string, gregexpr("\\d{4}[/-]\\d{2}[/-]\\d{2}", obs11$observed_on_string))
obs11$yyyy_md_md[sapply(obs11$yyyy_md_md, length)==0] <- NA
#nb_yyyy_md_md <- length(which(!is.na(obs11$yyyy_md_md))) # the vast majority

# Extracting the numbers
obs11$yyyy_md_md_string <- sapply(obs11$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", obs11$yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
obs11$yyyy_md_md_vector <- regmatches(obs11$yyyy_md_md_string, positions_yyyy_md_md)
obs11$yyyy_md_md_string <- NULL

first_md <- as.integer(sapply(obs11$yyyy_md_md_vector, function(x) return(x[3])))
second_md <- as.integer(sapply(obs11$yyyy_md_md_vector, function(x) return(x[4])))
obs11$yyyy_md_md_vector <- NULL

# Creating function to extract date and month based on the values of first_md and second_md.
# It seems like the vast majority should be month first.
choosing_month_or_date <- function(first_number, second_number){
  
  day <- NA
  month <- NA
  likely_day <- NA
  likely_month <- NA
  
  if (is.na(first_number) && is.na(second_number)){
    # do nothing
  }
  else if (first_number > 12){
    day <- first_number
    month <- second_number
  }
  else if (second_number > 12){
    day <- second_number
    month <- first_number
  }
  else { # when ambiguous, assume first number is the month but mark as ambiguous
    likely_day <- second_number
    likely_month <- first_number
  }
  return(list(day=day,month=month,likely_day=likely_day,likely_month=likely_month))
}

# Applying the function to our data and assigning results to columns 
yyyy_md_md_results <- mapply(choosing_month_or_date,first_md, second_md, SIMPLIFY=FALSE)

obs11$month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$month)
obs11$day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$day)
obs11$likely_month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_month)
obs11$likely_day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_day)



# Section 5: Extracting months and dates when listed as md-md-yyyy - there are very few
# Issue:  many formats (mm-dd-yyyy, dd-mm-yyyy, d-m-yyyy, m-d-yyyy)

# New column to contain only dates of this format
obs11$md_md_yyyy <- regmatches(obs11$observed_on_string, gregexpr("\\d{1,2}[-/]\\d{1,2}[-/]\\d{4}", obs11$observed_on_string))
obs11$md_md_yyyy[sapply(obs11$md_md_yyyy, length)==0] <- NA
#nb_md_md_yyyy <- length(which(!is.na(obs11$md_md_yyyy))) # very few

# Extract the numbers
obs11$md_md_yyyy_string <- sapply(obs11$md_md_yyyy, function(x) if(length(x)==0) NA else x[1])
positions_md_md_yyyy <- regexec("(\\d{1,2})[-/](\\d{1,2})[-/](\\d{4})", obs11$md_md_yyyy_string)
obs11$md_md_yyyy_vector <- NA
obs11$md_md_yyyy_vector <- regmatches(obs11$md_md_yyyy_string, positions_md_md_yyyy)
obs11$md_md_yyyy_string <- NULL

first_md_2 <- as.integer(sapply(obs11$md_md_yyyy_vector, function(x) return(x[3])))
second_md_2 <- as.integer(sapply(obs11$md_md_yyyy_vector, function(x) return(x[4])))
obs11$md_md_yyyy_vector <- NULL

# Applying the function to our data and assigning results to columns 
md_md_yyyy_results <- mapply(choosing_month_or_date, first_md_2, second_md_2, SIMPLIFY=FALSE)

obs11$month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$month)
obs11$day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$day)
obs11$likely_month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_month)
obs11$likely_day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_day)

#confirm they were all assigned
#length(which(!is.na(obs11$day_when_md_md_yyyy))) 



# Section 5: Dealing with exceptions
#nb_no_date <- length(obs11$observed_on_string[is.na(obs11$observed_on_string)]) # when no date is listed
# very few (0.02%) have some non-NA entry other than the formats we've handled. deal with them here if one wishes to
#nb_entries_not_caught <- nrow(obs11) - sum(nb_alhpabetic, nb_md_md_yyyy, nb_yyyy_md_md, nb_no_date)



# Section 6: Assembling uniform date column 
# 6.1 assembling together different parts of the date
obs11$certain_dd <-
  ifelse(!is.na(obs11$day_when_month_alphabetical), obs11$day_when_month_alphabetical,
         ifelse(!is.na(obs11$day_when_yyyy_md_md), obs11$day_when_yyyy_md_md,
                ifelse(!is.na(obs11$day_when_md_md_yyyy), obs11$day_when_md_md_yyyy, NA)))

obs11$probable_dd <- 
  ifelse(!is.na(obs11$likely_day_when_yyyy_md_md), obs11$likely_day_when_yyyy_md_md,
         ifelse(!is.na(obs11$likely_day_when_md_md_yyyy), obs11$likely_day_when_md_md_yyyy, NA))

obs11$certain_mm <-
  ifelse(!is.na(obs11$month_numeric_when_alphabetic), obs11$month_numeric_when_alphabetic,
         ifelse(!is.na(obs11$month_when_yyyy_md_md), obs11$month_when_yyyy_md_md,
                ifelse(!is.na(obs11$month_when_md_md_yyyy), obs11$month_when_md_md_yyyy, NA)))

obs11$probable_mm <-
  ifelse(!is.na(obs11$likely_month_when_yyyy_md_md), obs11$likely_month_when_yyyy_md_md,
         ifelse(!is.na(obs11$likely_month_when_md_md_yyyy), obs11$likely_month_when_md_md_yyyy, NA))

# 6.2 assembling final columns
certain_dd <- ifelse(!is.na(obs11$certain_dd), obs11$certain_dd, "??")
certain_mm <- ifelse(!is.na(obs11$certain_mm), obs11$certain_mm, "??")
certain_yyyy <- ifelse(!is.na(obs11$year), obs11$year, "????")

obs11$certain_ddmmyyyy <-
  paste(
    ifelse(!is.na(obs11$certain_dd), obs11$certain_dd, "??"),
    ifelse(!is.na(obs11$certain_mm), obs11$certain_mm, "??"),
    ifelse(!is.na(obs11$year), obs11$year, "??"),
    sep="/")

obs11$probable_ddmmyyyy <-
  ifelse(
    (!is.na(obs11$probable_dd) & !is.na(obs11$probable_mm) & !is.na(obs11$year)),
    paste(obs11$probable_dd, obs11$probable_mm, obs11$year, sep="/"),
    NA)

obs11$standardized_ddmmyyyy <-
  ifelse((!is.na(obs11$probable_ddmmyyyy)), obs11$probable_ddmmyyyy, obs11$certain_ddmmyyyy)


# # Section 7: Assessing the quality of the "assumed" dates of ambiguous cases
# # when there is indeed a day > 12 (clear case), what percentage of them are in the first position? 
# # this serves as an estimate for the non-clear cases, to see how accurate our month-first assumption is
# nb_total_obs <- nrow(obs11)
# test_yyyy_md_md_string <- sapply(obs11$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
# positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", test_yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
# test_yyyy_md_md_vector <- regmatches(test_yyyy_md_md_string, positions_yyyy_md_md)
# test_first_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[3])))
# test_second_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[4])))
# 
# counting_error_rate <- function(first_number, second_number){
#   
#   both_na <- is.na(test_first_md) & is.na(second_number)
#   total_yyyy_md_md <- sum(!is.na(first_number))
#   first_md_greater_than_12 <- first_number > 12
#   second_md_greater_than_12 <- second_number > 12
#   ambiguous_cases <- !(both_na | first_md_greater_than_12 | second_md_greater_than_12)
#   
#   nb_first_md_gt12 <- sum(first_md_greater_than_12, na.rm = TRUE)
#   nb_second_md_gt12 <- sum(second_md_greater_than_12, na.rm = TRUE)
#   total_clear_cases <- nb_first_md_gt12 + nb_second_md_gt12
#   nb_ambiguous <- sum(ambiguous_cases, na.rm=TRUE)
#   nb_both_na <- sum(both_na)
#   
#   percent_of_clear_cases_with_day_first <- (nb_first_md_gt12/total_clear_cases)*100
#   percent_successful_guess_of_month_first <- round((100-percent_of_clear_cases_with_day_first), 8)
#   
#   return( list(
#     summary = paste(
#       c(
#         paste("total cases of yyyy-md-md: ", total_yyyy_md_md),
#         paste("ambiguous cases:", nb_ambiguous),
#         paste("clear cases:", total_clear_cases),
#         paste0("percent of all the observations caught under this search: ", (total_yyyy_md_md/nb_total_obs)*100,"%"),
#         paste0("percent of yyyy-md-md cases that are ambiguous: ", (nb_ambiguous/total_yyyy_md_md)*100,"%"),
#         paste0("percent of clear cases with day first: ", percent_of_clear_cases_with_day_first,"%")
#       ),
#       collapse="\n"
#     ),
#     percent_successful_guess_of_month_first = percent_successful_guess_of_month_first
#   ))
# }
# 
# 
# test_result <- counting_error_rate(test_first_md, test_second_md)
# cat(test_result$summary) # so 99.9996% of cases, the first number is the day
# percent_successful_guess_of_month_first <- test_result$percent_successful_guess_of_month_first
# 
# # Indicating this error rate in the df
# probable_date_string <- paste0("probable_ddmmyyyy_to_approx_", percent_successful_guess_of_month_first,"_percent_certainty")
# colnames(obs11)[colnames(obs11) == "probable_ddmmyyyy"] <- probable_date_string

# Creating easy column to identify certain dates over probable dates
obs11$is_ambiguous <- 
  ifelse(is.na(obs11$probable_ddmmyyyy),
         0,
         1)

# # Screen for months that should not be
# improper_months_regex <- paste0("^\\d{1,2}/(", paste0(13:99, collapse = "|"), ")/\\d{4}$")
# erroneous_months <- which(!is.na(str_extract(obs11$standardized_ddmmyyyy, improper_months_regex)))
# erroneous_months_df <- obs11[erroneous_months,]

# Section 8: Writing out data with unified columns to desired location
obs11$year <- NULL
obs11$month_alphabetic <- NULL
obs11$day_when_month_alphabetical <- NULL
obs11$yyyy_md_md <- NULL
obs11$month_when_yyyy_md_md <- NULL
obs11$day_when_yyyy_md_md <- NULL
obs11$likely_month_when_yyyy_md_md <- NULL
obs11$likely_day_when_yyyy_md_md <- NULL
obs11$md_md_yyyy <- NULL
obs11$month_when_md_md_yyyy <- NULL
obs11$day_when_md_md_yyyy <- NULL
obs11$likely_month_when_md_md_yyyy <- NULL
obs11$likely_day_when_md_md_yyyy <- NULL
obs11$certain_dd <- NULL
obs11$probable_dd <- NULL
obs11$certain_mm <- NULL
obs11$probable_mm <- NULL
obs11$certain_ddmmyyyy <- NULL
obs11$probable_ddmmyyyy <- NULL
obs11$just_month_when_alphabetic <- NULL
obs11$month_numeric_when_alphabetic <- NULL





###################################12#########################

# Section 2: Extracting the year
#2.1 first pass - grep any set of 4 numbers
obs12$year4num <- regmatches(obs12$observed_on_string, gregexpr("\\d{4}", obs12$observed_on_string))
obs12$year <- obs12$year4num
obs12$year <- sapply(obs12$year4num, function(x){ ifelse(length(x)==0,NA,x[1])}) # to NA the ones with no year
#length(which(is.na(obs12$year))) # about 95% of these cause theres no entry at all. 5% have an entry without a year

#2.2 second pass - correct those with multiple entries of 4 numbers
twos_indx <- which(sapply(obs12$year4num, length) == 2)

year_only <- lapply(obs12$year4num[twos_indx], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 1800 & x_num < 2026] # the plausible 4digit is used
})
obs12$year[twos_indx] <- year_only

#2.3 Any more still left? Then refine date inclusion criteria
twos_indx_still <- which(sapply(obs12$year, length) == 2)
obs12$observed_on_string[twos_indx_still] # displays the dates

# All years 2015 and 2016 with manual check, so use >2000. Change this if need be for different dfs
year_only <- lapply(obs12$year[twos_indx_still], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 2000]
})
obs12$year[twos_indx_still] <- year_only

# 2.4 Final check - any left? If so manually edit your df
#length(which(sapply(obs12$year, length) == 2)) # if empty, no further checks needed
obs12$year4num <- NULL




# Section 3 - Extracting the Month and dates when month listed as letters

# create a lookup table
month_lookup <- c(
  Jan=1, January=1, janv=1, janvier=1, jan=1, january=1,
  Feb=2, February=2, Fév=2, fév=2, fevrier=2, février=2, feb=2, february=2,
  Mar=3, March=3, mars=3, mar=3, march=3,
  Apr=4, April=4, avr=4, avril=4, apr=4, april=4,
  May=5, mai=5, may=5,
  Jun=6, June=6, juin=6, jun=6, june=6,
  Jul=7, July=7, juil=7, juillet=7, jul=7, july=7,
  Aug=8, August=8, aout=8, août=8, aug=8, august=8,
  Sep=9, Sept=9, September=9, sept=9, septembre=9, sep=9, september=9,
  Oct=10, October=10, oct=10, octobre=10, oct=10, october=10,
  Nov=11, November=11, nov=11, novembre=11, nov=11, november=11,
  Dec=12, December=12, déc=12, dec=12, décembre=12, dec=12, december=12
)

# create regex and use str_extract to take out the month
months_alphabetic_regex <- paste0("\\b(", paste(names(month_lookup), collapse="|"), ")\\b")
obs12$just_month_when_alphabetic <- str_extract(obs12$observed_on_string, months_alphabetic_regex)
#nb_alhpabetic <- length(which(!is.na(obs12$just_month_when_alphabetic))) # roughly 15% of cases

# assigning it to new column
obs12$month_numeric_when_alphabetic <- sapply(obs12$just_month_when_alphabetic, function(x){
  if (!is.na(x)){
    month_lookup[[x]]
  }
  else{
    NA
  }
})

# Finding the dates from these rows - should be the only " XX " in the string
obs12$day_when_month_alphabetical <- NA
rows_with_alphabetical_month <- !is.na(obs12$month_numeric_when_alphabetic)
positions_alpha_day <- regexec("\\s(\\d{2})\\s", obs12$observed_on_string[rows_with_alphabetical_month])
day_when_alpha <- regmatches(obs12$observed_on_string[rows_with_alphabetical_month], positions_alpha_day)
obs12$day_when_month_alphabetical[rows_with_alphabetical_month] <- 
  sapply(day_when_alpha, function(x) x[2])

#looking for errors
#length(which(sapply(obs12$day_when_month_alphabetical, length)> 1)) # any double dates? should be none
#length(which(obs12$day_when_month_alphabetical > 31))# any false dates? should be none


# Section 4: Extracting months and dates when listed as yyyy-m?d?-m?d?
# Issue: many formats (yyyy-mm-dd, yyyy-dd-mm, yyyy-d-m, yyyy-m-d)

# New column to contain only dates of this format
obs12$yyyy_md_md <- regmatches(obs12$observed_on_string, gregexpr("\\d{4}[/-]\\d{2}[/-]\\d{2}", obs12$observed_on_string))
obs12$yyyy_md_md[sapply(obs12$yyyy_md_md, length)==0] <- NA
#nb_yyyy_md_md <- length(which(!is.na(obs12$yyyy_md_md))) # the vast majority

# Extracting the numbers
obs12$yyyy_md_md_string <- sapply(obs12$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", obs12$yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
obs12$yyyy_md_md_vector <- regmatches(obs12$yyyy_md_md_string, positions_yyyy_md_md)
obs12$yyyy_md_md_string <- NULL

first_md <- as.integer(sapply(obs12$yyyy_md_md_vector, function(x) return(x[3])))
second_md <- as.integer(sapply(obs12$yyyy_md_md_vector, function(x) return(x[4])))
obs12$yyyy_md_md_vector <- NULL

# Creating function to extract date and month based on the values of first_md and second_md.
# It seems like the vast majority should be month first.
choosing_month_or_date <- function(first_number, second_number){
  
  day <- NA
  month <- NA
  likely_day <- NA
  likely_month <- NA
  
  if (is.na(first_number) && is.na(second_number)){
    # do nothing
  }
  else if (first_number > 12){
    day <- first_number
    month <- second_number
  }
  else if (second_number > 12){
    day <- second_number
    month <- first_number
  }
  else { # when ambiguous, assume first number is the month but mark as ambiguous
    likely_day <- second_number
    likely_month <- first_number
  }
  return(list(day=day,month=month,likely_day=likely_day,likely_month=likely_month))
}

# Applying the function to our data and assigning results to columns 
yyyy_md_md_results <- mapply(choosing_month_or_date,first_md, second_md, SIMPLIFY=FALSE)

obs12$month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$month)
obs12$day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$day)
obs12$likely_month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_month)
obs12$likely_day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_day)



# Section 5: Extracting months and dates when listed as md-md-yyyy - there are very few
# Issue:  many formats (mm-dd-yyyy, dd-mm-yyyy, d-m-yyyy, m-d-yyyy)

# New column to contain only dates of this format
obs12$md_md_yyyy <- regmatches(obs12$observed_on_string, gregexpr("\\d{1,2}[-/]\\d{1,2}[-/]\\d{4}", obs12$observed_on_string))
obs12$md_md_yyyy[sapply(obs12$md_md_yyyy, length)==0] <- NA
#nb_md_md_yyyy <- length(which(!is.na(obs12$md_md_yyyy))) # very few

# Extract the numbers
obs12$md_md_yyyy_string <- sapply(obs12$md_md_yyyy, function(x) if(length(x)==0) NA else x[1])
positions_md_md_yyyy <- regexec("(\\d{1,2})[-/](\\d{1,2})[-/](\\d{4})", obs12$md_md_yyyy_string)
obs12$md_md_yyyy_vector <- NA
obs12$md_md_yyyy_vector <- regmatches(obs12$md_md_yyyy_string, positions_md_md_yyyy)
obs12$md_md_yyyy_string <- NULL

first_md_2 <- as.integer(sapply(obs12$md_md_yyyy_vector, function(x) return(x[3])))
second_md_2 <- as.integer(sapply(obs12$md_md_yyyy_vector, function(x) return(x[4])))
obs12$md_md_yyyy_vector <- NULL

# Applying the function to our data and assigning results to columns 
md_md_yyyy_results <- mapply(choosing_month_or_date, first_md_2, second_md_2, SIMPLIFY=FALSE)

obs12$month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$month)
obs12$day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$day)
obs12$likely_month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_month)
obs12$likely_day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_day)

#confirm they were all assigned
#length(which(!is.na(obs12$day_when_md_md_yyyy))) 



# Section 5: Dealing with exceptions
#nb_no_date <- length(obs12$observed_on_string[is.na(obs12$observed_on_string)]) # when no date is listed
# very few (0.02%) have some non-NA entry other than the formats we've handled. deal with them here if one wishes to
#nb_entries_not_caught <- nrow(obs12) - sum(nb_alhpabetic, nb_md_md_yyyy, nb_yyyy_md_md, nb_no_date)



# Section 6: Assembling uniform date column 
# 6.1 assembling together different parts of the date
obs12$certain_dd <-
  ifelse(!is.na(obs12$day_when_month_alphabetical), obs12$day_when_month_alphabetical,
         ifelse(!is.na(obs12$day_when_yyyy_md_md), obs12$day_when_yyyy_md_md,
                ifelse(!is.na(obs12$day_when_md_md_yyyy), obs12$day_when_md_md_yyyy, NA)))

obs12$probable_dd <- 
  ifelse(!is.na(obs12$likely_day_when_yyyy_md_md), obs12$likely_day_when_yyyy_md_md,
         ifelse(!is.na(obs12$likely_day_when_md_md_yyyy), obs12$likely_day_when_md_md_yyyy, NA))

obs12$certain_mm <-
  ifelse(!is.na(obs12$month_numeric_when_alphabetic), obs12$month_numeric_when_alphabetic,
         ifelse(!is.na(obs12$month_when_yyyy_md_md), obs12$month_when_yyyy_md_md,
                ifelse(!is.na(obs12$month_when_md_md_yyyy), obs12$month_when_md_md_yyyy, NA)))

obs12$probable_mm <-
  ifelse(!is.na(obs12$likely_month_when_yyyy_md_md), obs12$likely_month_when_yyyy_md_md,
         ifelse(!is.na(obs12$likely_month_when_md_md_yyyy), obs12$likely_month_when_md_md_yyyy, NA))

# 6.2 assembling final columns
certain_dd <- ifelse(!is.na(obs12$certain_dd), obs12$certain_dd, "??")
certain_mm <- ifelse(!is.na(obs12$certain_mm), obs12$certain_mm, "??")
certain_yyyy <- ifelse(!is.na(obs12$year), obs12$year, "????")

obs12$certain_ddmmyyyy <-
  paste(
    ifelse(!is.na(obs12$certain_dd), obs12$certain_dd, "??"),
    ifelse(!is.na(obs12$certain_mm), obs12$certain_mm, "??"),
    ifelse(!is.na(obs12$year), obs12$year, "??"),
    sep="/")

obs12$probable_ddmmyyyy <-
  ifelse(
    (!is.na(obs12$probable_dd) & !is.na(obs12$probable_mm) & !is.na(obs12$year)),
    paste(obs12$probable_dd, obs12$probable_mm, obs12$year, sep="/"),
    NA)

obs12$standardized_ddmmyyyy <-
  ifelse((!is.na(obs12$probable_ddmmyyyy)), obs12$probable_ddmmyyyy, obs12$certain_ddmmyyyy)


# # Section 7: Assessing the quality of the "assumed" dates of ambiguous cases
# # when there is indeed a day > 12 (clear case), what percentage of them are in the first position? 
# # this serves as an estimate for the non-clear cases, to see how accurate our month-first assumption is
# nb_total_obs <- nrow(obs12)
# test_yyyy_md_md_string <- sapply(obs12$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
# positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", test_yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
# test_yyyy_md_md_vector <- regmatches(test_yyyy_md_md_string, positions_yyyy_md_md)
# test_first_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[3])))
# test_second_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[4])))
# 
# counting_error_rate <- function(first_number, second_number){
#   
#   both_na <- is.na(test_first_md) & is.na(second_number)
#   total_yyyy_md_md <- sum(!is.na(first_number))
#   first_md_greater_than_12 <- first_number > 12
#   second_md_greater_than_12 <- second_number > 12
#   ambiguous_cases <- !(both_na | first_md_greater_than_12 | second_md_greater_than_12)
#   
#   nb_first_md_gt12 <- sum(first_md_greater_than_12, na.rm = TRUE)
#   nb_second_md_gt12 <- sum(second_md_greater_than_12, na.rm = TRUE)
#   total_clear_cases <- nb_first_md_gt12 + nb_second_md_gt12
#   nb_ambiguous <- sum(ambiguous_cases, na.rm=TRUE)
#   nb_both_na <- sum(both_na)
#   
#   percent_of_clear_cases_with_day_first <- (nb_first_md_gt12/total_clear_cases)*100
#   percent_successful_guess_of_month_first <- round((100-percent_of_clear_cases_with_day_first), 8)
#   
#   return( list(
#     summary = paste(
#       c(
#         paste("total cases of yyyy-md-md: ", total_yyyy_md_md),
#         paste("ambiguous cases:", nb_ambiguous),
#         paste("clear cases:", total_clear_cases),
#         paste0("percent of all the observations caught under this search: ", (total_yyyy_md_md/nb_total_obs)*100,"%"),
#         paste0("percent of yyyy-md-md cases that are ambiguous: ", (nb_ambiguous/total_yyyy_md_md)*100,"%"),
#         paste0("percent of clear cases with day first: ", percent_of_clear_cases_with_day_first,"%")
#       ),
#       collapse="\n"
#     ),
#     percent_successful_guess_of_month_first = percent_successful_guess_of_month_first
#   ))
# }
# 
# 
# test_result <- counting_error_rate(test_first_md, test_second_md)
# cat(test_result$summary) # so 99.9996% of cases, the first number is the day
# percent_successful_guess_of_month_first <- test_result$percent_successful_guess_of_month_first
# 
# # Indicating this error rate in the df
# probable_date_string <- paste0("probable_ddmmyyyy_to_approx_", percent_successful_guess_of_month_first,"_percent_certainty")
# colnames(obs12)[colnames(obs12) == "probable_ddmmyyyy"] <- probable_date_string

# Creating easy column to identify certain dates over probable dates
obs12$is_ambiguous <- 
  ifelse(is.na(obs12$probable_ddmmyyyy),
         0,
         1)

# # Screen for months that should not be
# improper_months_regex <- paste0("^\\d{1,2}/(", paste0(13:99, collapse = "|"), ")/\\d{4}$")
# erroneous_months <- which(!is.na(str_extract(obs12$standardized_ddmmyyyy, improper_months_regex)))
# erroneous_months_df <- obs12[erroneous_months,]

# Section 8: Writing out data with unified columns to desired location
obs12$year <- NULL
obs12$month_alphabetic <- NULL
obs12$day_when_month_alphabetical <- NULL
obs12$yyyy_md_md <- NULL
obs12$month_when_yyyy_md_md <- NULL
obs12$day_when_yyyy_md_md <- NULL
obs12$likely_month_when_yyyy_md_md <- NULL
obs12$likely_day_when_yyyy_md_md <- NULL
obs12$md_md_yyyy <- NULL
obs12$month_when_md_md_yyyy <- NULL
obs12$day_when_md_md_yyyy <- NULL
obs12$likely_month_when_md_md_yyyy <- NULL
obs12$likely_day_when_md_md_yyyy <- NULL
obs12$certain_dd <- NULL
obs12$probable_dd <- NULL
obs12$certain_mm <- NULL
obs12$probable_mm <- NULL
obs12$certain_ddmmyyyy <- NULL
obs12$probable_ddmmyyyy <- NULL
obs12$just_month_when_alphabetic <- NULL
obs12$month_numeric_when_alphabetic <- NULL










################################13############################


# Section 2: Extracting the year
#2.1 first pass - grep any set of 4 numbers
obs13$year4num <- regmatches(obs13$observed_on_string, gregexpr("\\d{4}", obs13$observed_on_string))
obs13$year <- obs13$year4num
obs13$year <- sapply(obs13$year4num, function(x){ ifelse(length(x)==0,NA,x[1])}) # to NA the ones with no year
#length(which(is.na(obs13$year))) # about 95% of these cause theres no entry at all. 5% have an entry without a year

#2.2 second pass - correct those with multiple entries of 4 numbers
twos_indx <- which(sapply(obs13$year4num, length) == 2)

year_only <- lapply(obs13$year4num[twos_indx], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 1800 & x_num < 2026] # the plausible 4digit is used
})
obs13$year[twos_indx] <- year_only

#2.3 Any more still left? Then refine date inclusion criteria
twos_indx_still <- which(sapply(obs13$year, length) == 2)
obs13$observed_on_string[twos_indx_still] # displays the dates

# All years 2015 and 2016 with manual check, so use >2000. Change this if need be for different dfs
year_only <- lapply(obs13$year[twos_indx_still], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 2000]
})
obs13$year[twos_indx_still] <- year_only

# 2.4 Final check - any left? If so manually edit your df
#length(which(sapply(obs13$year, length) == 2)) # if empty, no further checks needed
obs13$year4num <- NULL




# Section 3 - Extracting the Month and dates when month listed as letters

# create a lookup table
month_lookup <- c(
  Jan=1, January=1, janv=1, janvier=1, jan=1, january=1,
  Feb=2, February=2, Fév=2, fév=2, fevrier=2, février=2, feb=2, february=2,
  Mar=3, March=3, mars=3, mar=3, march=3,
  Apr=4, April=4, avr=4, avril=4, apr=4, april=4,
  May=5, mai=5, may=5,
  Jun=6, June=6, juin=6, jun=6, june=6,
  Jul=7, July=7, juil=7, juillet=7, jul=7, july=7,
  Aug=8, August=8, aout=8, août=8, aug=8, august=8,
  Sep=9, Sept=9, September=9, sept=9, septembre=9, sep=9, september=9,
  Oct=10, October=10, oct=10, octobre=10, oct=10, october=10,
  Nov=11, November=11, nov=11, novembre=11, nov=11, november=11,
  Dec=12, December=12, déc=12, dec=12, décembre=12, dec=12, december=12
)

# create regex and use str_extract to take out the month
months_alphabetic_regex <- paste0("\\b(", paste(names(month_lookup), collapse="|"), ")\\b")
obs13$just_month_when_alphabetic <- str_extract(obs13$observed_on_string, months_alphabetic_regex)
#nb_alhpabetic <- length(which(!is.na(obs13$just_month_when_alphabetic))) # roughly 15% of cases

# assigning it to new column
obs13$month_numeric_when_alphabetic <- sapply(obs13$just_month_when_alphabetic, function(x){
  if (!is.na(x)){
    month_lookup[[x]]
  }
  else{
    NA
  }
})

# Finding the dates from these rows - should be the only " XX " in the string
obs13$day_when_month_alphabetical <- NA
rows_with_alphabetical_month <- !is.na(obs13$month_numeric_when_alphabetic)
positions_alpha_day <- regexec("\\s(\\d{2})\\s", obs13$observed_on_string[rows_with_alphabetical_month])
day_when_alpha <- regmatches(obs13$observed_on_string[rows_with_alphabetical_month], positions_alpha_day)
obs13$day_when_month_alphabetical[rows_with_alphabetical_month] <- 
  sapply(day_when_alpha, function(x) x[2])

#looking for errors
#length(which(sapply(obs13$day_when_month_alphabetical, length)> 1)) # any double dates? should be none
#length(which(obs13$day_when_month_alphabetical > 31))# any false dates? should be none


# Section 4: Extracting months and dates when listed as yyyy-m?d?-m?d?
# Issue: many formats (yyyy-mm-dd, yyyy-dd-mm, yyyy-d-m, yyyy-m-d)

# New column to contain only dates of this format
obs13$yyyy_md_md <- regmatches(obs13$observed_on_string, gregexpr("\\d{4}[/-]\\d{2}[/-]\\d{2}", obs13$observed_on_string))
obs13$yyyy_md_md[sapply(obs13$yyyy_md_md, length)==0] <- NA
#nb_yyyy_md_md <- length(which(!is.na(obs13$yyyy_md_md))) # the vast majority

# Extracting the numbers
obs13$yyyy_md_md_string <- sapply(obs13$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", obs13$yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
obs13$yyyy_md_md_vector <- regmatches(obs13$yyyy_md_md_string, positions_yyyy_md_md)
obs13$yyyy_md_md_string <- NULL

first_md <- as.integer(sapply(obs13$yyyy_md_md_vector, function(x) return(x[3])))
second_md <- as.integer(sapply(obs13$yyyy_md_md_vector, function(x) return(x[4])))
obs13$yyyy_md_md_vector <- NULL

# Creating function to extract date and month based on the values of first_md and second_md.
# It seems like the vast majority should be month first.
choosing_month_or_date <- function(first_number, second_number){
  
  day <- NA
  month <- NA
  likely_day <- NA
  likely_month <- NA
  
  if (is.na(first_number) && is.na(second_number)){
    # do nothing
  }
  else if (first_number > 12){
    day <- first_number
    month <- second_number
  }
  else if (second_number > 12){
    day <- second_number
    month <- first_number
  }
  else { # when ambiguous, assume first number is the month but mark as ambiguous
    likely_day <- second_number
    likely_month <- first_number
  }
  return(list(day=day,month=month,likely_day=likely_day,likely_month=likely_month))
}

# Applying the function to our data and assigning results to columns 
yyyy_md_md_results <- mapply(choosing_month_or_date,first_md, second_md, SIMPLIFY=FALSE)

obs13$month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$month)
obs13$day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$day)
obs13$likely_month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_month)
obs13$likely_day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_day)



# Section 5: Extracting months and dates when listed as md-md-yyyy - there are very few
# Issue:  many formats (mm-dd-yyyy, dd-mm-yyyy, d-m-yyyy, m-d-yyyy)

# New column to contain only dates of this format
obs13$md_md_yyyy <- regmatches(obs13$observed_on_string, gregexpr("\\d{1,2}[-/]\\d{1,2}[-/]\\d{4}", obs13$observed_on_string))
obs13$md_md_yyyy[sapply(obs13$md_md_yyyy, length)==0] <- NA
#nb_md_md_yyyy <- length(which(!is.na(obs13$md_md_yyyy))) # very few

# Extract the numbers
obs13$md_md_yyyy_string <- sapply(obs13$md_md_yyyy, function(x) if(length(x)==0) NA else x[1])
positions_md_md_yyyy <- regexec("(\\d{1,2})[-/](\\d{1,2})[-/](\\d{4})", obs13$md_md_yyyy_string)
obs13$md_md_yyyy_vector <- NA
obs13$md_md_yyyy_vector <- regmatches(obs13$md_md_yyyy_string, positions_md_md_yyyy)
obs13$md_md_yyyy_string <- NULL

first_md_2 <- as.integer(sapply(obs13$md_md_yyyy_vector, function(x) return(x[3])))
second_md_2 <- as.integer(sapply(obs13$md_md_yyyy_vector, function(x) return(x[4])))
obs13$md_md_yyyy_vector <- NULL

# Applying the function to our data and assigning results to columns 
md_md_yyyy_results <- mapply(choosing_month_or_date, first_md_2, second_md_2, SIMPLIFY=FALSE)

obs13$month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$month)
obs13$day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$day)
obs13$likely_month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_month)
obs13$likely_day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_day)

#confirm they were all assigned
#length(which(!is.na(obs13$day_when_md_md_yyyy))) 



# Section 5: Dealing with exceptions
#nb_no_date <- length(obs13$observed_on_string[is.na(obs13$observed_on_string)]) # when no date is listed
# very few (0.02%) have some non-NA entry other than the formats we've handled. deal with them here if one wishes to
#nb_entries_not_caught <- nrow(obs13) - sum(nb_alhpabetic, nb_md_md_yyyy, nb_yyyy_md_md, nb_no_date)



# Section 6: Assembling uniform date column 
# 6.1 assembling together different parts of the date
obs13$certain_dd <-
  ifelse(!is.na(obs13$day_when_month_alphabetical), obs13$day_when_month_alphabetical,
         ifelse(!is.na(obs13$day_when_yyyy_md_md), obs13$day_when_yyyy_md_md,
                ifelse(!is.na(obs13$day_when_md_md_yyyy), obs13$day_when_md_md_yyyy, NA)))

obs13$probable_dd <- 
  ifelse(!is.na(obs13$likely_day_when_yyyy_md_md), obs13$likely_day_when_yyyy_md_md,
         ifelse(!is.na(obs13$likely_day_when_md_md_yyyy), obs13$likely_day_when_md_md_yyyy, NA))

obs13$certain_mm <-
  ifelse(!is.na(obs13$month_numeric_when_alphabetic), obs13$month_numeric_when_alphabetic,
         ifelse(!is.na(obs13$month_when_yyyy_md_md), obs13$month_when_yyyy_md_md,
                ifelse(!is.na(obs13$month_when_md_md_yyyy), obs13$month_when_md_md_yyyy, NA)))

obs13$probable_mm <-
  ifelse(!is.na(obs13$likely_month_when_yyyy_md_md), obs13$likely_month_when_yyyy_md_md,
         ifelse(!is.na(obs13$likely_month_when_md_md_yyyy), obs13$likely_month_when_md_md_yyyy, NA))

# 6.2 assembling final columns
certain_dd <- ifelse(!is.na(obs13$certain_dd), obs13$certain_dd, "??")
certain_mm <- ifelse(!is.na(obs13$certain_mm), obs13$certain_mm, "??")
certain_yyyy <- ifelse(!is.na(obs13$year), obs13$year, "????")

obs13$certain_ddmmyyyy <-
  paste(
    ifelse(!is.na(obs13$certain_dd), obs13$certain_dd, "??"),
    ifelse(!is.na(obs13$certain_mm), obs13$certain_mm, "??"),
    ifelse(!is.na(obs13$year), obs13$year, "??"),
    sep="/")

obs13$probable_ddmmyyyy <-
  ifelse(
    (!is.na(obs13$probable_dd) & !is.na(obs13$probable_mm) & !is.na(obs13$year)),
    paste(obs13$probable_dd, obs13$probable_mm, obs13$year, sep="/"),
    NA)

obs13$standardized_ddmmyyyy <-
  ifelse((!is.na(obs13$probable_ddmmyyyy)), obs13$probable_ddmmyyyy, obs13$certain_ddmmyyyy)


# # Section 7: Assessing the quality of the "assumed" dates of ambiguous cases
# # when there is indeed a day > 12 (clear case), what percentage of them are in the first position? 
# # this serves as an estimate for the non-clear cases, to see how accurate our month-first assumption is
# nb_total_obs <- nrow(obs13)
# test_yyyy_md_md_string <- sapply(obs13$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
# positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", test_yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
# test_yyyy_md_md_vector <- regmatches(test_yyyy_md_md_string, positions_yyyy_md_md)
# test_first_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[3])))
# test_second_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[4])))
# 
# counting_error_rate <- function(first_number, second_number){
#   
#   both_na <- is.na(test_first_md) & is.na(second_number)
#   total_yyyy_md_md <- sum(!is.na(first_number))
#   first_md_greater_than_12 <- first_number > 12
#   second_md_greater_than_12 <- second_number > 12
#   ambiguous_cases <- !(both_na | first_md_greater_than_12 | second_md_greater_than_12)
#   
#   nb_first_md_gt12 <- sum(first_md_greater_than_12, na.rm = TRUE)
#   nb_second_md_gt12 <- sum(second_md_greater_than_12, na.rm = TRUE)
#   total_clear_cases <- nb_first_md_gt12 + nb_second_md_gt12
#   nb_ambiguous <- sum(ambiguous_cases, na.rm=TRUE)
#   nb_both_na <- sum(both_na)
#   
#   percent_of_clear_cases_with_day_first <- (nb_first_md_gt12/total_clear_cases)*100
#   percent_successful_guess_of_month_first <- round((100-percent_of_clear_cases_with_day_first), 8)
#   
#   return( list(
#     summary = paste(
#       c(
#         paste("total cases of yyyy-md-md: ", total_yyyy_md_md),
#         paste("ambiguous cases:", nb_ambiguous),
#         paste("clear cases:", total_clear_cases),
#         paste0("percent of all the observations caught under this search: ", (total_yyyy_md_md/nb_total_obs)*100,"%"),
#         paste0("percent of yyyy-md-md cases that are ambiguous: ", (nb_ambiguous/total_yyyy_md_md)*100,"%"),
#         paste0("percent of clear cases with day first: ", percent_of_clear_cases_with_day_first,"%")
#       ),
#       collapse="\n"
#     ),
#     percent_successful_guess_of_month_first = percent_successful_guess_of_month_first
#   ))
# }
# 
# 
# test_result <- counting_error_rate(test_first_md, test_second_md)
# cat(test_result$summary) # so 99.9996% of cases, the first number is the day
# percent_successful_guess_of_month_first <- test_result$percent_successful_guess_of_month_first
# 
# # Indicating this error rate in the df
# probable_date_string <- paste0("probable_ddmmyyyy_to_approx_", percent_successful_guess_of_month_first,"_percent_certainty")
# colnames(obs13)[colnames(obs13) == "probable_ddmmyyyy"] <- probable_date_string

# Creating easy column to identify certain dates over probable dates
obs13$is_ambiguous <- 
  ifelse(is.na(obs13$probable_ddmmyyyy),
         0,
         1)

# # Screen for months that should not be
# improper_months_regex <- paste0("^\\d{1,2}/(", paste0(13:99, collapse = "|"), ")/\\d{4}$")
# erroneous_months <- which(!is.na(str_extract(obs13$standardized_ddmmyyyy, improper_months_regex)))
# erroneous_months_df <- obs13[erroneous_months,]

# Section 8: Writing out data with unified columns to desired location
obs13$year <- NULL
obs13$month_alphabetic <- NULL
obs13$day_when_month_alphabetical <- NULL
obs13$yyyy_md_md <- NULL
obs13$month_when_yyyy_md_md <- NULL
obs13$day_when_yyyy_md_md <- NULL
obs13$likely_month_when_yyyy_md_md <- NULL
obs13$likely_day_when_yyyy_md_md <- NULL
obs13$md_md_yyyy <- NULL
obs13$month_when_md_md_yyyy <- NULL
obs13$day_when_md_md_yyyy <- NULL
obs13$likely_month_when_md_md_yyyy <- NULL
obs13$likely_day_when_md_md_yyyy <- NULL
obs13$certain_dd <- NULL
obs13$probable_dd <- NULL
obs13$certain_mm <- NULL
obs13$probable_mm <- NULL
obs13$certain_ddmmyyyy <- NULL
obs13$probable_ddmmyyyy <- NULL
obs13$just_month_when_alphabetic <- NULL
obs13$month_numeric_when_alphabetic <- NULL








################################14############################


# Section 2: Extracting the year
#2.1 first pass - grep any set of 4 numbers
obs14$year4num <- regmatches(obs14$observed_on_string, gregexpr("\\d{4}", obs14$observed_on_string))
obs14$year <- obs14$year4num
obs14$year <- sapply(obs14$year4num, function(x){ ifelse(length(x)==0,NA,x[1])}) # to NA the ones with no year
#length(which(is.na(obs14$year))) # about 95% of these cause theres no entry at all. 5% have an entry without a year

#2.2 second pass - correct those with multiple entries of 4 numbers
twos_indx <- which(sapply(obs14$year4num, length) == 2)

year_only <- lapply(obs14$year4num[twos_indx], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 1800 & x_num < 2026] # the plausible 4digit is used
})
obs14$year[twos_indx] <- year_only

#2.3 Any more still left? Then refine date inclusion criteria
twos_indx_still <- which(sapply(obs14$year, length) == 2)
obs14$observed_on_string[twos_indx_still] # displays the dates

# All years 2015 and 2016 with manual check, so use >2000. Change this if need be for different dfs
year_only <- lapply(obs14$year[twos_indx_still], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 2000]
})
obs14$year[twos_indx_still] <- year_only

# 2.4 Final check - any left? If so manually edit your df
#length(which(sapply(obs14$year, length) == 2)) # if empty, no further checks needed
obs14$year4num <- NULL




# Section 3 - Extracting the Month and dates when month listed as letters

# create a lookup table
month_lookup <- c(
  Jan=1, January=1, janv=1, janvier=1, jan=1, january=1,
  Feb=2, February=2, Fév=2, fév=2, fevrier=2, février=2, feb=2, february=2,
  Mar=3, March=3, mars=3, mar=3, march=3,
  Apr=4, April=4, avr=4, avril=4, apr=4, april=4,
  May=5, mai=5, may=5,
  Jun=6, June=6, juin=6, jun=6, june=6,
  Jul=7, July=7, juil=7, juillet=7, jul=7, july=7,
  Aug=8, August=8, aout=8, août=8, aug=8, august=8,
  Sep=9, Sept=9, September=9, sept=9, septembre=9, sep=9, september=9,
  Oct=10, October=10, oct=10, octobre=10, oct=10, october=10,
  Nov=11, November=11, nov=11, novembre=11, nov=11, november=11,
  Dec=12, December=12, déc=12, dec=12, décembre=12, dec=12, december=12
)

# create regex and use str_extract to take out the month
months_alphabetic_regex <- paste0("\\b(", paste(names(month_lookup), collapse="|"), ")\\b")
obs14$just_month_when_alphabetic <- str_extract(obs14$observed_on_string, months_alphabetic_regex)
#nb_alhpabetic <- length(which(!is.na(obs14$just_month_when_alphabetic))) # roughly 15% of cases

# assigning it to new column
obs14$month_numeric_when_alphabetic <- sapply(obs14$just_month_when_alphabetic, function(x){
  if (!is.na(x)){
    month_lookup[[x]]
  }
  else{
    NA
  }
})

# Finding the dates from these rows - should be the only " XX " in the string
obs14$day_when_month_alphabetical <- NA
rows_with_alphabetical_month <- !is.na(obs14$month_numeric_when_alphabetic)
positions_alpha_day <- regexec("\\s(\\d{2})\\s", obs14$observed_on_string[rows_with_alphabetical_month])
day_when_alpha <- regmatches(obs14$observed_on_string[rows_with_alphabetical_month], positions_alpha_day)
obs14$day_when_month_alphabetical[rows_with_alphabetical_month] <- 
  sapply(day_when_alpha, function(x) x[2])

#looking for errors
#length(which(sapply(obs14$day_when_month_alphabetical, length)> 1)) # any double dates? should be none
#length(which(obs14$day_when_month_alphabetical > 31))# any false dates? should be none


# Section 4: Extracting months and dates when listed as yyyy-m?d?-m?d?
# Issue: many formats (yyyy-mm-dd, yyyy-dd-mm, yyyy-d-m, yyyy-m-d)

# New column to contain only dates of this format
obs14$yyyy_md_md <- regmatches(obs14$observed_on_string, gregexpr("\\d{4}[/-]\\d{2}[/-]\\d{2}", obs14$observed_on_string))
obs14$yyyy_md_md[sapply(obs14$yyyy_md_md, length)==0] <- NA
#nb_yyyy_md_md <- length(which(!is.na(obs14$yyyy_md_md))) # the vast majority

# Extracting the numbers
obs14$yyyy_md_md_string <- sapply(obs14$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", obs14$yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
obs14$yyyy_md_md_vector <- regmatches(obs14$yyyy_md_md_string, positions_yyyy_md_md)
obs14$yyyy_md_md_string <- NULL

first_md <- as.integer(sapply(obs14$yyyy_md_md_vector, function(x) return(x[3])))
second_md <- as.integer(sapply(obs14$yyyy_md_md_vector, function(x) return(x[4])))
obs14$yyyy_md_md_vector <- NULL

# Creating function to extract date and month based on the values of first_md and second_md.
# It seems like the vast majority should be month first.
choosing_month_or_date <- function(first_number, second_number){
  
  day <- NA
  month <- NA
  likely_day <- NA
  likely_month <- NA
  
  if (is.na(first_number) && is.na(second_number)){
    # do nothing
  }
  else if (first_number > 12){
    day <- first_number
    month <- second_number
  }
  else if (second_number > 12){
    day <- second_number
    month <- first_number
  }
  else { # when ambiguous, assume first number is the month but mark as ambiguous
    likely_day <- second_number
    likely_month <- first_number
  }
  return(list(day=day,month=month,likely_day=likely_day,likely_month=likely_month))
}

# Applying the function to our data and assigning results to columns 
yyyy_md_md_results <- mapply(choosing_month_or_date,first_md, second_md, SIMPLIFY=FALSE)

obs14$month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$month)
obs14$day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$day)
obs14$likely_month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_month)
obs14$likely_day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_day)



# Section 5: Extracting months and dates when listed as md-md-yyyy - there are very few
# Issue:  many formats (mm-dd-yyyy, dd-mm-yyyy, d-m-yyyy, m-d-yyyy)

# New column to contain only dates of this format
obs14$md_md_yyyy <- regmatches(obs14$observed_on_string, gregexpr("\\d{1,2}[-/]\\d{1,2}[-/]\\d{4}", obs14$observed_on_string))
obs14$md_md_yyyy[sapply(obs14$md_md_yyyy, length)==0] <- NA
#nb_md_md_yyyy <- length(which(!is.na(obs14$md_md_yyyy))) # very few

# Extract the numbers
obs14$md_md_yyyy_string <- sapply(obs14$md_md_yyyy, function(x) if(length(x)==0) NA else x[1])
positions_md_md_yyyy <- regexec("(\\d{1,2})[-/](\\d{1,2})[-/](\\d{4})", obs14$md_md_yyyy_string)
obs14$md_md_yyyy_vector <- NA
obs14$md_md_yyyy_vector <- regmatches(obs14$md_md_yyyy_string, positions_md_md_yyyy)
obs14$md_md_yyyy_string <- NULL

first_md_2 <- as.integer(sapply(obs14$md_md_yyyy_vector, function(x) return(x[3])))
second_md_2 <- as.integer(sapply(obs14$md_md_yyyy_vector, function(x) return(x[4])))
obs14$md_md_yyyy_vector <- NULL

# Applying the function to our data and assigning results to columns 
md_md_yyyy_results <- mapply(choosing_month_or_date, first_md_2, second_md_2, SIMPLIFY=FALSE)

obs14$month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$month)
obs14$day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$day)
obs14$likely_month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_month)
obs14$likely_day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_day)

#confirm they were all assigned
#length(which(!is.na(obs14$day_when_md_md_yyyy))) 



# Section 5: Dealing with exceptions
#nb_no_date <- length(obs14$observed_on_string[is.na(obs14$observed_on_string)]) # when no date is listed
# very few (0.02%) have some non-NA entry other than the formats we've handled. deal with them here if one wishes to
#nb_entries_not_caught <- nrow(obs14) - sum(nb_alhpabetic, nb_md_md_yyyy, nb_yyyy_md_md, nb_no_date)



# Section 6: Assembling uniform date column 
# 6.1 assembling together different parts of the date
obs14$certain_dd <-
  ifelse(!is.na(obs14$day_when_month_alphabetical), obs14$day_when_month_alphabetical,
         ifelse(!is.na(obs14$day_when_yyyy_md_md), obs14$day_when_yyyy_md_md,
                ifelse(!is.na(obs14$day_when_md_md_yyyy), obs14$day_when_md_md_yyyy, NA)))

obs14$probable_dd <- 
  ifelse(!is.na(obs14$likely_day_when_yyyy_md_md), obs14$likely_day_when_yyyy_md_md,
         ifelse(!is.na(obs14$likely_day_when_md_md_yyyy), obs14$likely_day_when_md_md_yyyy, NA))

obs14$certain_mm <-
  ifelse(!is.na(obs14$month_numeric_when_alphabetic), obs14$month_numeric_when_alphabetic,
         ifelse(!is.na(obs14$month_when_yyyy_md_md), obs14$month_when_yyyy_md_md,
                ifelse(!is.na(obs14$month_when_md_md_yyyy), obs14$month_when_md_md_yyyy, NA)))

obs14$probable_mm <-
  ifelse(!is.na(obs14$likely_month_when_yyyy_md_md), obs14$likely_month_when_yyyy_md_md,
         ifelse(!is.na(obs14$likely_month_when_md_md_yyyy), obs14$likely_month_when_md_md_yyyy, NA))

# 6.2 assembling final columns
certain_dd <- ifelse(!is.na(obs14$certain_dd), obs14$certain_dd, "??")
certain_mm <- ifelse(!is.na(obs14$certain_mm), obs14$certain_mm, "??")
certain_yyyy <- ifelse(!is.na(obs14$year), obs14$year, "????")

obs14$certain_ddmmyyyy <-
  paste(
    ifelse(!is.na(obs14$certain_dd), obs14$certain_dd, "??"),
    ifelse(!is.na(obs14$certain_mm), obs14$certain_mm, "??"),
    ifelse(!is.na(obs14$year), obs14$year, "??"),
    sep="/")

obs14$probable_ddmmyyyy <-
  ifelse(
    (!is.na(obs14$probable_dd) & !is.na(obs14$probable_mm) & !is.na(obs14$year)),
    paste(obs14$probable_dd, obs14$probable_mm, obs14$year, sep="/"),
    NA)

obs14$standardized_ddmmyyyy <-
  ifelse((!is.na(obs14$probable_ddmmyyyy)), obs14$probable_ddmmyyyy, obs14$certain_ddmmyyyy)


# # Section 7: Assessing the quality of the "assumed" dates of ambiguous cases
# # when there is indeed a day > 12 (clear case), what percentage of them are in the first position? 
# # this serves as an estimate for the non-clear cases, to see how accurate our month-first assumption is
# nb_total_obs <- nrow(obs14)
# test_yyyy_md_md_string <- sapply(obs14$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
# positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", test_yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
# test_yyyy_md_md_vector <- regmatches(test_yyyy_md_md_string, positions_yyyy_md_md)
# test_first_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[3])))
# test_second_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[4])))
# 
# counting_error_rate <- function(first_number, second_number){
#   
#   both_na <- is.na(test_first_md) & is.na(second_number)
#   total_yyyy_md_md <- sum(!is.na(first_number))
#   first_md_greater_than_12 <- first_number > 12
#   second_md_greater_than_12 <- second_number > 12
#   ambiguous_cases <- !(both_na | first_md_greater_than_12 | second_md_greater_than_12)
#   
#   nb_first_md_gt12 <- sum(first_md_greater_than_12, na.rm = TRUE)
#   nb_second_md_gt12 <- sum(second_md_greater_than_12, na.rm = TRUE)
#   total_clear_cases <- nb_first_md_gt12 + nb_second_md_gt12
#   nb_ambiguous <- sum(ambiguous_cases, na.rm=TRUE)
#   nb_both_na <- sum(both_na)
#   
#   percent_of_clear_cases_with_day_first <- (nb_first_md_gt12/total_clear_cases)*100
#   percent_successful_guess_of_month_first <- round((100-percent_of_clear_cases_with_day_first), 8)
#   
#   return( list(
#     summary = paste(
#       c(
#         paste("total cases of yyyy-md-md: ", total_yyyy_md_md),
#         paste("ambiguous cases:", nb_ambiguous),
#         paste("clear cases:", total_clear_cases),
#         paste0("percent of all the observations caught under this search: ", (total_yyyy_md_md/nb_total_obs)*100,"%"),
#         paste0("percent of yyyy-md-md cases that are ambiguous: ", (nb_ambiguous/total_yyyy_md_md)*100,"%"),
#         paste0("percent of clear cases with day first: ", percent_of_clear_cases_with_day_first,"%")
#       ),
#       collapse="\n"
#     ),
#     percent_successful_guess_of_month_first = percent_successful_guess_of_month_first
#   ))
# }
# 
# 
# test_result <- counting_error_rate(test_first_md, test_second_md)
# cat(test_result$summary) # so 99.9996% of cases, the first number is the day
# percent_successful_guess_of_month_first <- test_result$percent_successful_guess_of_month_first
# 
# # Indicating this error rate in the df
# probable_date_string <- paste0("probable_ddmmyyyy_to_approx_", percent_successful_guess_of_month_first,"_percent_certainty")
# colnames(obs14)[colnames(obs14) == "probable_ddmmyyyy"] <- probable_date_string

# Creating easy column to identify certain dates over probable dates
obs14$is_ambiguous <- 
  ifelse(is.na(obs14$probable_ddmmyyyy),
         0,
         1)

# # Screen for months that should not be
# improper_months_regex <- paste0("^\\d{1,2}/(", paste0(13:99, collapse = "|"), ")/\\d{4}$")
# erroneous_months <- which(!is.na(str_extract(obs14$standardized_ddmmyyyy, improper_months_regex)))
# erroneous_months_df <- obs14[erroneous_months,]

# Section 8: Writing out data with unified columns to desired location
obs14$year <- NULL
obs14$month_alphabetic <- NULL
obs14$day_when_month_alphabetical <- NULL
obs14$yyyy_md_md <- NULL
obs14$month_when_yyyy_md_md <- NULL
obs14$day_when_yyyy_md_md <- NULL
obs14$likely_month_when_yyyy_md_md <- NULL
obs14$likely_day_when_yyyy_md_md <- NULL
obs14$md_md_yyyy <- NULL
obs14$month_when_md_md_yyyy <- NULL
obs14$day_when_md_md_yyyy <- NULL
obs14$likely_month_when_md_md_yyyy <- NULL
obs14$likely_day_when_md_md_yyyy <- NULL
obs14$certain_dd <- NULL
obs14$probable_dd <- NULL
obs14$certain_mm <- NULL
obs14$probable_mm <- NULL
obs14$certain_ddmmyyyy <- NULL
obs14$probable_ddmmyyyy <- NULL
obs14$just_month_when_alphabetic <- NULL
obs14$month_numeric_when_alphabetic <- NULL








#################################15###################################

# Section 2: Extracting the year
#2.1 first pass - grep any set of 4 numbers
obs15$year4num <- regmatches(obs15$observed_on_string, gregexpr("\\d{4}", obs15$observed_on_string))
obs15$year <- obs15$year4num
obs15$year <- sapply(obs15$year4num, function(x){ ifelse(length(x)==0,NA,x[1])}) # to NA the ones with no year
#length(which(is.na(obs15$year))) # about 95% of these cause theres no entry at all. 5% have an entry without a year

#2.2 second pass - correct those with multiple entries of 4 numbers
twos_indx <- which(sapply(obs15$year4num, length) == 2)

year_only <- lapply(obs15$year4num[twos_indx], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 1800 & x_num < 2026] # the plausible 4digit is used
})
obs15$year[twos_indx] <- year_only

#2.3 Any more still left? Then refine date inclusion criteria
twos_indx_still <- which(sapply(obs15$year, length) == 2)
obs15$observed_on_string[twos_indx_still] # displays the dates

# All years 2015 and 2016 with manual check, so use >2000. Change this if need be for different dfs
year_only <- lapply(obs15$year[twos_indx_still], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 2000]
})
obs15$year[twos_indx_still] <- year_only

# 2.4 Final check - any left? If so manually edit your df
#length(which(sapply(obs15$year, length) == 2)) # if empty, no further checks needed
obs15$year4num <- NULL




# Section 3 - Extracting the Month and dates when month listed as letters

# create a lookup table
month_lookup <- c(
  Jan=1, January=1, janv=1, janvier=1, jan=1, january=1,
  Feb=2, February=2, Fév=2, fév=2, fevrier=2, février=2, feb=2, february=2,
  Mar=3, March=3, mars=3, mar=3, march=3,
  Apr=4, April=4, avr=4, avril=4, apr=4, april=4,
  May=5, mai=5, may=5,
  Jun=6, June=6, juin=6, jun=6, june=6,
  Jul=7, July=7, juil=7, juillet=7, jul=7, july=7,
  Aug=8, August=8, aout=8, août=8, aug=8, august=8,
  Sep=9, Sept=9, September=9, sept=9, septembre=9, sep=9, september=9,
  Oct=10, October=10, oct=10, octobre=10, oct=10, october=10,
  Nov=11, November=11, nov=11, novembre=11, nov=11, november=11,
  Dec=12, December=12, déc=12, dec=12, décembre=12, dec=12, december=12
)

# create regex and use str_extract to take out the month
months_alphabetic_regex <- paste0("\\b(", paste(names(month_lookup), collapse="|"), ")\\b")
obs15$just_month_when_alphabetic <- str_extract(obs15$observed_on_string, months_alphabetic_regex)
#nb_alhpabetic <- length(which(!is.na(obs15$just_month_when_alphabetic))) # roughly 15% of cases

# assigning it to new column
obs15$month_numeric_when_alphabetic <- sapply(obs15$just_month_when_alphabetic, function(x){
  if (!is.na(x)){
    month_lookup[[x]]
  }
  else{
    NA
  }
})

# Finding the dates from these rows - should be the only " XX " in the string
obs15$day_when_month_alphabetical <- NA
rows_with_alphabetical_month <- !is.na(obs15$month_numeric_when_alphabetic)
positions_alpha_day <- regexec("\\s(\\d{2})\\s", obs15$observed_on_string[rows_with_alphabetical_month])
day_when_alpha <- regmatches(obs15$observed_on_string[rows_with_alphabetical_month], positions_alpha_day)
obs15$day_when_month_alphabetical[rows_with_alphabetical_month] <- 
  sapply(day_when_alpha, function(x) x[2])

#looking for errors
#length(which(sapply(obs15$day_when_month_alphabetical, length)> 1)) # any double dates? should be none
#length(which(obs15$day_when_month_alphabetical > 31))# any false dates? should be none


# Section 4: Extracting months and dates when listed as yyyy-m?d?-m?d?
# Issue: many formats (yyyy-mm-dd, yyyy-dd-mm, yyyy-d-m, yyyy-m-d)

# New column to contain only dates of this format
obs15$yyyy_md_md <- regmatches(obs15$observed_on_string, gregexpr("\\d{4}[/-]\\d{2}[/-]\\d{2}", obs15$observed_on_string))
obs15$yyyy_md_md[sapply(obs15$yyyy_md_md, length)==0] <- NA
#nb_yyyy_md_md <- length(which(!is.na(obs15$yyyy_md_md))) # the vast majority

# Extracting the numbers
obs15$yyyy_md_md_string <- sapply(obs15$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", obs15$yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
obs15$yyyy_md_md_vector <- regmatches(obs15$yyyy_md_md_string, positions_yyyy_md_md)
obs15$yyyy_md_md_string <- NULL

first_md <- as.integer(sapply(obs15$yyyy_md_md_vector, function(x) return(x[3])))
second_md <- as.integer(sapply(obs15$yyyy_md_md_vector, function(x) return(x[4])))
obs15$yyyy_md_md_vector <- NULL

# Creating function to extract date and month based on the values of first_md and second_md.
# It seems like the vast majority should be month first.
choosing_month_or_date <- function(first_number, second_number){
  
  day <- NA
  month <- NA
  likely_day <- NA
  likely_month <- NA
  
  if (is.na(first_number) && is.na(second_number)){
    # do nothing
  }
  else if (first_number > 12){
    day <- first_number
    month <- second_number
  }
  else if (second_number > 12){
    day <- second_number
    month <- first_number
  }
  else { # when ambiguous, assume first number is the month but mark as ambiguous
    likely_day <- second_number
    likely_month <- first_number
  }
  return(list(day=day,month=month,likely_day=likely_day,likely_month=likely_month))
}

# Applying the function to our data and assigning results to columns 
yyyy_md_md_results <- mapply(choosing_month_or_date,first_md, second_md, SIMPLIFY=FALSE)

obs15$month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$month)
obs15$day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$day)
obs15$likely_month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_month)
obs15$likely_day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_day)



# Section 5: Extracting months and dates when listed as md-md-yyyy - there are very few
# Issue:  many formats (mm-dd-yyyy, dd-mm-yyyy, d-m-yyyy, m-d-yyyy)

# New column to contain only dates of this format
obs15$md_md_yyyy <- regmatches(obs15$observed_on_string, gregexpr("\\d{1,2}[-/]\\d{1,2}[-/]\\d{4}", obs15$observed_on_string))
obs15$md_md_yyyy[sapply(obs15$md_md_yyyy, length)==0] <- NA
#nb_md_md_yyyy <- length(which(!is.na(obs15$md_md_yyyy))) # very few

# Extract the numbers
obs15$md_md_yyyy_string <- sapply(obs15$md_md_yyyy, function(x) if(length(x)==0) NA else x[1])
positions_md_md_yyyy <- regexec("(\\d{1,2})[-/](\\d{1,2})[-/](\\d{4})", obs15$md_md_yyyy_string)
obs15$md_md_yyyy_vector <- NA
obs15$md_md_yyyy_vector <- regmatches(obs15$md_md_yyyy_string, positions_md_md_yyyy)
obs15$md_md_yyyy_string <- NULL

first_md_2 <- as.integer(sapply(obs15$md_md_yyyy_vector, function(x) return(x[3])))
second_md_2 <- as.integer(sapply(obs15$md_md_yyyy_vector, function(x) return(x[4])))
obs15$md_md_yyyy_vector <- NULL

# Applying the function to our data and assigning results to columns 
md_md_yyyy_results <- mapply(choosing_month_or_date, first_md_2, second_md_2, SIMPLIFY=FALSE)

obs15$month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$month)
obs15$day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$day)
obs15$likely_month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_month)
obs15$likely_day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_day)

#confirm they were all assigned
#length(which(!is.na(obs15$day_when_md_md_yyyy))) 



# Section 5: Dealing with exceptions
#nb_no_date <- length(obs15$observed_on_string[is.na(obs15$observed_on_string)]) # when no date is listed
# very few (0.02%) have some non-NA entry other than the formats we've handled. deal with them here if one wishes to
#nb_entries_not_caught <- nrow(obs15) - sum(nb_alhpabetic, nb_md_md_yyyy, nb_yyyy_md_md, nb_no_date)



# Section 6: Assembling uniform date column 
# 6.1 assembling together different parts of the date
obs15$certain_dd <-
  ifelse(!is.na(obs15$day_when_month_alphabetical), obs15$day_when_month_alphabetical,
         ifelse(!is.na(obs15$day_when_yyyy_md_md), obs15$day_when_yyyy_md_md,
                ifelse(!is.na(obs15$day_when_md_md_yyyy), obs15$day_when_md_md_yyyy, NA)))

obs15$probable_dd <- 
  ifelse(!is.na(obs15$likely_day_when_yyyy_md_md), obs15$likely_day_when_yyyy_md_md,
         ifelse(!is.na(obs15$likely_day_when_md_md_yyyy), obs15$likely_day_when_md_md_yyyy, NA))

obs15$certain_mm <-
  ifelse(!is.na(obs15$month_numeric_when_alphabetic), obs15$month_numeric_when_alphabetic,
         ifelse(!is.na(obs15$month_when_yyyy_md_md), obs15$month_when_yyyy_md_md,
                ifelse(!is.na(obs15$month_when_md_md_yyyy), obs15$month_when_md_md_yyyy, NA)))

obs15$probable_mm <-
  ifelse(!is.na(obs15$likely_month_when_yyyy_md_md), obs15$likely_month_when_yyyy_md_md,
         ifelse(!is.na(obs15$likely_month_when_md_md_yyyy), obs15$likely_month_when_md_md_yyyy, NA))

# 6.2 assembling final columns
certain_dd <- ifelse(!is.na(obs15$certain_dd), obs15$certain_dd, "??")
certain_mm <- ifelse(!is.na(obs15$certain_mm), obs15$certain_mm, "??")
certain_yyyy <- ifelse(!is.na(obs15$year), obs15$year, "????")

obs15$certain_ddmmyyyy <-
  paste(
    ifelse(!is.na(obs15$certain_dd), obs15$certain_dd, "??"),
    ifelse(!is.na(obs15$certain_mm), obs15$certain_mm, "??"),
    ifelse(!is.na(obs15$year), obs15$year, "??"),
    sep="/")

obs15$probable_ddmmyyyy <-
  ifelse(
    (!is.na(obs15$probable_dd) & !is.na(obs15$probable_mm) & !is.na(obs15$year)),
    paste(obs15$probable_dd, obs15$probable_mm, obs15$year, sep="/"),
    NA)

obs15$standardized_ddmmyyyy <-
  ifelse((!is.na(obs15$probable_ddmmyyyy)), obs15$probable_ddmmyyyy, obs15$certain_ddmmyyyy)


# # Section 7: Assessing the quality of the "assumed" dates of ambiguous cases
# # when there is indeed a day > 12 (clear case), what percentage of them are in the first position? 
# # this serves as an estimate for the non-clear cases, to see how accurate our month-first assumption is
# nb_total_obs <- nrow(obs15)
# test_yyyy_md_md_string <- sapply(obs15$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
# positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", test_yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
# test_yyyy_md_md_vector <- regmatches(test_yyyy_md_md_string, positions_yyyy_md_md)
# test_first_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[3])))
# test_second_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[4])))
# 
# counting_error_rate <- function(first_number, second_number){
#   
#   both_na <- is.na(test_first_md) & is.na(second_number)
#   total_yyyy_md_md <- sum(!is.na(first_number))
#   first_md_greater_than_12 <- first_number > 12
#   second_md_greater_than_12 <- second_number > 12
#   ambiguous_cases <- !(both_na | first_md_greater_than_12 | second_md_greater_than_12)
#   
#   nb_first_md_gt12 <- sum(first_md_greater_than_12, na.rm = TRUE)
#   nb_second_md_gt12 <- sum(second_md_greater_than_12, na.rm = TRUE)
#   total_clear_cases <- nb_first_md_gt12 + nb_second_md_gt12
#   nb_ambiguous <- sum(ambiguous_cases, na.rm=TRUE)
#   nb_both_na <- sum(both_na)
#   
#   percent_of_clear_cases_with_day_first <- (nb_first_md_gt12/total_clear_cases)*100
#   percent_successful_guess_of_month_first <- round((100-percent_of_clear_cases_with_day_first), 8)
#   
#   return( list(
#     summary = paste(
#       c(
#         paste("total cases of yyyy-md-md: ", total_yyyy_md_md),
#         paste("ambiguous cases:", nb_ambiguous),
#         paste("clear cases:", total_clear_cases),
#         paste0("percent of all the observations caught under this search: ", (total_yyyy_md_md/nb_total_obs)*100,"%"),
#         paste0("percent of yyyy-md-md cases that are ambiguous: ", (nb_ambiguous/total_yyyy_md_md)*100,"%"),
#         paste0("percent of clear cases with day first: ", percent_of_clear_cases_with_day_first,"%")
#       ),
#       collapse="\n"
#     ),
#     percent_successful_guess_of_month_first = percent_successful_guess_of_month_first
#   ))
# }
# 
# 
# test_result <- counting_error_rate(test_first_md, test_second_md)
# cat(test_result$summary) # so 99.9996% of cases, the first number is the day
# percent_successful_guess_of_month_first <- test_result$percent_successful_guess_of_month_first
# 
# # Indicating this error rate in the df
# probable_date_string <- paste0("probable_ddmmyyyy_to_approx_", percent_successful_guess_of_month_first,"_percent_certainty")
# colnames(obs15)[colnames(obs15) == "probable_ddmmyyyy"] <- probable_date_string

# Creating easy column to identify certain dates over probable dates
obs15$is_ambiguous <- 
  ifelse(is.na(obs15$probable_ddmmyyyy),
         0,
         1)

# # Screen for months that should not be
# improper_months_regex <- paste0("^\\d{1,2}/(", paste0(13:99, collapse = "|"), ")/\\d{4}$")
# erroneous_months <- which(!is.na(str_extract(obs15$standardized_ddmmyyyy, improper_months_regex)))
# erroneous_months_df <- obs15[erroneous_months,]

# Section 8: Writing out data with unified columns to desired location
obs15$year <- NULL
obs15$month_alphabetic <- NULL
obs15$day_when_month_alphabetical <- NULL
obs15$yyyy_md_md <- NULL
obs15$month_when_yyyy_md_md <- NULL
obs15$day_when_yyyy_md_md <- NULL
obs15$likely_month_when_yyyy_md_md <- NULL
obs15$likely_day_when_yyyy_md_md <- NULL
obs15$md_md_yyyy <- NULL
obs15$month_when_md_md_yyyy <- NULL
obs15$day_when_md_md_yyyy <- NULL
obs15$likely_month_when_md_md_yyyy <- NULL
obs15$likely_day_when_md_md_yyyy <- NULL
obs15$certain_dd <- NULL
obs15$probable_dd <- NULL
obs15$certain_mm <- NULL
obs15$probable_mm <- NULL
obs15$certain_ddmmyyyy <- NULL
obs15$probable_ddmmyyyy <- NULL
obs15$just_month_when_alphabetic <- NULL
obs15$month_numeric_when_alphabetic <- NULL









#################################16############################

# Section 2: Extracting the year
#2.1 first pass - grep any set of 4 numbers
obs16$year4num <- regmatches(obs16$observed_on_string, gregexpr("\\d{4}", obs16$observed_on_string))
obs16$year <- obs16$year4num
obs16$year <- sapply(obs16$year4num, function(x){ ifelse(length(x)==0,NA,x[1])}) # to NA the ones with no year
#length(which(is.na(obs16$year))) # about 95% of these cause theres no entry at all. 5% have an entry without a year

#2.2 second pass - correct those with multiple entries of 4 numbers
twos_indx <- which(sapply(obs16$year4num, length) == 2)

year_only <- lapply(obs16$year4num[twos_indx], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 1800 & x_num < 2026] # the plausible 4digit is used
})
obs16$year[twos_indx] <- year_only

#2.3 Any more still left? Then refine date inclusion criteria
twos_indx_still <- which(sapply(obs16$year, length) == 2)
obs16$observed_on_string[twos_indx_still] # displays the dates

# All years 2015 and 2016 with manual check, so use >2000. Change this if need be for different dfs
year_only <- lapply(obs16$year[twos_indx_still], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 2000]
})
obs16$year[twos_indx_still] <- year_only

# 2.4 Final check - any left? If so manually edit your df
#length(which(sapply(obs16$year, length) == 2)) # if empty, no further checks needed
obs16$year4num <- NULL




# Section 3 - Extracting the Month and dates when month listed as letters

# create a lookup table
month_lookup <- c(
  Jan=1, January=1, janv=1, janvier=1, jan=1, january=1,
  Feb=2, February=2, Fév=2, fév=2, fevrier=2, février=2, feb=2, february=2,
  Mar=3, March=3, mars=3, mar=3, march=3,
  Apr=4, April=4, avr=4, avril=4, apr=4, april=4,
  May=5, mai=5, may=5,
  Jun=6, June=6, juin=6, jun=6, june=6,
  Jul=7, July=7, juil=7, juillet=7, jul=7, july=7,
  Aug=8, August=8, aout=8, août=8, aug=8, august=8,
  Sep=9, Sept=9, September=9, sept=9, septembre=9, sep=9, september=9,
  Oct=10, October=10, oct=10, octobre=10, oct=10, october=10,
  Nov=11, November=11, nov=11, novembre=11, nov=11, november=11,
  Dec=12, December=12, déc=12, dec=12, décembre=12, dec=12, december=12
)

# create regex and use str_extract to take out the month
months_alphabetic_regex <- paste0("\\b(", paste(names(month_lookup), collapse="|"), ")\\b")
obs16$just_month_when_alphabetic <- str_extract(obs16$observed_on_string, months_alphabetic_regex)
#nb_alhpabetic <- length(which(!is.na(obs16$just_month_when_alphabetic))) # roughly 15% of cases

# assigning it to new column
obs16$month_numeric_when_alphabetic <- sapply(obs16$just_month_when_alphabetic, function(x){
  if (!is.na(x)){
    month_lookup[[x]]
  }
  else{
    NA
  }
})

# Finding the dates from these rows - should be the only " XX " in the string
obs16$day_when_month_alphabetical <- NA
rows_with_alphabetical_month <- !is.na(obs16$month_numeric_when_alphabetic)
positions_alpha_day <- regexec("\\s(\\d{2})\\s", obs16$observed_on_string[rows_with_alphabetical_month])
day_when_alpha <- regmatches(obs16$observed_on_string[rows_with_alphabetical_month], positions_alpha_day)
obs16$day_when_month_alphabetical[rows_with_alphabetical_month] <- 
  sapply(day_when_alpha, function(x) x[2])

#looking for errors
#length(which(sapply(obs16$day_when_month_alphabetical, length)> 1)) # any double dates? should be none
#length(which(obs16$day_when_month_alphabetical > 31))# any false dates? should be none


# Section 4: Extracting months and dates when listed as yyyy-m?d?-m?d?
# Issue: many formats (yyyy-mm-dd, yyyy-dd-mm, yyyy-d-m, yyyy-m-d)

# New column to contain only dates of this format
obs16$yyyy_md_md <- regmatches(obs16$observed_on_string, gregexpr("\\d{4}[/-]\\d{2}[/-]\\d{2}", obs16$observed_on_string))
obs16$yyyy_md_md[sapply(obs16$yyyy_md_md, length)==0] <- NA
#nb_yyyy_md_md <- length(which(!is.na(obs16$yyyy_md_md))) # the vast majority

# Extracting the numbers
obs16$yyyy_md_md_string <- sapply(obs16$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", obs16$yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
obs16$yyyy_md_md_vector <- regmatches(obs16$yyyy_md_md_string, positions_yyyy_md_md)
obs16$yyyy_md_md_string <- NULL

first_md <- as.integer(sapply(obs16$yyyy_md_md_vector, function(x) return(x[3])))
second_md <- as.integer(sapply(obs16$yyyy_md_md_vector, function(x) return(x[4])))
obs16$yyyy_md_md_vector <- NULL

# Creating function to extract date and month based on the values of first_md and second_md.
# It seems like the vast majority should be month first.
choosing_month_or_date <- function(first_number, second_number){
  
  day <- NA
  month <- NA
  likely_day <- NA
  likely_month <- NA
  
  if (is.na(first_number) && is.na(second_number)){
    # do nothing
  }
  else if (first_number > 12){
    day <- first_number
    month <- second_number
  }
  else if (second_number > 12){
    day <- second_number
    month <- first_number
  }
  else { # when ambiguous, assume first number is the month but mark as ambiguous
    likely_day <- second_number
    likely_month <- first_number
  }
  return(list(day=day,month=month,likely_day=likely_day,likely_month=likely_month))
}

# Applying the function to our data and assigning results to columns 
yyyy_md_md_results <- mapply(choosing_month_or_date,first_md, second_md, SIMPLIFY=FALSE)

obs16$month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$month)
obs16$day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$day)
obs16$likely_month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_month)
obs16$likely_day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_day)



# Section 5: Extracting months and dates when listed as md-md-yyyy - there are very few
# Issue:  many formats (mm-dd-yyyy, dd-mm-yyyy, d-m-yyyy, m-d-yyyy)

# New column to contain only dates of this format
obs16$md_md_yyyy <- regmatches(obs16$observed_on_string, gregexpr("\\d{1,2}[-/]\\d{1,2}[-/]\\d{4}", obs16$observed_on_string))
obs16$md_md_yyyy[sapply(obs16$md_md_yyyy, length)==0] <- NA
#nb_md_md_yyyy <- length(which(!is.na(obs16$md_md_yyyy))) # very few

# Extract the numbers
obs16$md_md_yyyy_string <- sapply(obs16$md_md_yyyy, function(x) if(length(x)==0) NA else x[1])
positions_md_md_yyyy <- regexec("(\\d{1,2})[-/](\\d{1,2})[-/](\\d{4})", obs16$md_md_yyyy_string)
obs16$md_md_yyyy_vector <- NA
obs16$md_md_yyyy_vector <- regmatches(obs16$md_md_yyyy_string, positions_md_md_yyyy)
obs16$md_md_yyyy_string <- NULL

first_md_2 <- as.integer(sapply(obs16$md_md_yyyy_vector, function(x) return(x[3])))
second_md_2 <- as.integer(sapply(obs16$md_md_yyyy_vector, function(x) return(x[4])))
obs16$md_md_yyyy_vector <- NULL

# Applying the function to our data and assigning results to columns 
md_md_yyyy_results <- mapply(choosing_month_or_date, first_md_2, second_md_2, SIMPLIFY=FALSE)

obs16$month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$month)
obs16$day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$day)
obs16$likely_month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_month)
obs16$likely_day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_day)

#confirm they were all assigned
#length(which(!is.na(obs16$day_when_md_md_yyyy))) 



# Section 5: Dealing with exceptions
#nb_no_date <- length(obs16$observed_on_string[is.na(obs16$observed_on_string)]) # when no date is listed
# very few (0.02%) have some non-NA entry other than the formats we've handled. deal with them here if one wishes to
#nb_entries_not_caught <- nrow(obs16) - sum(nb_alhpabetic, nb_md_md_yyyy, nb_yyyy_md_md, nb_no_date)



# Section 6: Assembling uniform date column 
# 6.1 assembling together different parts of the date
obs16$certain_dd <-
  ifelse(!is.na(obs16$day_when_month_alphabetical), obs16$day_when_month_alphabetical,
         ifelse(!is.na(obs16$day_when_yyyy_md_md), obs16$day_when_yyyy_md_md,
                ifelse(!is.na(obs16$day_when_md_md_yyyy), obs16$day_when_md_md_yyyy, NA)))

obs16$probable_dd <- 
  ifelse(!is.na(obs16$likely_day_when_yyyy_md_md), obs16$likely_day_when_yyyy_md_md,
         ifelse(!is.na(obs16$likely_day_when_md_md_yyyy), obs16$likely_day_when_md_md_yyyy, NA))

obs16$certain_mm <-
  ifelse(!is.na(obs16$month_numeric_when_alphabetic), obs16$month_numeric_when_alphabetic,
         ifelse(!is.na(obs16$month_when_yyyy_md_md), obs16$month_when_yyyy_md_md,
                ifelse(!is.na(obs16$month_when_md_md_yyyy), obs16$month_when_md_md_yyyy, NA)))

obs16$probable_mm <-
  ifelse(!is.na(obs16$likely_month_when_yyyy_md_md), obs16$likely_month_when_yyyy_md_md,
         ifelse(!is.na(obs16$likely_month_when_md_md_yyyy), obs16$likely_month_when_md_md_yyyy, NA))

# 6.2 assembling final columns
certain_dd <- ifelse(!is.na(obs16$certain_dd), obs16$certain_dd, "??")
certain_mm <- ifelse(!is.na(obs16$certain_mm), obs16$certain_mm, "??")
certain_yyyy <- ifelse(!is.na(obs16$year), obs16$year, "????")

obs16$certain_ddmmyyyy <-
  paste(
    ifelse(!is.na(obs16$certain_dd), obs16$certain_dd, "??"),
    ifelse(!is.na(obs16$certain_mm), obs16$certain_mm, "??"),
    ifelse(!is.na(obs16$year), obs16$year, "??"),
    sep="/")

obs16$probable_ddmmyyyy <-
  ifelse(
    (!is.na(obs16$probable_dd) & !is.na(obs16$probable_mm) & !is.na(obs16$year)),
    paste(obs16$probable_dd, obs16$probable_mm, obs16$year, sep="/"),
    NA)

obs16$standardized_ddmmyyyy <-
  ifelse((!is.na(obs16$probable_ddmmyyyy)), obs16$probable_ddmmyyyy, obs16$certain_ddmmyyyy)


# # Section 7: Assessing the quality of the "assumed" dates of ambiguous cases
# # when there is indeed a day > 12 (clear case), what percentage of them are in the first position? 
# # this serves as an estimate for the non-clear cases, to see how accurate our month-first assumption is
# nb_total_obs <- nrow(obs16)
# test_yyyy_md_md_string <- sapply(obs16$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
# positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", test_yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
# test_yyyy_md_md_vector <- regmatches(test_yyyy_md_md_string, positions_yyyy_md_md)
# test_first_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[3])))
# test_second_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[4])))
# 
# counting_error_rate <- function(first_number, second_number){
#   
#   both_na <- is.na(test_first_md) & is.na(second_number)
#   total_yyyy_md_md <- sum(!is.na(first_number))
#   first_md_greater_than_12 <- first_number > 12
#   second_md_greater_than_12 <- second_number > 12
#   ambiguous_cases <- !(both_na | first_md_greater_than_12 | second_md_greater_than_12)
#   
#   nb_first_md_gt12 <- sum(first_md_greater_than_12, na.rm = TRUE)
#   nb_second_md_gt12 <- sum(second_md_greater_than_12, na.rm = TRUE)
#   total_clear_cases <- nb_first_md_gt12 + nb_second_md_gt12
#   nb_ambiguous <- sum(ambiguous_cases, na.rm=TRUE)
#   nb_both_na <- sum(both_na)
#   
#   percent_of_clear_cases_with_day_first <- (nb_first_md_gt12/total_clear_cases)*100
#   percent_successful_guess_of_month_first <- round((100-percent_of_clear_cases_with_day_first), 8)
#   
#   return( list(
#     summary = paste(
#       c(
#         paste("total cases of yyyy-md-md: ", total_yyyy_md_md),
#         paste("ambiguous cases:", nb_ambiguous),
#         paste("clear cases:", total_clear_cases),
#         paste0("percent of all the observations caught under this search: ", (total_yyyy_md_md/nb_total_obs)*100,"%"),
#         paste0("percent of yyyy-md-md cases that are ambiguous: ", (nb_ambiguous/total_yyyy_md_md)*100,"%"),
#         paste0("percent of clear cases with day first: ", percent_of_clear_cases_with_day_first,"%")
#       ),
#       collapse="\n"
#     ),
#     percent_successful_guess_of_month_first = percent_successful_guess_of_month_first
#   ))
# }
# 
# 
# test_result <- counting_error_rate(test_first_md, test_second_md)
# cat(test_result$summary) # so 99.9996% of cases, the first number is the day
# percent_successful_guess_of_month_first <- test_result$percent_successful_guess_of_month_first
# 
# # Indicating this error rate in the df
# probable_date_string <- paste0("probable_ddmmyyyy_to_approx_", percent_successful_guess_of_month_first,"_percent_certainty")
# colnames(obs16)[colnames(obs16) == "probable_ddmmyyyy"] <- probable_date_string

# Creating easy column to identify certain dates over probable dates
obs16$is_ambiguous <- 
  ifelse(is.na(obs16$probable_ddmmyyyy),
         0,
         1)

# # Screen for months that should not be
# improper_months_regex <- paste0("^\\d{1,2}/(", paste0(13:99, collapse = "|"), ")/\\d{4}$")
# erroneous_months <- which(!is.na(str_extract(obs16$standardized_ddmmyyyy, improper_months_regex)))
# erroneous_months_df <- obs16[erroneous_months,]

# Section 8: Writing out data with unified columns to desired location
obs16$year <- NULL
obs16$month_alphabetic <- NULL
obs16$day_when_month_alphabetical <- NULL
obs16$yyyy_md_md <- NULL
obs16$month_when_yyyy_md_md <- NULL
obs16$day_when_yyyy_md_md <- NULL
obs16$likely_month_when_yyyy_md_md <- NULL
obs16$likely_day_when_yyyy_md_md <- NULL
obs16$md_md_yyyy <- NULL
obs16$month_when_md_md_yyyy <- NULL
obs16$day_when_md_md_yyyy <- NULL
obs16$likely_month_when_md_md_yyyy <- NULL
obs16$likely_day_when_md_md_yyyy <- NULL
obs16$certain_dd <- NULL
obs16$probable_dd <- NULL
obs16$certain_mm <- NULL
obs16$probable_mm <- NULL
obs16$certain_ddmmyyyy <- NULL
obs16$probable_ddmmyyyy <- NULL
obs16$just_month_when_alphabetic <- NULL
obs16$month_numeric_when_alphabetic <- NULL







###################################################17 #################################


# Section 2: Extracting the year
#2.1 first pass - grep any set of 4 numbers
obs17$year4num <- regmatches(obs17$observed_on_string, gregexpr("\\d{4}", obs17$observed_on_string))
obs17$year <- obs17$year4num
obs17$year <- sapply(obs17$year4num, function(x){ ifelse(length(x)==0,NA,x[1])}) # to NA the ones with no year
#length(which(is.na(obs17$year))) # about 95% of these cause theres no entry at all. 5% have an entry without a year

#2.2 second pass - correct those with multiple entries of 4 numbers
twos_indx <- which(sapply(obs17$year4num, length) == 2)

year_only <- lapply(obs17$year4num[twos_indx], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 1800 & x_num < 2026] # the plausible 4digit is used
})
obs17$year[twos_indx] <- year_only

#2.3 Any more still left? Then refine date inclusion criteria
twos_indx_still <- which(sapply(obs17$year, length) == 2)
obs17$observed_on_string[twos_indx_still] # displays the dates

# All years 2015 and 2016 with manual check, so use >2000. Change this if need be for different dfs
year_only <- lapply(obs17$year[twos_indx_still], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 2000]
})
obs17$year[twos_indx_still] <- year_only

# 2.4 Final check - any left? If so manually edit your df
#length(which(sapply(obs17$year, length) == 2)) # if empty, no further checks needed
obs17$year4num <- NULL




# Section 3 - Extracting the Month and dates when month listed as letters

# create a lookup table
month_lookup <- c(
  Jan=1, January=1, janv=1, janvier=1, jan=1, january=1,
  Feb=2, February=2, Fév=2, fév=2, fevrier=2, février=2, feb=2, february=2,
  Mar=3, March=3, mars=3, mar=3, march=3,
  Apr=4, April=4, avr=4, avril=4, apr=4, april=4,
  May=5, mai=5, may=5,
  Jun=6, June=6, juin=6, jun=6, june=6,
  Jul=7, July=7, juil=7, juillet=7, jul=7, july=7,
  Aug=8, August=8, aout=8, août=8, aug=8, august=8,
  Sep=9, Sept=9, September=9, sept=9, septembre=9, sep=9, september=9,
  Oct=10, October=10, oct=10, octobre=10, oct=10, october=10,
  Nov=11, November=11, nov=11, novembre=11, nov=11, november=11,
  Dec=12, December=12, déc=12, dec=12, décembre=12, dec=12, december=12
)

# create regex and use str_extract to take out the month
months_alphabetic_regex <- paste0("\\b(", paste(names(month_lookup), collapse="|"), ")\\b")
obs17$just_month_when_alphabetic <- str_extract(obs17$observed_on_string, months_alphabetic_regex)
#nb_alhpabetic <- length(which(!is.na(obs17$just_month_when_alphabetic))) # roughly 15% of cases

# assigning it to new column
obs17$month_numeric_when_alphabetic <- sapply(obs17$just_month_when_alphabetic, function(x){
  if (!is.na(x)){
    month_lookup[[x]]
  }
  else{
    NA
  }
})

# Finding the dates from these rows - should be the only " XX " in the string
obs17$day_when_month_alphabetical <- NA
rows_with_alphabetical_month <- !is.na(obs17$month_numeric_when_alphabetic)
positions_alpha_day <- regexec("\\s(\\d{2})\\s", obs17$observed_on_string[rows_with_alphabetical_month])
day_when_alpha <- regmatches(obs17$observed_on_string[rows_with_alphabetical_month], positions_alpha_day)
obs17$day_when_month_alphabetical[rows_with_alphabetical_month] <- 
  sapply(day_when_alpha, function(x) x[2])

#looking for errors
#length(which(sapply(obs17$day_when_month_alphabetical, length)> 1)) # any double dates? should be none
#length(which(obs17$day_when_month_alphabetical > 31))# any false dates? should be none


# Section 4: Extracting months and dates when listed as yyyy-m?d?-m?d?
# Issue: many formats (yyyy-mm-dd, yyyy-dd-mm, yyyy-d-m, yyyy-m-d)

# New column to contain only dates of this format
obs17$yyyy_md_md <- regmatches(obs17$observed_on_string, gregexpr("\\d{4}[/-]\\d{2}[/-]\\d{2}", obs17$observed_on_string))
obs17$yyyy_md_md[sapply(obs17$yyyy_md_md, length)==0] <- NA
#nb_yyyy_md_md <- length(which(!is.na(obs17$yyyy_md_md))) # the vast majority

# Extracting the numbers
obs17$yyyy_md_md_string <- sapply(obs17$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", obs17$yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
obs17$yyyy_md_md_vector <- regmatches(obs17$yyyy_md_md_string, positions_yyyy_md_md)
obs17$yyyy_md_md_string <- NULL

first_md <- as.integer(sapply(obs17$yyyy_md_md_vector, function(x) return(x[3])))
second_md <- as.integer(sapply(obs17$yyyy_md_md_vector, function(x) return(x[4])))
obs17$yyyy_md_md_vector <- NULL

# Creating function to extract date and month based on the values of first_md and second_md.
# It seems like the vast majority should be month first.
choosing_month_or_date <- function(first_number, second_number){
  
  day <- NA
  month <- NA
  likely_day <- NA
  likely_month <- NA
  
  if (is.na(first_number) && is.na(second_number)){
    # do nothing
  }
  else if (first_number > 12){
    day <- first_number
    month <- second_number
  }
  else if (second_number > 12){
    day <- second_number
    month <- first_number
  }
  else { # when ambiguous, assume first number is the month but mark as ambiguous
    likely_day <- second_number
    likely_month <- first_number
  }
  return(list(day=day,month=month,likely_day=likely_day,likely_month=likely_month))
}

# Applying the function to our data and assigning results to columns 
yyyy_md_md_results <- mapply(choosing_month_or_date,first_md, second_md, SIMPLIFY=FALSE)

obs17$month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$month)
obs17$day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$day)
obs17$likely_month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_month)
obs17$likely_day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_day)



# Section 5: Extracting months and dates when listed as md-md-yyyy - there are very few
# Issue:  many formats (mm-dd-yyyy, dd-mm-yyyy, d-m-yyyy, m-d-yyyy)

# New column to contain only dates of this format
obs17$md_md_yyyy <- regmatches(obs17$observed_on_string, gregexpr("\\d{1,2}[-/]\\d{1,2}[-/]\\d{4}", obs17$observed_on_string))
obs17$md_md_yyyy[sapply(obs17$md_md_yyyy, length)==0] <- NA
#nb_md_md_yyyy <- length(which(!is.na(obs17$md_md_yyyy))) # very few

# Extract the numbers
obs17$md_md_yyyy_string <- sapply(obs17$md_md_yyyy, function(x) if(length(x)==0) NA else x[1])
positions_md_md_yyyy <- regexec("(\\d{1,2})[-/](\\d{1,2})[-/](\\d{4})", obs17$md_md_yyyy_string)
obs17$md_md_yyyy_vector <- NA
obs17$md_md_yyyy_vector <- regmatches(obs17$md_md_yyyy_string, positions_md_md_yyyy)
obs17$md_md_yyyy_string <- NULL

first_md_2 <- as.integer(sapply(obs17$md_md_yyyy_vector, function(x) return(x[3])))
second_md_2 <- as.integer(sapply(obs17$md_md_yyyy_vector, function(x) return(x[4])))
obs17$md_md_yyyy_vector <- NULL

# Applying the function to our data and assigning results to columns 
md_md_yyyy_results <- mapply(choosing_month_or_date, first_md_2, second_md_2, SIMPLIFY=FALSE)

obs17$month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$month)
obs17$day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$day)
obs17$likely_month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_month)
obs17$likely_day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_day)

#confirm they were all assigned
#length(which(!is.na(obs17$day_when_md_md_yyyy))) 



# Section 5: Dealing with exceptions
#nb_no_date <- length(obs17$observed_on_string[is.na(obs17$observed_on_string)]) # when no date is listed
# very few (0.02%) have some non-NA entry other than the formats we've handled. deal with them here if one wishes to
#nb_entries_not_caught <- nrow(obs17) - sum(nb_alhpabetic, nb_md_md_yyyy, nb_yyyy_md_md, nb_no_date)



# Section 6: Assembling uniform date column 
# 6.1 assembling together different parts of the date
obs17$certain_dd <-
  ifelse(!is.na(obs17$day_when_month_alphabetical), obs17$day_when_month_alphabetical,
         ifelse(!is.na(obs17$day_when_yyyy_md_md), obs17$day_when_yyyy_md_md,
                ifelse(!is.na(obs17$day_when_md_md_yyyy), obs17$day_when_md_md_yyyy, NA)))

obs17$probable_dd <- 
  ifelse(!is.na(obs17$likely_day_when_yyyy_md_md), obs17$likely_day_when_yyyy_md_md,
         ifelse(!is.na(obs17$likely_day_when_md_md_yyyy), obs17$likely_day_when_md_md_yyyy, NA))

obs17$certain_mm <-
  ifelse(!is.na(obs17$month_numeric_when_alphabetic), obs17$month_numeric_when_alphabetic,
         ifelse(!is.na(obs17$month_when_yyyy_md_md), obs17$month_when_yyyy_md_md,
                ifelse(!is.na(obs17$month_when_md_md_yyyy), obs17$month_when_md_md_yyyy, NA)))

obs17$probable_mm <-
  ifelse(!is.na(obs17$likely_month_when_yyyy_md_md), obs17$likely_month_when_yyyy_md_md,
         ifelse(!is.na(obs17$likely_month_when_md_md_yyyy), obs17$likely_month_when_md_md_yyyy, NA))

# 6.2 assembling final columns
certain_dd <- ifelse(!is.na(obs17$certain_dd), obs17$certain_dd, "??")
certain_mm <- ifelse(!is.na(obs17$certain_mm), obs17$certain_mm, "??")
certain_yyyy <- ifelse(!is.na(obs17$year), obs17$year, "????")

obs17$certain_ddmmyyyy <-
  paste(
    ifelse(!is.na(obs17$certain_dd), obs17$certain_dd, "??"),
    ifelse(!is.na(obs17$certain_mm), obs17$certain_mm, "??"),
    ifelse(!is.na(obs17$year), obs17$year, "??"),
    sep="/")

obs17$probable_ddmmyyyy <-
  ifelse(
    (!is.na(obs17$probable_dd) & !is.na(obs17$probable_mm) & !is.na(obs17$year)),
    paste(obs17$probable_dd, obs17$probable_mm, obs17$year, sep="/"),
    NA)

obs17$standardized_ddmmyyyy <-
  ifelse((!is.na(obs17$probable_ddmmyyyy)), obs17$probable_ddmmyyyy, obs17$certain_ddmmyyyy)


# # Section 7: Assessing the quality of the "assumed" dates of ambiguous cases
# # when there is indeed a day > 12 (clear case), what percentage of them are in the first position? 
# # this serves as an estimate for the non-clear cases, to see how accurate our month-first assumption is
# nb_total_obs <- nrow(obs17)
# test_yyyy_md_md_string <- sapply(obs17$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
# positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", test_yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
# test_yyyy_md_md_vector <- regmatches(test_yyyy_md_md_string, positions_yyyy_md_md)
# test_first_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[3])))
# test_second_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[4])))
# 
# counting_error_rate <- function(first_number, second_number){
#   
#   both_na <- is.na(test_first_md) & is.na(second_number)
#   total_yyyy_md_md <- sum(!is.na(first_number))
#   first_md_greater_than_12 <- first_number > 12
#   second_md_greater_than_12 <- second_number > 12
#   ambiguous_cases <- !(both_na | first_md_greater_than_12 | second_md_greater_than_12)
#   
#   nb_first_md_gt12 <- sum(first_md_greater_than_12, na.rm = TRUE)
#   nb_second_md_gt12 <- sum(second_md_greater_than_12, na.rm = TRUE)
#   total_clear_cases <- nb_first_md_gt12 + nb_second_md_gt12
#   nb_ambiguous <- sum(ambiguous_cases, na.rm=TRUE)
#   nb_both_na <- sum(both_na)
#   
#   percent_of_clear_cases_with_day_first <- (nb_first_md_gt12/total_clear_cases)*100
#   percent_successful_guess_of_month_first <- round((100-percent_of_clear_cases_with_day_first), 8)
#   
#   return( list(
#     summary = paste(
#       c(
#         paste("total cases of yyyy-md-md: ", total_yyyy_md_md),
#         paste("ambiguous cases:", nb_ambiguous),
#         paste("clear cases:", total_clear_cases),
#         paste0("percent of all the observations caught under this search: ", (total_yyyy_md_md/nb_total_obs)*100,"%"),
#         paste0("percent of yyyy-md-md cases that are ambiguous: ", (nb_ambiguous/total_yyyy_md_md)*100,"%"),
#         paste0("percent of clear cases with day first: ", percent_of_clear_cases_with_day_first,"%")
#       ),
#       collapse="\n"
#     ),
#     percent_successful_guess_of_month_first = percent_successful_guess_of_month_first
#   ))
# }
# 
# 
# test_result <- counting_error_rate(test_first_md, test_second_md)
# cat(test_result$summary) # so 99.9996% of cases, the first number is the day
# percent_successful_guess_of_month_first <- test_result$percent_successful_guess_of_month_first
# 
# # Indicating this error rate in the df
# probable_date_string <- paste0("probable_ddmmyyyy_to_approx_", percent_successful_guess_of_month_first,"_percent_certainty")
# colnames(obs17)[colnames(obs17) == "probable_ddmmyyyy"] <- probable_date_string

# Creating easy column to identify certain dates over probable dates
obs17$is_ambiguous <- 
  ifelse(is.na(obs17$probable_ddmmyyyy),
         0,
         1)

# # Screen for months that should not be
# improper_months_regex <- paste0("^\\d{1,2}/(", paste0(13:99, collapse = "|"), ")/\\d{4}$")
# erroneous_months <- which(!is.na(str_extract(obs17$standardized_ddmmyyyy, improper_months_regex)))
# erroneous_months_df <- obs17[erroneous_months,]

# Section 8: Writing out data with unified columns to desired location
obs17$year <- NULL
obs17$month_alphabetic <- NULL
obs17$day_when_month_alphabetical <- NULL
obs17$yyyy_md_md <- NULL
obs17$month_when_yyyy_md_md <- NULL
obs17$day_when_yyyy_md_md <- NULL
obs17$likely_month_when_yyyy_md_md <- NULL
obs17$likely_day_when_yyyy_md_md <- NULL
obs17$md_md_yyyy <- NULL
obs17$month_when_md_md_yyyy <- NULL
obs17$day_when_md_md_yyyy <- NULL
obs17$likely_month_when_md_md_yyyy <- NULL
obs17$likely_day_when_md_md_yyyy <- NULL
obs17$certain_dd <- NULL
obs17$probable_dd <- NULL
obs17$certain_mm <- NULL
obs17$probable_mm <- NULL
obs17$certain_ddmmyyyy <- NULL
obs17$probable_ddmmyyyy <- NULL
obs17$just_month_when_alphabetic <- NULL
obs17$month_numeric_when_alphabetic <- NULL












############################### 18 #################################


# Section 2: Extracting the year
#2.1 first pass - grep any set of 4 numbers
obs18$year4num <- regmatches(obs18$observed_on_string, gregexpr("\\d{4}", obs18$observed_on_string))
obs18$year <- obs18$year4num
obs18$year <- sapply(obs18$year4num, function(x){ ifelse(length(x)==0,NA,x[1])}) # to NA the ones with no year
#length(which(is.na(obs18$year))) # about 95% of these cause theres no entry at all. 5% have an entry without a year

#2.2 second pass - correct those with multiple entries of 4 numbers
twos_indx <- which(sapply(obs18$year4num, length) == 2)

year_only <- lapply(obs18$year4num[twos_indx], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 1800 & x_num < 2026] # the plausible 4digit is used
})
obs18$year[twos_indx] <- year_only

#2.3 Any more still left? Then refine date inclusion criteria
twos_indx_still <- which(sapply(obs18$year, length) == 2)
obs18$observed_on_string[twos_indx_still] # displays the dates

# All years 2015 and 2016 with manual check, so use >2000. Change this if need be for different dfs
year_only <- lapply(obs18$year[twos_indx_still], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 2000]
})
obs18$year[twos_indx_still] <- year_only

# 2.4 Final check - any left? If so manually edit your df
#length(which(sapply(obs18$year, length) == 2)) # if empty, no further checks needed
obs18$year4num <- NULL




# Section 3 - Extracting the Month and dates when month listed as letters

# create a lookup table
month_lookup <- c(
  Jan=1, January=1, janv=1, janvier=1, jan=1, january=1,
  Feb=2, February=2, Fév=2, fév=2, fevrier=2, février=2, feb=2, february=2,
  Mar=3, March=3, mars=3, mar=3, march=3,
  Apr=4, April=4, avr=4, avril=4, apr=4, april=4,
  May=5, mai=5, may=5,
  Jun=6, June=6, juin=6, jun=6, june=6,
  Jul=7, July=7, juil=7, juillet=7, jul=7, july=7,
  Aug=8, August=8, aout=8, août=8, aug=8, august=8,
  Sep=9, Sept=9, September=9, sept=9, septembre=9, sep=9, september=9,
  Oct=10, October=10, oct=10, octobre=10, oct=10, october=10,
  Nov=11, November=11, nov=11, novembre=11, nov=11, november=11,
  Dec=12, December=12, déc=12, dec=12, décembre=12, dec=12, december=12
)

# create regex and use str_extract to take out the month
months_alphabetic_regex <- paste0("\\b(", paste(names(month_lookup), collapse="|"), ")\\b")
obs18$just_month_when_alphabetic <- str_extract(obs18$observed_on_string, months_alphabetic_regex)
#nb_alhpabetic <- length(which(!is.na(obs18$just_month_when_alphabetic))) # roughly 15% of cases

# assigning it to new column
obs18$month_numeric_when_alphabetic <- sapply(obs18$just_month_when_alphabetic, function(x){
  if (!is.na(x)){
    month_lookup[[x]]
  }
  else{
    NA
  }
})

# Finding the dates from these rows - should be the only " XX " in the string
obs18$day_when_month_alphabetical <- NA
rows_with_alphabetical_month <- !is.na(obs18$month_numeric_when_alphabetic)
positions_alpha_day <- regexec("\\s(\\d{2})\\s", obs18$observed_on_string[rows_with_alphabetical_month])
day_when_alpha <- regmatches(obs18$observed_on_string[rows_with_alphabetical_month], positions_alpha_day)
obs18$day_when_month_alphabetical[rows_with_alphabetical_month] <- 
  sapply(day_when_alpha, function(x) x[2])

#looking for errors
#length(which(sapply(obs18$day_when_month_alphabetical, length)> 1)) # any double dates? should be none
#length(which(obs18$day_when_month_alphabetical > 31))# any false dates? should be none


# Section 4: Extracting months and dates when listed as yyyy-m?d?-m?d?
# Issue: many formats (yyyy-mm-dd, yyyy-dd-mm, yyyy-d-m, yyyy-m-d)

# New column to contain only dates of this format
obs18$yyyy_md_md <- regmatches(obs18$observed_on_string, gregexpr("\\d{4}[/-]\\d{2}[/-]\\d{2}", obs18$observed_on_string))
obs18$yyyy_md_md[sapply(obs18$yyyy_md_md, length)==0] <- NA
#nb_yyyy_md_md <- length(which(!is.na(obs18$yyyy_md_md))) # the vast majority

# Extracting the numbers
obs18$yyyy_md_md_string <- sapply(obs18$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", obs18$yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
obs18$yyyy_md_md_vector <- regmatches(obs18$yyyy_md_md_string, positions_yyyy_md_md)
obs18$yyyy_md_md_string <- NULL

first_md <- as.integer(sapply(obs18$yyyy_md_md_vector, function(x) return(x[3])))
second_md <- as.integer(sapply(obs18$yyyy_md_md_vector, function(x) return(x[4])))
obs18$yyyy_md_md_vector <- NULL

# Creating function to extract date and month based on the values of first_md and second_md.
# It seems like the vast majority should be month first.
choosing_month_or_date <- function(first_number, second_number){
  
  day <- NA
  month <- NA
  likely_day <- NA
  likely_month <- NA
  
  if (is.na(first_number) && is.na(second_number)){
    # do nothing
  }
  else if (first_number > 12){
    day <- first_number
    month <- second_number
  }
  else if (second_number > 12){
    day <- second_number
    month <- first_number
  }
  else { # when ambiguous, assume first number is the month but mark as ambiguous
    likely_day <- second_number
    likely_month <- first_number
  }
  return(list(day=day,month=month,likely_day=likely_day,likely_month=likely_month))
}

# Applying the function to our data and assigning results to columns 
yyyy_md_md_results <- mapply(choosing_month_or_date,first_md, second_md, SIMPLIFY=FALSE)

obs18$month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$month)
obs18$day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$day)
obs18$likely_month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_month)
obs18$likely_day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_day)



# Section 5: Extracting months and dates when listed as md-md-yyyy - there are very few
# Issue:  many formats (mm-dd-yyyy, dd-mm-yyyy, d-m-yyyy, m-d-yyyy)

# New column to contain only dates of this format
obs18$md_md_yyyy <- regmatches(obs18$observed_on_string, gregexpr("\\d{1,2}[-/]\\d{1,2}[-/]\\d{4}", obs18$observed_on_string))
obs18$md_md_yyyy[sapply(obs18$md_md_yyyy, length)==0] <- NA
#nb_md_md_yyyy <- length(which(!is.na(obs18$md_md_yyyy))) # very few

# Extract the numbers
obs18$md_md_yyyy_string <- sapply(obs18$md_md_yyyy, function(x) if(length(x)==0) NA else x[1])
positions_md_md_yyyy <- regexec("(\\d{1,2})[-/](\\d{1,2})[-/](\\d{4})", obs18$md_md_yyyy_string)
obs18$md_md_yyyy_vector <- NA
obs18$md_md_yyyy_vector <- regmatches(obs18$md_md_yyyy_string, positions_md_md_yyyy)
obs18$md_md_yyyy_string <- NULL

first_md_2 <- as.integer(sapply(obs18$md_md_yyyy_vector, function(x) return(x[3])))
second_md_2 <- as.integer(sapply(obs18$md_md_yyyy_vector, function(x) return(x[4])))
obs18$md_md_yyyy_vector <- NULL

# Applying the function to our data and assigning results to columns 
md_md_yyyy_results <- mapply(choosing_month_or_date, first_md_2, second_md_2, SIMPLIFY=FALSE)

obs18$month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$month)
obs18$day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$day)
obs18$likely_month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_month)
obs18$likely_day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_day)

#confirm they were all assigned
#length(which(!is.na(obs18$day_when_md_md_yyyy))) 



# Section 5: Dealing with exceptions
#nb_no_date <- length(obs18$observed_on_string[is.na(obs18$observed_on_string)]) # when no date is listed
# very few (0.02%) have some non-NA entry other than the formats we've handled. deal with them here if one wishes to
#nb_entries_not_caught <- nrow(obs18) - sum(nb_alhpabetic, nb_md_md_yyyy, nb_yyyy_md_md, nb_no_date)



# Section 6: Assembling uniform date column 
# 6.1 assembling together different parts of the date
obs18$certain_dd <-
  ifelse(!is.na(obs18$day_when_month_alphabetical), obs18$day_when_month_alphabetical,
         ifelse(!is.na(obs18$day_when_yyyy_md_md), obs18$day_when_yyyy_md_md,
                ifelse(!is.na(obs18$day_when_md_md_yyyy), obs18$day_when_md_md_yyyy, NA)))

obs18$probable_dd <- 
  ifelse(!is.na(obs18$likely_day_when_yyyy_md_md), obs18$likely_day_when_yyyy_md_md,
         ifelse(!is.na(obs18$likely_day_when_md_md_yyyy), obs18$likely_day_when_md_md_yyyy, NA))

obs18$certain_mm <-
  ifelse(!is.na(obs18$month_numeric_when_alphabetic), obs18$month_numeric_when_alphabetic,
         ifelse(!is.na(obs18$month_when_yyyy_md_md), obs18$month_when_yyyy_md_md,
                ifelse(!is.na(obs18$month_when_md_md_yyyy), obs18$month_when_md_md_yyyy, NA)))

obs18$probable_mm <-
  ifelse(!is.na(obs18$likely_month_when_yyyy_md_md), obs18$likely_month_when_yyyy_md_md,
         ifelse(!is.na(obs18$likely_month_when_md_md_yyyy), obs18$likely_month_when_md_md_yyyy, NA))

# 6.2 assembling final columns
certain_dd <- ifelse(!is.na(obs18$certain_dd), obs18$certain_dd, "??")
certain_mm <- ifelse(!is.na(obs18$certain_mm), obs18$certain_mm, "??")
certain_yyyy <- ifelse(!is.na(obs18$year), obs18$year, "????")

obs18$certain_ddmmyyyy <-
  paste(
    ifelse(!is.na(obs18$certain_dd), obs18$certain_dd, "??"),
    ifelse(!is.na(obs18$certain_mm), obs18$certain_mm, "??"),
    ifelse(!is.na(obs18$year), obs18$year, "??"),
    sep="/")

obs18$probable_ddmmyyyy <-
  ifelse(
    (!is.na(obs18$probable_dd) & !is.na(obs18$probable_mm) & !is.na(obs18$year)),
    paste(obs18$probable_dd, obs18$probable_mm, obs18$year, sep="/"),
    NA)

obs18$standardized_ddmmyyyy <-
  ifelse((!is.na(obs18$probable_ddmmyyyy)), obs18$probable_ddmmyyyy, obs18$certain_ddmmyyyy)


# # Section 7: Assessing the quality of the "assumed" dates of ambiguous cases
# # when there is indeed a day > 12 (clear case), what percentage of them are in the first position? 
# # this serves as an estimate for the non-clear cases, to see how accurate our month-first assumption is
# nb_total_obs <- nrow(obs18)
# test_yyyy_md_md_string <- sapply(obs18$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
# positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", test_yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
# test_yyyy_md_md_vector <- regmatches(test_yyyy_md_md_string, positions_yyyy_md_md)
# test_first_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[3])))
# test_second_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[4])))
# 
# counting_error_rate <- function(first_number, second_number){
#   
#   both_na <- is.na(test_first_md) & is.na(second_number)
#   total_yyyy_md_md <- sum(!is.na(first_number))
#   first_md_greater_than_12 <- first_number > 12
#   second_md_greater_than_12 <- second_number > 12
#   ambiguous_cases <- !(both_na | first_md_greater_than_12 | second_md_greater_than_12)
#   
#   nb_first_md_gt12 <- sum(first_md_greater_than_12, na.rm = TRUE)
#   nb_second_md_gt12 <- sum(second_md_greater_than_12, na.rm = TRUE)
#   total_clear_cases <- nb_first_md_gt12 + nb_second_md_gt12
#   nb_ambiguous <- sum(ambiguous_cases, na.rm=TRUE)
#   nb_both_na <- sum(both_na)
#   
#   percent_of_clear_cases_with_day_first <- (nb_first_md_gt12/total_clear_cases)*100
#   percent_successful_guess_of_month_first <- round((100-percent_of_clear_cases_with_day_first), 8)
#   
#   return( list(
#     summary = paste(
#       c(
#         paste("total cases of yyyy-md-md: ", total_yyyy_md_md),
#         paste("ambiguous cases:", nb_ambiguous),
#         paste("clear cases:", total_clear_cases),
#         paste0("percent of all the observations caught under this search: ", (total_yyyy_md_md/nb_total_obs)*100,"%"),
#         paste0("percent of yyyy-md-md cases that are ambiguous: ", (nb_ambiguous/total_yyyy_md_md)*100,"%"),
#         paste0("percent of clear cases with day first: ", percent_of_clear_cases_with_day_first,"%")
#       ),
#       collapse="\n"
#     ),
#     percent_successful_guess_of_month_first = percent_successful_guess_of_month_first
#   ))
# }
# 
# 
# test_result <- counting_error_rate(test_first_md, test_second_md)
# cat(test_result$summary) # so 99.9996% of cases, the first number is the day
# percent_successful_guess_of_month_first <- test_result$percent_successful_guess_of_month_first
# 
# # Indicating this error rate in the df
# probable_date_string <- paste0("probable_ddmmyyyy_to_approx_", percent_successful_guess_of_month_first,"_percent_certainty")
# colnames(obs18)[colnames(obs18) == "probable_ddmmyyyy"] <- probable_date_string

# Creating easy column to identify certain dates over probable dates
obs18$is_ambiguous <- 
  ifelse(is.na(obs18$probable_ddmmyyyy),
         0,
         1)

# # Screen for months that should not be
# improper_months_regex <- paste0("^\\d{1,2}/(", paste0(13:99, collapse = "|"), ")/\\d{4}$")
# erroneous_months <- which(!is.na(str_extract(obs18$standardized_ddmmyyyy, improper_months_regex)))
# erroneous_months_df <- obs18[erroneous_months,]

# Section 8: Writing out data with unified columns to desired location
obs18$year <- NULL
obs18$month_alphabetic <- NULL
obs18$day_when_month_alphabetical <- NULL
obs18$yyyy_md_md <- NULL
obs18$month_when_yyyy_md_md <- NULL
obs18$day_when_yyyy_md_md <- NULL
obs18$likely_month_when_yyyy_md_md <- NULL
obs18$likely_day_when_yyyy_md_md <- NULL
obs18$md_md_yyyy <- NULL
obs18$month_when_md_md_yyyy <- NULL
obs18$day_when_md_md_yyyy <- NULL
obs18$likely_month_when_md_md_yyyy <- NULL
obs18$likely_day_when_md_md_yyyy <- NULL
obs18$certain_dd <- NULL
obs18$probable_dd <- NULL
obs18$certain_mm <- NULL
obs18$probable_mm <- NULL
obs18$certain_ddmmyyyy <- NULL
obs18$probable_ddmmyyyy <- NULL
obs18$just_month_when_alphabetic <- NULL
obs18$month_numeric_when_alphabetic <- NULL






# Re-assembling our 18 parts

all_obs <- rbind(obs1, obs2, obs3, obs4, obs5, obs6, obs7, obs8, obs9, obs10, obs11, obs12, obs13, obs14, obs15, obs16, obs17, obs18)

print(obs[7777777, "observed_on_string"])
print(all_obs[7777777, "observed_on_string"])
print(all_obs[7777777, "standardized_date_ddmmyyyy"])





#### (commented out the line to avoid massive accident downloads in undesired locations)
write_parquet(obs, "C:/Users/Dell/OneDrive - McGill University/Laura's Lab_Group - Blitz the Gap/iNaturalist Canada parquet/iNat_non_sensitive_data_Jan2025_dates_standardized.parquet")