# Goal: edit date cols in iNat parquet file to make all entries the same format
# Usage: Run the code line by line for your specific dataframe as the checks may need slight modifications

# Ryan Hull with help from: Leeya Nagpal and Laura Pollock
# Quantitative Biodiversity Lab, McGill University
# July 2025

# Section 1: Loading in data
library(ggplot2)
library(dplyr)
library(arrow)
library(dbplyr, warn.conflicts = FALSE)
library(duckdb)

# Loading and viewing data from iNat parquet file
rm(list=ls())
inat_pq <- arrow::open_dataset("C:/Users/Dell/OneDrive - McGill University/Laura's Lab_Group - Blitz the Gap/iNaturalist Canada parquet/iNat_non_sensitive_data_Jan2025.parquet")

# Load the portion of the parquet file you wish to modify the dates for
obs <- inat_pq |>
  # filter to mammals in this case
  filter(iconic_taxon_name %in% "Mammalia") |>
  # select columns we want to keep
  select(c(longitude, latitude, 
           observed_on_string,
           iconic_taxon_name, 
           scientific_name, 
           coordinates_obscured,
           place_county_name)) |>
  collect()



# Section 2: Extracting the year
#2.1 first pass - grep any set of 4 numbers
obs$year4num <- regmatches(obs$observed_on_string, gregexpr("\\d{4}", obs$observed_on_string))
obs$year <- obs$year4num
obs$year <- sapply(obs$year4num, function(x){ ifelse(length(x)==0,NA,x[1])}) # to NA the ones with no year
length(which(is.na(obs$year))) # about 95% of these cause theres no entry at all. 5% have an entry without a year

#2.2 second pass - correct those with multiple entries of 4 numbers
twos_indx <- which(sapply(obs$year4num, length) == 2)

year_only <- lapply(obs$year4num[twos_indx], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 1800 & x_num < 2026] # the plausible 4digit is used
})
obs$year[twos_indx] <- year_only

#2.3 Any more still left? Then refine date inclusion criteria
twos_indx_still <- which(sapply(obs$year, length) == 2)
obs$observed_on_string[twos_indx_still] # displays the dates
    
# All years 2015 and 2016 with manual check, so use >2000. Change this if need be for different dfs
year_only <- lapply(obs$year[twos_indx_still], function(x) {
  x_num <- as.numeric(x)
  x[x_num > 2000]
})
obs$year[twos_indx_still] <- year_only

# 2.4 Final check - any left? If so manually edit your df
length(which(sapply(obs$year, length) == 2)) # if empty, no further checks needed
obs$year4num <- NULL




# Section 3 - Extracting the Month and dates when month listed as letters

# Finding the positions
months_abbreviated <- c("Jan","Feb","Mar","Apr","May","Jun", "Jul","Aug","Sep",
                        "Oct","Nov","Dec","jan","feb","mar","apr","may","jun",
                        "jul","aug","sep","oct","nov","dec", "fév","avr","juil",
                        "sept")
months_full <- c("January", "February","March","April","May","June","July",
                 "August", "September","October","November","December",
                 "january", "february","march","april","may","june","july",
                 "august", "september","october","november","december",
                 "janvier","février","mars","avril","mai","juin","juillet",
                 "aout","août","septembre","octobre","novembre","décembre",
                 "Janvier","Février","Mars","Avril","Mai","Juin","Juillet",
                 "Aout","Août","Septembre","Octobre","Novembre","Décembre")
months_alphabetic <- c(months_abbreviated, months_full)
months_alphabetic_regex <- paste(months_alphabetic, collapse="|")
month_alphabetic_positions <- regexpr(months_alphabetic_regex, obs$observed_on_string) # -1 if not found

# how many of these are there?
nb_alhpabetical <- length(which(month_alphabetic_positions!=-1)) # roughly 15% of cases

# Assigning them or NA to month column
obs$month_alphabetic <-
  ifelse(month_alphabetic_positions == -1, NA, regmatches(obs$observed_on_string, month_alphabetic_positions))

# Finding the dates from these rows - should be the only " XX " in the string
obs$day_when_month_alphabetical <- NA
rows_with_alphabetical_month <- (month_alphabetic_positions != -1) & !is.na(month_alphabetic_positions)
positions_alpha_day <- regexec("\\s(\\d{2})\\s", obs$observed_on_string[rows_with_alphabetical_month])
day_when_alpha <- regmatches(obs$observed_on_string[rows_with_alphabetical_month], positions_alpha_day)
obs$day_when_month_alphabetical[rows_with_alphabetical_month] <- 
  sapply(day_when_alpha, function(x) x[2])

# Looking for errors
length(which(sapply(obs$day_when_month_alphabetical, length)> 1)) # any double dates? should be none
length(which(obs$day_when_month_alphabetical > 31))# any false dates? should be none



# Section 4: Extracting months and dates when listed as yyyy-m?d?-m?d?
# Issue: many formats (yyyy-mm-dd, yyyy-dd-mm, yyyy-d-m, yyyy-m-d)

# New column to contain only dates of this format
obs$yyyy_md_md <- regmatches(obs$observed_on_string, gregexpr("\\d{4}[/-]\\d{2}[/-]\\d{2}", obs$observed_on_string))
obs$yyyy_md_md[sapply(obs$yyyy_md_md, length)==0] <- NA
nb_yyyy_md_md <- length(which(!is.na(obs$yyyy_md_md))) # the vast majority

# Extracting the numbers
obs$yyyy_md_md_string <- sapply(obs$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", obs$yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
obs$yyyy_md_md_vector <- regmatches(obs$yyyy_md_md_string, positions_yyyy_md_md)
obs$yyyy_md_md_string <- NULL

first_md <- as.integer(sapply(obs$yyyy_md_md_vector, function(x) return(x[3])))
second_md <- as.integer(sapply(obs$yyyy_md_md_vector, function(x) return(x[4])))
obs$yyyy_md_md_vector <- NULL

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

obs$month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$month)
obs$day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$day)
obs$likely_month_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_month)
obs$likely_day_when_yyyy_md_md <- sapply(yyyy_md_md_results, function(x) x$likely_day)



# Section 5: Extracting months and dates when listed as md-md-yyyy - there are very few
# Issue:  many formats (mm-dd-yyyy, dd-mm-yyyy, d-m-yyyy, m-d-yyyy)

# New column to contain only dates of this format
obs$md_md_yyyy <- regmatches(obs$observed_on_string, gregexpr("\\d{1,2}[-/]\\d{1,2}[-/]\\d{4}", obs$observed_on_string))
obs$md_md_yyyy[sapply(obs$md_md_yyyy, length)==0] <- NA
nb_md_md_yyyy <- length(which(!is.na(obs$md_md_yyyy))) # very few

# Extract the numbers
obs$md_md_yyyy_string <- sapply(obs$md_md_yyyy, function(x) if(length(x)==0) NA else x[1])
positions_md_md_yyyy <- regexec("(\\d{1,2})[-/](\\d{1,2})[-/](\\d{4})", obs$md_md_yyyy_string)
obs$md_md_yyyy_vector <- NA
obs$md_md_yyyy_vector <- regmatches(obs$md_md_yyyy_string, positions_md_md_yyyy)
obs$md_md_yyyy_string <- NULL

first_md_2 <- as.integer(sapply(obs$md_md_yyyy_vector, function(x) return(x[3])))
second_md_2 <- as.integer(sapply(obs$md_md_yyyy_vector, function(x) return(x[4])))
obs$md_md_yyyy_vector <- NULL

# Applying the function to our data and assigning results to columns 
md_md_yyyy_results <- mapply(choosing_month_or_date, first_md_2, second_md_2, SIMPLIFY=FALSE)

obs$month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$month)
obs$day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$day)
obs$likely_month_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_month)
obs$likely_day_when_md_md_yyyy <- sapply(md_md_yyyy_results, function(x) x$likely_day)

# Confirm they were all assigned
length(which(!is.na(obs$day_when_md_md_yyyy))) 



# Section 5: Dealing with exceptions
nb_no_date <- length(obs$observed_on_string[is.na(obs$observed_on_string)]) # when no date is listed
# very few (0.02%) have some non-NA entry other than the formats we've handled. deal with them here if one wishes to
nb_entries_not_caught <- nrow(obs) - sum(nb_alhpabetical, nb_md_md_yyyy, nb_yyyy_md_md, nb_no_date)
paste(nb_entries_not_caught)


# Section 6: Assembling uniform date column 
# 6.1 assembling together different parts of the date
obs$certain_dd <-
  ifelse(!is.na(obs$day_when_month_alphabetical), obs$day_when_month_alphabetical,
                         ifelse(!is.na(obs$day_when_yyyy_md_md), obs$day_when_yyyy_md_md,
                                ifelse(!is.na(obs$day_when_md_md_yyyy), obs$day_when_md_md_yyyy, NA)))

obs$probable_dd <- 
  ifelse(!is.na(obs$likely_day_when_yyyy_md_md), obs$likely_day_when_yyyy_md_md,
         ifelse(!is.na(obs$likely_day_when_md_md_yyyy), obs$likely_day_when_md_md_yyyy, NA))

obs$certain_mm <-
  ifelse(!is.na(obs$month_alphabetic), match(obs$month_alphabetic, month.abb),
         ifelse(!is.na(obs$month_when_yyyy_md_md), obs$month_when_yyyy_md_md,
                ifelse(!is.na(obs$month_when_md_md_yyyy), obs$month_when_md_md_yyyy, NA)))

obs$probable_mm <-
  ifelse(!is.na(obs$likely_month_when_yyyy_md_md), obs$likely_month_when_yyyy_md_md,
         ifelse(!is.na(obs$likely_month_when_md_md_yyyy), obs$likely_month_when_md_md_yyyy, NA))

# 6.2 assembling final columns
certain_dd <- ifelse(!is.na(obs$certain_dd), obs$certain_dd, "??")
certain_mm <- ifelse(!is.na(obs$certain_mm), obs$certain_mm, "??")
certain_yyyy <- ifelse(!is.na(obs$year), obs$year, "????")

obs$certain_ddmmyyyy <-
  paste(
    ifelse(!is.na(obs$certain_dd), obs$certain_dd, "??"),
    ifelse(!is.na(obs$certain_mm), obs$certain_mm, "??"),
    ifelse(!is.na(obs$year), obs$year, "??"),
    sep="/")

obs$probable_ddmmyyyy <-
  ifelse(
    (!is.na(obs$probable_dd) & !is.na(obs$probable_mm) & !is.na(obs$year)),
    paste(obs$probable_dd, obs$probable_mm, obs$year, sep="/"),
    NA)

obs$probable_and_certain_ddmmyyyy_combined <-
  ifelse((!is.na(obs$probable_ddmmyyyy)), obs$probable_ddmmyyyy, obs$certain_ddmmyyyy)


# Section 7: Assessing the quality of the "assumed" dates of ambiguous cases
# when there is indeed a day > 12 (clear case), what percentage of them are in the first position? 
# this serves as an estimate for the non-clear cases, to see how accurate our month-first assumption is
nb_total_obs <- nrow(obs)
test_yyyy_md_md_string <- sapply(obs$yyyy_md_md,  function(x) if(length(x)==0) NA else x[1])
positions_yyyy_md_md <- regexec("(\\d{4})[-/](\\d{1,2})[-/](\\d{1,2})", test_yyyy_md_md_string) # this will give c("yyyy-md-md", "yyyy", "md", "md")
test_yyyy_md_md_vector <- regmatches(test_yyyy_md_md_string, positions_yyyy_md_md)
test_first_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[3])))
test_second_md <- as.integer(sapply(test_yyyy_md_md_vector, function(x) return(x[4])))

counting_error_rate <- function(first_number, second_number){
  
  both_na <- is.na(test_first_md) & is.na(second_number)
  total_yyyy_md_md <- sum(!is.na(first_number))
  first_md_greater_than_12 <- first_number > 12
  second_md_greater_than_12 <- second_number > 12
  ambiguous_cases <- !(both_na | first_md_greater_than_12 | second_md_greater_than_12)
  
  nb_first_md_gt12 <- sum(first_md_greater_than_12, na.rm = TRUE)
  nb_second_md_gt12 <- sum(second_md_greater_than_12, na.rm = TRUE)
  total_clear_cases <- nb_first_md_gt12 + nb_second_md_gt12
  nb_ambiguous <- sum(ambiguous_cases, na.rm=TRUE)
  nb_both_na <- sum(both_na)
  
  percent_of_clear_cases_with_day_first <- (nb_first_md_gt12/total_clear_cases)*100
  percent_successful_guess_of_month_first <- round((100-percent_of_clear_cases_with_day_first), 8)
  
  return( list(
    summary = paste(
      c(
        paste("total cases of yyyy-md-md: ", total_yyyy_md_md),
        paste("ambiguous cases:", nb_ambiguous),
        paste("clear cases:", total_clear_cases),
        paste0("percent of all the observations caught under this search: ", (total_yyyy_md_md/nb_total_obs)*100,"%"),
        paste0("percent of yyyy-md-md cases that are ambiguous: ", (nb_ambiguous/total_yyyy_md_md)*100,"%"),
        paste0("percent of clear cases with day first: ", percent_of_clear_cases_with_day_first,"%")
      ),
      collapse="\n"
    ),
    percent_successful_guess_of_month_first = percent_successful_guess_of_month_first
  ))
}


test_result <- counting_error_rate(test_first_md, test_second_md)
cat(test_result$summary) # so 99.9996% of cases, the first number is the day
percent_successful_guess_of_month_first <- test_result$percent_successful_guess_of_month_first

# Indicating this error rate in the df
probable_date_string <- paste0("probable_ddmmyyyy_to_approx_", percent_successful_guess_of_month_first,"_percent_certainty")
colnames(obs)[colnames(obs) == "probable_ddmmyyyy"] <- probable_date_string



# Section 8: Writing out data with unified columns to desired location
obs$year <- NULL
obs$month_alphabetic <- NULL
obs$day_when_month_alphabetical <- NULL
obs$yyyy_md_md <- NULL
obs$month_when_yyyy_md_md <- NULL
obs$day_when_yyyy_md_md <- NULL
obs$likely_month_when_yyyy_md_md <- NULL
obs$likely_day_when_yyyy_md_md <- NULL
obs$md_md_yyyy <- NULL
obs$month_when_md_md_yyyy <- NULL
obs$day_when_md_md_yyyy <- NULL
obs$likely_month_when_md_md_yyyy <- NULL
obs$likely_day_when_md_md_yyyy <- NULL
obs$certain_dd <- NULL
obs$probable_dd <- NULL
obs$certain_mm <- NULL
obs$probable_mm <- NULL

#### (commented out the line to avoid massive accident downloads in undesired locations)
#### write_parquet(obs, "path/to/your/file.parquet")

# View what you just downloaded:
downloaded_fix_dates_inat_pq <- arrow::open_dataset("path/to/your/file.parquet")

# Load the portion of the parquet file you wish to modify the dates for
fixed_dates_obs <- downloaded_fix_dates_inat_pq |>
  # select columns we want to keep
  select(c(longitude, latitude,
           observed_on_string,
           iconic_taxon_name,
           scientific_name,
           coordinates_obscured,
           place_county_name, certain_ddmmyyyy, probable_date_string, probable_and_certain_ddmmyyyy_combined, )) |>
  collect()