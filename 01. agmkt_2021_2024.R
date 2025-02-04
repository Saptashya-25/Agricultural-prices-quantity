rm(list = ls())


# Load necessary libraries
library(dplyr)

#install.packages("lubridate")
library(lubridate)

####For prices###
# Set the base working directory
base_dir <- "C:/Users/saptashya.ghosh/Dropbox/agmarket_spillover/2. raw/agmkt_new/prices/"

# Loop through each year from 2021 to 2024
for (year in 2021:2024) {
  # Set the working directory for the current year
  setwd(paste0(base_dir, year))
  
  # Get a list of all CSV files in the directory (full names to avoid path issues)
  csv_files <- list.files(pattern = "\\.csv$", full.names = TRUE)
  
  # Filter out empty files
  csv_files <- csv_files[file.info(csv_files)$size > 0]
  
  # Read and append all CSV files into one dataframe, converting specified columns to character
  combined_data <- lapply(csv_files, function(file) {
    tryCatch({
      read.csv(file, stringsAsFactors = FALSE) %>%
        mutate(
          state_id = as.character(state_id),          # Convert state_id to character
          state_name = as.character(state_name),       # Convert state_name to character
          district_name = as.character(district_name),  # Convert district_name to character
          market_name = as.character(market_name),      # Convert market_name to character
          census_state_name = as.character(census_state_name),
          census_district_name = as.character(census_district_name),
          commodity_name = as.character(commodity_name),
          variety = as.character(variety),
          grade = as.character(grade),
          date = as.character(date)
        )
    }, error = function(e) {
      message(paste("Error in file:", file, ":", e$message))
      return(NULL)  # Return NULL for problematic files
    })
  }) %>%
    bind_rows()
  
  # Write the combined data for the current year into one CSV file
  write.csv(combined_data, paste0("combined_data_", year, "_price.csv"), row.names = FALSE)
}

# Initialize an empty list to store the data from each year
all_data <- list()

# Loop through each year (2021 to 2024) and merge the combined data from each directory
for (year in 2021:2024) {
  # Construct the full path for the current year's directory
  file_path <- paste0(base_dir, year, "/combined_data_", year, "_price.csv")
  
  # Check if the file exists, and if so, read and append the data
  if (file.exists(file_path)) {
    year_data <- read.csv(file_path, stringsAsFactors = FALSE)
    all_data[[length(all_data) + 1]] <- year_data
  } else {
    message(paste("File not found:", file_path))
  }
}

# Combine all the data from 2021 to 2024 into one dataframe
final_combined_data <- bind_rows(all_data)

# Write the final merged data to a CSV file
write.csv(final_combined_data, paste0(base_dir, "combined_price.csv"), row.names = FALSE)


####For Quantity###
# Set the base working directory
base_dir <- "C:/Users/saptashya.ghosh/Dropbox/agmarket_spillover/2. raw/agmkt_new/quantity/"

# Loop through each year from 2021 to 2024
for (year in 2021:2024) {
  # Set the working directory for the current year
  setwd(paste0(base_dir, year))
  
  # Get a list of all CSV files in the directory (full names to avoid path issues)
  csv_files <- list.files(pattern = "\\.csv$", full.names = TRUE)
  
  # Filter out empty files
  csv_files <- csv_files[file.info(csv_files)$size > 0]
  
  # Read and append all CSV files into one dataframe, converting specified columns to character
  combined_data <- lapply(csv_files, function(file) {
    tryCatch({
      read.csv(file, stringsAsFactors = FALSE) %>%
        mutate(
          id = as.numeric(id),
          state_id = as.character(state_id),          # Convert state_id to character
          state_name = as.character(state_name),       # Convert state_name to character
          district_name = as.character(district_name),  # Convert district_name to character
          district_id  = as.numeric(district_id),
          market_name = as.character(market_name),      # Convert market_name to character
          market_id = as.numeric(market_id),
          census_state_id = as.numeric(census_state_id),
          census_state_name = as.character(census_state_name),
          census_district_id = as.numeric(census_district_id),
          census_district_name = as.character(census_district_name),
          commodity_id = as.numeric(commodity_id),
          commodity_name = as.character(commodity_name),
          date = as.character(date)
        )
    }, error = function(e) {
      message(paste("Error in file:", file, ":", e$message))
      return(NULL)  # Return NULL for problematic files
    })
  }) %>%
    bind_rows()
  
  # Write the combined data for the current year into one CSV file
  write.csv(combined_data, paste0("combined_data_", year, "_qnt.csv"), row.names = FALSE)
}

# Initialize an empty list to store the data from each year
all_data <- list()

# Loop through each year (2021 to 2024) and merge the combined data from each directory
for (year in 2021:2024) {
  # Construct the full path for the current year's directory
  file_path <- paste0(base_dir, year, "/combined_data_", year, "_qnt.csv")
  
  # Check if the file exists, and if so, read and append the data
  if (file.exists(file_path)) {
    year_data <- read.csv(file_path, stringsAsFactors = FALSE)
    all_data[[length(all_data) + 1]] <- year_data
  } else {
    message(paste("File not found:", file_path))
  }
}

# Convert all variables to character except 'quantity'
#all_data <- lapply(all_data, function(df) {
 # df %>%
 #   mutate(across(-quantity, as.character))  # Convert all columns except 'quantity' to character
#})

# Combine all the modified data frames into one
final_combined_data <- bind_rows(all_data)

# Write the final merged data to a CSV file
write.csv(final_combined_data, paste0(base_dir, "combined_qnt.csv"), row.names = FALSE)

##########################################################################################

# File paths for the price and quantity datasets
price_file <- "C:/Users/saptashya.ghosh/Dropbox/agmarket_spillover/2. raw/agmkt_new/prices/combined_price.csv"
quantity_file <- "C:/Users/saptashya.ghosh/Dropbox/agmarket_spillover/2. raw/agmkt_new/quantity/combined_qnt.csv"

price_file <- "2. raw/agmkt_new/prices/combined_price.csv"
quantity_file <- "2. raw/agmkt_new/quantity/combined_qnt.csv"

# Read the price dataset
price_data <- read.csv(price_file, stringsAsFactors = FALSE)

# Read the quantity dataset
quantity_data <- read.csv(quantity_file, stringsAsFactors = FALSE)

# Sorting by commodity_id, market_id, census_state_id, district_id, date
price_data <- price_data %>%
  arrange(commodity_id, market_id, census_state_id, district_id, date)

# Remove duplicates based on the specified columns for price
price_data <- price_data %>%
  distinct(commodity_id, market_id, census_state_id, district_id, date, .keep_all = TRUE)

quantity_data <- quantity_data %>%
  arrange(commodity_id, market_id, census_state_id, district_id, date)

# Remove duplicates based on the specified columns for Quantity
quantity_data <- quantity_data %>%
  distinct(commodity_id, market_id, census_state_id, district_id, date, .keep_all = TRUE)

quantity_data <- quantity_data[ , !(names(quantity_data) %in% c("variety", "grade", "min_price", "max_price", "modal_price"))]

# Ensure 'date' is a character in both datasets
price_data$date <- as.character(price_data$date)
quantity_data$date <- as.character(quantity_data$date)

merged_data <- merge(price_data, quantity_data, 
                     by = c("commodity_id", "market_id", "census_state_id", "district_id", "date"), 
                     all = TRUE)

# Create the 'merge' column:
# - If both price and quantity are present, set 'merge' to 1.
# - If only price is present, set 'merge' to 2.
# - If only quantity is present, set 'merge' to 3.

# Assuming 'price' and 'quantity' are the columns that hold the values in each dataset
merged_data$merge <- with(merged_data, ifelse(!is.na(modal_price) & !is.na(quantity), 1, 
                                              ifelse(!is.na(modal_price) & is.na(quantity), 2, 
                                                     ifelse(is.na(modal_price) & !is.na(quantity), 3, NA))))

#order the id varibles in merged data
merged_data <- merged_data %>%
  arrange(commodity_id, market_id, census_state_id, district_id, date)

# Filter rows where merge == 1
merged_data <- merged_data %>%
  filter(merge == 1)

merged_data <- merged_data[ , !(names(merged_data) %in% c("id.y", "state_id.y", "state_name.y", "district_name.y", 
                                                          "market_name.y", "census_state_name.y", "census_district_id.y", 
                                                          "census_district_name.y", "commodity_name.y"))]

     
# Use gsub to remove ".y" from column names
names(merged_data) <- gsub("\\.x$", "", names(merged_data))


# Convert the character date to a Date object (if needed)
merged_data$date <- ymd(merged_data$date)

# Extract year, month, and day
merged_data$year <- year(merged_data$date)
merged_data$month <- month(merged_data$date)
merged_data$day <- day(merged_data$date)

# Aggregate data to monthly level by summing the quantity
monthly_qnt <- merged_data %>%
  group_by(commodity_id, market_id, census_state_id, district_id, year, month) %>%
  summarise(agg_qnt = sum(quantity, na.rm = TRUE)) %>%
  ungroup()

# Join the aggregated quantity back to the original data
merged_data <- left_join(merged_data, monthly_qnt, by = c("commodity_id", "market_id", "census_state_id", "district_id", "year", "month"))

# Calculate the new result
merged_data$result <- (merged_data$modal_price * merged_data$quantity) / merged_data$agg_qnt

# Aggregate data to monthly level by summing the quantity
monthly_pr <- merged_data %>%
  group_by(commodity_id, market_id, census_state_id, district_id, year, month) %>%
  summarise(agg_pr = sum(result, na.rm = TRUE)) %>%
  ungroup()

# Join the aggregated quantity back to the original data
merged_data <- left_join(merged_data, monthly_pr, by = c("commodity_id", "market_id", "census_state_id", "district_id", "year", "month"))



# Remove duplicates based on the specified columns for Quantity
merged_data <- merged_data %>%
  distinct(commodity_id, market_id, census_state_id, district_id, year, month, .keep_all = TRUE)


merged_data <- merged_data[ , !(names(merged_data) %in% c("day", "min_price", "max_price", "modal_price", 
                                                          "quantity", "merge", "result", "grade", "date"))]


# Rename agg_qnt to quantity
colnames(merged_data)[colnames(merged_data) == "agg_qnt"] <- "quantity"

# Rename agg_pr to price
colnames(merged_data)[colnames(merged_data) == "agg_pr"] <- "price"

# Create a new column for date in the format "YYYY-M"
merged_data <- merged_data %>%
  mutate(date = paste(year, month, sep = "-"))  # Create date as character

# Sorting by commodity_id, market_id, census_state_id, district_id, date
merged_data <- merged_data %>%
  arrange(date, commodity_id, market_id, census_state_id, district_id)

# Save the merged data to a new CSV file
write.csv(merged_data, "C:/Users/saptashya.ghosh/Dropbox/agmarket_spillover/4.outputs/agmkt_new.csv", row.names = FALSE)

agmkt_data <- read.csv("C:/Users/saptashya.ghosh/Dropbox/agmarket_spillover/4.outputs/agmkt_new.csv")
