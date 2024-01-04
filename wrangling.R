# Load necessary libraries for data manipulation and reading Excel files
library(data.table)
library(readxl)
library(purrr)
library(dplyr)
library(tidyr)
library(furrr)
library(googlesheets4)
library(gargle)

# Google Sheets URL for output
sheet_url <- "https://docs.google.com/spreadsheets/d/1r4hPXZ57cNn4TJPtKHKUlTSAm0DFPNqLShx2rIsP8G0/edit#gid=0"
email <- "gmail_account"
# Set Google authentication options for API access
options(gargle_oauth_email = email)

# Configure parallel processing with 4 workers
plan(multisession, workers = 4)

# List all Excel files in specified directories
xls_files <- list.files("./metadata-excel/", "*.xlsx", full.names = T)
xls_wrong_files <- list.files("./metadata-excel-wrong", "*.xlsx", full.names = T)

# Define a function to read and process metadata from Excel files
read_metadata  <- function(x) {
  # Read an Excel file
  .md <- read_excel(x)
  # Check if the file has any rows to process
  if (nrow(.md) > 0) {
    .md %>%  
      # Standardize column names by converting to lower case and replacing spaces with underscores
      rename_all(~tolower(gsub(" ", "_", .))) %>% 
      # Select specific columns that start with given strings
      select(starts_with("assembly"), 
             starts_with("org"),
             starts_with("ani")) %>% 
      # Transform data from long to wide format
      pivot_wider(names_from = assembly_biosample_attribute_name,
                  values_from = assembly_biosample_attribute_value) %>% 
      # Replace NA values in a specific column with values from another column
      mutate(assembly_biosample_sample_identifiers_label = ifelse(is.na(assembly_biosample_sample_identifiers_label),assembly_biosample_sample_identifiers_database,assembly_biosample_sample_identifiers_label)) %>% 
      # Remove certain columns
      select(-c(assembly_biosample_sample_identifiers_database,
                assembly_bioproject_lineage_title,
                assembly_bioproject_lineage_accession,
                assembly_bioproject_lineage_parent_accessions)) %>% 
      # Another transformation from long to wide format, making column names unique
      pivot_wider(names_from = assembly_biosample_sample_identifiers_label,
                  values_from = assembly_biosample_sample_identifiers_value,
                  names_repair = "unique") %>% 
      # If any column is a list, extract the first element
      mutate_if(is.list, ~ lapply(.x, `[`, 1)) %>% 
      # Convert all columns to character type
      mutate_all(as.character) %>% 
      # Remove columns containing ".." in their names
      select(-contains("..")) %>% 
      # Add the file name as a new column
      mutate(file_name = basename(x)) %>% 
      # Move the file name column to the first position
      relocate(file_name, .before = everything()) 
  } else {
    return(NA)
  }
}

# Apply the read_metadata function to all files in xls_files and xls_wrong_files using parallel processing
# and discard any results that are NA
map_list <- future_map(xls_files, read_metadata, .progress = T) %>% 
  discard(.,function(x) is.na(x[[1]]))
map_list_wrong <- future_map(xls_wrong_files, read_metadata, .progress = T) %>% 
  discard(.,function(x) is.na(x[[1]]))

# Combine the data from both sets of files into one data frame
df <- bind_rows(map_list, map_list_wrong)

# Select columns where all values are not NA and get their names
selected_col <- apply(df, 2, function(x) all(!is.na(x))) %>% names(df)[.]

# Create a final data frame selecting specific columns and rearranging some of them
final_df <- df %>% dplyr::select(selected_col,
                                 "serovar",
                                 SRA,
                                 starts_with("strain"),
                                 contains("geo_loc"),
                                 lat_lon,
                                 starts_with("source"),
                                 starts_with("organism"),
                                 contains("_date"),
                                 host,
                                 host_disease
) %>% 
  relocate(serovar:geo_loc_name, .after = assembly_bioproject_accession) %>% 
  # Select columns which don't have all NA values
  select_if(~ !all(is.na(.))) %>% 
  # Remove the .xlsx extension from the file_name column
  mutate(file_name = gsub(".xlsx","",file_name))



# Write the final data frame to a Google Sheet
write_sheet(final_df, ss = sheet_url, sheet = "NCBI_METADATA")

# Read another Excel file for comparison
original_xls <- read_excel("assembly_search_list.xlsx", col_names = c("assembly_acc", "assembly_name", "undefined")) %>% 
  select(1,2)

# Perform a left join to find missing accession numbers
missing_acc <- left_join(original_xls, final_df, by = c("assembly_acc"= "file_name"))

# Filter for missing accession numbers and select the first two columns
missing_acc %>% 
  filter(is.na(assembly_accession)) %>% 
  select(1:2) -> missing_accessions

# Write the missing accessions to a Google Sheet
write_sheet(missing_accessions, ss = sheet_url, sheet = "MISSING_ACCESSIONS")

# Uncomment this line to read a TSV file if needed
# complete <- fread("complete_genomes (1).tsv")
