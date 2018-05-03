##############################################################################################################################
#     Title: ImportQTribParameters.R
#     Description: This script is based on the ImportProfiles.R script developed by
#        Dan Crocker. This script will process/import Quabbin Trib Field Parameter Data 
#        to database
#     Written by: Dan Crocker, Max Nyquist, Brett Boisjolie
#     Last Update: May 2018
#     Please note - Comments with #!# are from Brett
#
##############################################################################################################################

#!# Delete # when running outside Shiny

#library(tidyverse)
#library(stringr)
#library(odbc)
#library(RODBC)
#library(DBI)
#library(lubridate)
#library(magrittr)
#library(readxl)
#library(DescTools)
# 
#scriptname <- "ImportProfilesQ.R"
#config <- read.csv("//env.govt.state.ma.us/enterprise/DCR-WestBoylston-WKGRP/WatershedJAH/EQStaff/WQDatabase/R-Shared/WAVE-WIT/Configs/WAVE_WIT_Config.csv", header = TRUE)
#config <- as.character(config$CONFIG_VALUE)
#dataset <-  read_excel(config[9], sheet = 1, col_names = T, trim_ws = T) %>%
#filter(ImportMethod == "Importer-R" & ScriptProcessImport == scriptname)
#Choose the dataset from options (Trib Selected):
#dataset <- slice(dataset,2)

### Function Arguments:
#rawdatafolder <- paste0(dataset[10])
#processedfolder <- paste0(dataset[11])
#testdb #!#Not sure what this is for
#filename.db <- paste0(dataset[6])
#ImportTable <- paste0(dataset[7])
#ImportFlagTable <- NULL # This data has no related flag table
#probe <- "Eureka"
#
# ### Find the file to Import
#files <- grep(
#   x = list.files(rawdatafolder, ignore.case = T, include.dirs = F),
#   pattern = "^(?=.*\\b(xlsx|csv)\\b)(?!.*\\$\\b)", # regex to show xlsx files, but filter out lockfiles string = "$"
#   value = T,
#   perl =T
#)
# 
# #  List the files:
#files

# # Select the file to import manually
#file <- files[1]

###############################################################################################
PROCESS_DATA <- function(file, rawdatafolder, filename.db, probe = NULL, ImportTable, ImportFlagTable = NULL){ # Start the function - takes 1 input (File)
 
  # Eliminate Scientific notation in numerical fields
  options(scipen = 50) 
  
  # Get the full path to the file
  path <- paste0(rawdatafolder,"/", file)
 
  # Read in the raw data - defaults to the last sheet added
  df.wq <- read.csv(path, header=TRUE)

  # Data class/formats
  df.wq$DATE <- as.POSIXct(mdy(df.wq$DATE))
  df.wq$SampleDateTime <- NA
  df.wq$SampleDateTime <- as.POSIXct(paste(as.Date(df.wq$DATE, format ="%Y-%m-%d"), df.wq$TIME, sep = " "), format = "%Y-%m-%d %H:%M:%S", tz = "America/New_York", usetz = T)
  
  #!# Drop unnecesary columns from DF
  df.wq$DATE <- NULL
  df.wq$TIME <- NULL
  df.wq$Depth.m <- NULL
  df.wq$CablePower.V <- NULL
  df.wq$Chl.ug.l <- NULL
  
  #!# Change our parameter names to match DB parameter abbreviations
  colnames(df.wq) <- c("TWA-C","LDOd","LDOs","pH","SPCD","Site","SampleDateTime")
  
  # reformat the Quabbin Trib field data to "Tidy" data format ("Long" instead of "Wide")
  df.wq <- gather(df.wq, Parameter, Result, c(1:5))
  df.wq$Result <- round(as.numeric(df.wq$Result), 3)
  
  #Specify database connection
  con <- dbConnect(odbc::odbc(),
                   .connection_string = paste("driver={Microsoft Access Driver (*.mdb, *.accdb)}",
                                              paste0("DBQ=", filename.db), "Uid=Admin;Pwd=;", sep = ";"),
                   timezone = "America/New_York")
  df_param <- dbReadTable(con,"tblParameters")
  
  df.wq$`Unit ID` <- probe
  
  # UniqueID
  df.wq$UniqueID <- ""
  df.wq$UniqueID <- paste(df.wq$Site, df.wq$SampleDateTime, df.wq$Parameter, df.wq$`Unit ID`, sep = "_")
  
  ## Make sure it is unique within the data file - if not then exit function and send warning
  dupecheck <- which(duplicated(df.wq$UniqueID))
  dupes <- df.wq$UniqueID[dupecheck] # These are the dupes
  
  if (length(dupes) > 0){
    # Exit function and send a warning to userlength(dupes) # number of dupes
    stop(paste("This data file contains", length(dupes),
               "records that appear to be duplicates. Eliminate all duplicates before proceeding.",
               "The duplicate records include:", paste(head(dupes, 15), collapse = ", ")), call. = FALSE)
  }
  
  Uniq <- dbGetQuery(con,paste0("SELECT UniqueID, ID FROM ", ImportTable))
  dupes2 <- Uniq[Uniq$UniqueID %in% df.wq$UniqueID,]
  
  if (nrow(dupes2) > 0){
    # Exit function and send a warning to user
    stop(paste("This data file contains", nrow(dupes2),
               "records that appear to already exist in the database! 
               Eliminate all duplicates before proceeding.",
               "The duplicate records include:", paste(head(dupes2$UniqueID, 15), collapse = ", ")), call. = FALSE)
  }
  rm(Uniq)
  
  ###############################################################################################
  
  ###############################################################################################
  
  ### DataSource
  df.wq <- df.wq %>% mutate(DataSource = paste(file))
  
  ### DataSourceID
  # Do some sorting first:
  df.wq <- df.wq[with(df.wq, order(SampleDateTime, Site)),]
  
  # Assign the numbers
  df.wq$DataSourceID <- seq(1, nrow(df.wq), 1)
  
  ### Importdate (Date)
  df.wq$ImportDate <- Sys.Date() %>% as.Date()
  
  
  # Read Tables
  # WQ
  setIDs <- function(){
    query.wq <- dbGetQuery(con, paste0("SELECT max(ID) FROM ", ImportTable))
    # Get current max ID
    if(is.na(query.wq)) {
      query.wq <- 0
    } else {
      query.wq <- query.wq
    }
    ID.max.wq <- as.numeric(unlist(query.wq))
    rm(query.wq)
    
    ### ID wq
    df.wq$ID <- seq.int(nrow(df.wq)) + ID.max.wq
  }
  df.wq$ID <- setIDs()
  
  
  # Change parameter to full name and add column for units
  df.wq$Parameter <- df_param$ParameterName[match(df.wq$Parameter, df_param$ParameterAbbreviation)]
  df.wq$Units <- df_param$ParameterUnits[match(df.wq$Parameter, df_param$ParameterName)]
  
  # Reorder remaining 30 columns to match the database table exactly
  df.wq <- df.wq[, c(10, 1:4, 11, 5:9)]
  
  # Get column names from db table
  cnames <- dbListFields(con, ImportTable)
  #list(cnames)
  names(df.wq) <- cnames
  
  # Create a list of the processed datasets
  dfs <- list()
  dfs[[1]] <- df.wq
  dfs[[2]] <- path
  dfs[[3]] <- NULL # Removed condition to test for flags and put it in the setFlagIDS() function
  
  # Disconnect from db and remove connection obj
  dbDisconnect(con)
  rm(con)
  return(dfs)
} # END FUNCTION

##!##Remove comments outside of Shiny
# dfs <- PROCESS_DATA(file, rawdatafolder, filename.db, probe, ImportTable = ImportTable, ImportFlagTable = NULL )

# Extract each element needed
##!##Remove comments outside of Shiny
#df.wq     <- dfs[[1]]
#path      <- dfs[[2]]
#df.flags  <- dfs[[3]]

########################################################################################################

##########################
# Write data to Database #
##########################

IMPORT_DATA <- function(df.wq, df.flags = NULL, path, file, filename.db, processedfolder, ImportTable, ImportFlagTable = NULL){
  # df.flags is an optional argument  - not used for this dataset
  
  # Establish db connection
  con <-  odbcConnectAccess(filename.db)
  # Get Import Table Columns
  ColumnsOfTable <- sqlColumns(con, ImportTable)
  
  # Set variable types
  varTypes  <- as.character(ColumnsOfTable$TYPE_NAME) 
  sqlSave(con, df.wq, tablename = ImportTable, append = T, 
          rownames = F, colnames = F, addPK = F , fast = F, varTypes = varTypes)
  
  # Disconnect from db and remove connection obj
  odbcCloseAll()
  rm(con)
  
  return("Import Successful")
}
### END 

#!# Remove comments outside Shiny
#IMPORT_DATA(df.wq, df.flags = NULL, path, file, filename.db, processedfolder = NULL,
#            ImportTable = ImportTable, ImportFlagTable = NULL)
