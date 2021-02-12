# SERDP-10x10meter-Burn-Cleaner
The code is to take the raw sonic and thermocouple files from the 10 meter by 10 meter dataloggers and put them into a user friendly format. There is an option to apply parameters to remove data values within specified bounds and to remove those invalid observations.
This repository contains four python files that are used for data extraction.
## Burn_Compiler.py
This file contains the functions that are specific to each burn set up. Since the burns happen on different days the datalogger arangement changes, this is seen in the seperate functions contained in this file: comipler#_#.  
The master function contians the timestart for each of the burns. This start time was found by taking the earliest timestamp that all dataloggers were recording accurate data. There is data before these timestamps on the raw datalogger, but we have found duplicate data entries for the same timestamp and values that do not fit the limits of our instruments, therefor we have found the most reliable times to start and end the files. The master function acts as the facilitor where the user can give inputs that will dictate the output of the files. 

## Clean_10x10.py
This file acts as the input of the cleaning function. There are multiple inputs to the files that can be edited to dictate the output file format and the ability to add data parameters that remove invalid observations and values that do not fit within the instrument's operating conditions. 

## Raw_Cleaner.py
This file contains the data functions that are used in the clean_10x10.py. Most of the functions in this data were writen for these 10x10meter burns, but can be applied to other data files with a similar format. 

## Unused_Data_Functions.py
This file contains data extraction functions that were used in iterations before this final format of the data extraction. These functions are writen in so that they can be used on other data with similar format and can be used in future data cleaning exercises. 


# How to use
In order for the clean_10x10.py to work, both Burn_Compiler.py and Raw_Cleaner.py must be in the same directory. They must be in the same directory since the clean_10x10.py pulls all the functions from these files to perform the extraction. 

First to set up the batch job, enter clean_10x10.py with either a text editor, such as nano, microsoft notepad, spyder and review the input parameters laid out in detail within the file. Once the desired parameters are set, simply run clean_10x10.py and the desired files will be saved. Note: Burns 27-34 have a scorched sonic that was not removed, this causes the correction check to take a couple of minutes to complete since most of the data is invalid.
