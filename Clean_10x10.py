# -*- coding: utf-8 -*-
"""
This is the configuration for the 10x10m SERDP Burns and parameters for 
'cleaning' the data and extracting the data from the dataloggers.

Creator: Joseph Seitz
Graduate Student in the Geograpy Department at Michigan State University
Created: January 2021
"""
'''Location of Raw Dataloggers'''
### This is the location where the raw datalogger files are located. They have been
### put into directories as structured is the path given. If you want to 
### download the raw dataloggers, the program will run quicker on local 
### machines, change the path below 

#base_path = "http://35.12.130.8/study/SERDP-10x10m-Data/Raw-Sonic-TC-Data/"+ \
#        "SERDP-Burn"

### Example of a local directory
base_path = "/Users/joeyp/OneDrive/SERDP/10X10_Truss_SERDP_Burns/"+ \
                   "Raw-Sonic-TC-Data/SERDP-Burn"

'''Save Attributes'''
### The default saving location is the current working directory, if you would 
### like to specify a different location, change save_loc = Full Path. Note 
### Note that the directory format must be with "/" and not "\".         

save_loc = "./"

### Although it is recommended that you save the data as a .txt file, you can 
### change the file to a .csv file but changing this value to ".csv"

file_type = ".txt"

### We recommend saving the files as a text file, since excel can mess up the 
### Timestamp column. Since it is a text file you can specific what seperator
### you want between your values. Default: " ", EX: "\t" (tab sep), "," (csv), 
### etc. NOTE: If .csv file change to ","

seperator = " "


''' Data Structure Inputs'''
### Below is the list of the burn aviable to clean in the current state of code
### Note Burns 9-13 are currently missing due to metadata conflicts (int)
burn_num_lst = [1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 18, 19, 20, 21, 22, 23,\
               24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34]

## NOTE: Burn #'s 10, 14, 15, 16, 17 don't have data
### If less burns are desired comment out the list above and add the burn 
### Numbers to the list here
#burn_num_lst = [1, 2, 3, 4, 5, 6, 18, 19, 20, 21, 22, 23,\
#               24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34]

'''Making Data Continuous'''
### If the user does not want gaps in the data, set mk_contins = "y", to 
### make the data continuous. This will fill in any missing times with a
### missing data flag. Set to "n" will leave the time gaps.

mk_contins = "y"

'''Dealing with the timestamp columns'''
### If "n" timestamp column will look like this: "2018-03-05 13:32:28.500"
### If "y" the columns will be split to this format: YYYY MM DD Hr Min Sec 

sep_time_cols = "n"

'''Dealing with missing values'''
### The default missing value is 'NaN'. This can be any string, integer, or number
### desired, another common missing value is 9999.

fill_nan = "NaN"

'''Correcting Data'''
### If the user does not want to apply corrections to the data change this to
### "n" and just the raw output will be saved, the sonic files will also 
### contain an extra "DIAG column, this is the RY Sonic Diagnostic which says
### any code that is not 0, the data is invalid. If set to "y" it will apply the 
### corrections with the parameters that follow.

mk_corcts = "y"

### If you have mk_corcts= "n" you do not need to change any of these below, 
### since the corrections will not be applied.

'''Correction Parameters: Sonics'''
### Below are the correction parameters that the user defines. NOTE: If the 
### data doesn't fit the set paremeters it will be replace with the nan value
# Max wind speeds for the sonics
m_speed = 40 #m/s (RMY Sonic operating range = |40m/s|)

# Minimum Sonic Temperature. It can be helpful to put a mimimum based off the 
# daily minimum to remove invalid temperatures. Set to -10 since no burns reach
# negative temperature values.
min_sn_T = -10 # degrees C (RMY Sonic operating range = |50C|)

# Maximum Sonic Temperature
max_sn_T = 50 # degrees C (RMY Sonic operating range = |50C|)


# Changing the wind directions to meterologic definition. Note this is done to
# all sonics, so this is NOT were the tilt correction would be applied
u_fctr = -1 # U vector winds
v_fctr = -1 # V vector winds

# This is if there is need to correct or 'flip' all the vertical wind values
w_fctr = 1 # W vector (verticle winds)

'''Correction Parameters: Thermocouples'''
# Minimum thermocouple temperature. It can be helpful to put a mimimum based 
# off the daily minimum to remove invalid temperatures. Set to -10 since no 
# burns reach negative temperature values.
min_tc_T = -10

# Max Thermocouple temperature. Since there is a high temperature threshold for
# TC's there is not much need to check the max values. If max_tc_T = int(10E6),
# There will not be a max temperature check in the TC, change the value to 
# a different value and the check with be applied. 
max_tc_T = int(10E6)

'''Diagostic Check'''
# From the RMY Sonic Manual: 
# "Any non-zero error code is an invalid measurement ... Keys to error provide
# no useful information"
# If the user wants to keep these measurements with non-zero error code and the
# error code column, "DIAG", make diag_check = "n". If this option is used the 
# user may have non-valid sonic obervations.  
diag_check = "y"

fmt = "Corrections to be Applied \n" + \
    "{}*U, {}*V, {}*W (No Change),\n" + \
    "Max Wind Speed = |{}| m/s, Max Sonic Temperature = {}C \n" + \
    "Min Sonic Temperature = {} C, Min TC Temperature = {}C\n" + \
    "Max TC Temperature = {}C (no check = 10E6)" 
if mk_corcts == "y":
    print(fmt.format(u_fctr, v_fctr, w_fctr, m_speed, max_sn_T, min_sn_T, \
                     min_tc_T, max_tc_T))

""" Don't change anything below this point """

cor_lst = [m_speed, min_sn_T, max_sn_T, u_fctr, v_fctr, w_fctr, min_tc_T, \
           max_tc_T, diag_check]

# time to run the batch cleaner
from Burn_Compiler import master
for burn in burn_num_lst:
    master(base_path, burn, save_loc,seperator, file_type, mk_contins, sep_time_cols, \
           fill_nan, mk_corcts, cor_lst)
    
