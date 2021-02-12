# SERDP-10x10meter-Burn-Cleaner
The code is to take the raw sonic and thermocouple files from the 10 meter by 10 meter dataloggers and put them into a user friendly format. There is an option to apply parameters to remove data values within specified bounds and to remove those invalid observations.
This repository contains four python files that are used for data extraction.
## Burn_Compiler.py
This file contains the functions that are specific to each burn set up. Since the burns happen on different days the datalogger arangement changes, this is seen in the seperate functions contained in this file comipler#_#.  
