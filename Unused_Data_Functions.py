# -*- coding: utf-8 -*-
"""
Extra Functions for data manipulation
@author: joeyp
"""
import pandas as pd
import numpy as np

def timestamp_col(df):
    """
    This function takes seperated time columns and combines them into a 
    pandas.Timestamp() and replaces the seperated time columns with one 
    "TIMESTAMP" column.

    Parameters
    ----------
    df : pandas.DataFrame()
        Dataframe that contains seperated time columns. See drop_col below.

    Returns
    -------
    df : pandas.DataFrame()
        Same dataframe as about but now with one time column: "TIMESTAMP"

    """
    drop_col = ["YYYY", "MM", "DD", "Hr", "Min", "Sec"]
    timestamp_lst = list(np.full(len(df), np.nan))
    for t in range(len(df)):
        timestamp_lst[t] = pd.Timestamp(str(df["YYYY"][t])+"-"+str(df["MM"][t])+"-"+str(df["DD"][t]) +" "+str(df["Hr"][t])+":"+str(df["Min"][t])+":"+str(df["Sec"][t]),freq = ".1S")
    
    df.insert(0, column= "TIMESTAMP", value = timestamp_lst)
    df = df.drop(drop_col, axis=1)
    
    return df

def scalar_wind_tilt_correction(u_i, v_i, theta = 135):
    """
    This function takes U and V wind components and will correct by a 
    specified amount. Default is 135 due to SERDP Flux tower.
    
    Inputs:
        u_i - the U wind component 
        v_i - the V wind component 
        theta - angle of desired correction in degrees (default 135 due to the
                                                        march 2019 flux tower)
    
    Outputs:
        u_f - corrected U  wind component
        v_f - corrected V  wind component
    """
    
    u_f = u_i * np.cos(theta*np.pi/180) - v_i * np.sin(theta*np.pi/180)
    v_f = u_i * np.sin(theta*np.pi/180) + v_i * np.cos(theta*np.pi/180)
    
    return u_f, v_f
    
def df_wind_tilt_correction(df, theta = 135, U_col = "U(19m)", V_col = "V(19m)"):
    """
    This function combined with the 'scalar_wind_tilt_correction' function will
    correct a data frame with U or V columns and apply the angle of correction.
    
    Inputs:
        df - pandas dataframe containing the U and V columns
        theta - angle of desired correction in degrees (default 
                +135 for  SERDP March 2019 Flux tower)
        U_col - the column name containing the U wind components ( default 
                "U(19)" for  SERDP March 2019 Flux tower)
        V_col - the column name containing the V wind components ( default 
                "U(19)" for  SERDP March 2019 Flux tower)
    
    Outputs:
        df - The pandas dataframe with the corrected wind columns
    """
    u_list, v_list = np.full(len(df), np.nan), np.full(len(df), np.nan)
    for i in range(len(df)):
        u_list[i], v_list[i] = scalar_wind_tilt_correction(float(df[U_col][i]),\
                                                float(df[V_col][i]),theta)
             
    df[U_col], df[V_col] = list(u_list), list(v_list)

    return df

def formater(df,columns):
    """
    This function takes a dataframe with timestamp columns and data columns and 
    formats them so that each column looks like this: ##.## or -#.##
        
    input:
        df - pandas dataframe with the separated time columns and data columns
        columns - the columns that are not the time columns but are desired to 
                  be formated
    output:
        df - the formated dataframe 
    """
    
    for col in columns:
        lst_df=list(df[str(col)])
        for i in range(len(lst_df)):
            lst_df[i] = "{:2.2f}".format(float(lst_df[i])).zfill(5)
        df[col]= lst_df
        
    year_lst, month_lst,day_lst= list(df["YYYY"]), list(df["MM"]), list(df["DD"])
    hour_lst, min_lst, second_lst = list(df["Hr"]), list(df["Min"]), list(df["Sec"])
    for i in range(len(df)):
        year_lst[i]  = "{:.0f}".format(float(year_lst[i])).zfill(4)
        month_lst[i] = "{:.0f}".format(float(month_lst[i])).zfill(2)
        day_lst[i]   = "{:.0f}".format(float(day_lst[i])).zfill(2)
        hour_lst[i]  = "{:.0f}".format(float(hour_lst[i])).zfill(2)
        min_lst[i]   = "{:.0f}".format(float(min_lst[i])).zfill(2)
        second_lst[i]= "{:.1f}".format(float(second_lst[i])).zfill(4)
    
    df["YYYY"] = year_lst
    df["MM"]   = month_lst
    df["DD"]   = day_lst
    df["Hr"] = hour_lst
    df["Min"]  = min_lst
    df["Sec"]  = second_lst
    
    return df

def timestamp_correction(df):
    """
    This function takes a df with a messed up timestamp column and creates one 
    with full timestamps 
    input:
        df - the pandas dataframe that has the messed up timestamp columns
        
    output:
        df -  the dataframe with the fixed timestamp column
    """
    
    time=list(df["TIMESTAMP"])
    ### initialize the t_d
    for i in range(len(time)):
        if len(time[i]) > 8:
            t_d= time[i].replace(":"," ").split()[:2]
            break
            
    for i in range(len(time)):

        if len(time[i]) > 8:
            t_d= time[i].replace(":"," ").split()[:2]

        if i !=len(df)-1 and len(time[i])>8:
            t_s = time[i+1].split(".")[0]+".0"

            time[i] = t_d[0] +" "+t_d[1]+":"+t_s 

        else:
            time[i]= t_d[0] +" "+ t_d[1]+":"+time[i]

    df["TIMESTAMP"]=time
    df.drop(df.tail(1).index,inplace=True)
    df["TIMESTAMP"] = pd.to_datetime(df['TIMESTAMP']) 
    
    return df

def repeat(df):
    
    """
    This function takes a df with a "TIMESTAMP" column and checks for 
    repeated timestamps. If it finds repeats, it will return the index of all the 
    repeated times and print out how many and what times it spans. If it 
    doesn't find one it will return an index of zero's 
    
    input:
        df - pandas dataframe to be checked
        
    output:
        repeat_index - A list of the index's that have repeated times in this
                       format: [[],[]] 
    """
    #### Check for repeated times
    lst=list(df["TIMESTAMP"])
    u= np.unique(lst)
    if len(u)!=len(df):
        
        repeat_index, repeat_1, repeat_2 = [], [], []
        for i in range(len(u)):
            ind = df.index[df["TIMESTAMP"]==u[i]].tolist()
            if len(ind)>1:
                repeat_index.append(ind)
                repeat_1.append(df["TIMESTAMP"][ind[0]])
                repeat_2.append(df["TIMESTAMP"][ind[-1]])
        if len(repeat_index)>0:
            print("Yikes! Number of repeated times: ",len(repeat_index),)
            print("Start:", repeat_index[0],"End:",repeat_index[-1])
            print("Time stamp repeats:",repeat_1[0],"-", repeat_2[-1])
                
            return repeat_index
    else:
        print("Hurray! No time Repeats")
        
        return [[0],[0]]
        
def timestamp_matcher(df_names, file_num):
    """
    Takes a list of dataframes, prints the start and ends of all the files,
    then find the earliest timestamp that all dataframes have. Returns the 
    start and end time that can be used to trim the dataframes
    
    input: 
        df_names - list of dataframes 
        file_num - list of names for the files, used only for the print
    
    output:
        time-start - the starting timestampt that works with all files 
        time-end   - the ending timestampt that works with all files
    """
    
    min_lst, max_lst =[],[]

    for i in df_names:
        min_lst.append(i["TIMESTAMP"].min())
        max_lst.append(i["TIMESTAMP"].max())

        fmt = "File: {} | Start: {} | End: {}" 
    for i in range(len(min_lst)):
        print(fmt.format(file_num[i], min_lst[i],max_lst[i] ))

    time_start, time_end = max(min_lst), min(max_lst)    
    print()
    print("Start timestamp Pulled:",time_start, "| End Timestamp Pulled:",\
          time_end)

    finder = ['5T', '30S','S','.1S']
    for n in range(len(finder)):
        test_start=list(pd.date_range(start=time_start, end=time_end,\
                                      freq = finder[n]))
        test_end = test_start[::-1]
        time=[]
        for t in range(len(test_start)): 
            for df in range(len(df_names)):
                if test_start[t] not in list(df_names[df]["TIMESTAMP"]):
                    break
                else:
                    time.append(test_start[t])
                    break
            if len(time)==1:
                if t ==0 or n == len(finder)-1:
                    time_start = test_start[t]
                else:
                    time_start = test_start[t-1]
                break


        time=[]
        for t in range(len(test_end)):
            for df in range(len(df_names)):
                if test_end[t] not in list(df_names[df]["TIMESTAMP"]):
                    break
                else:
                    time.append(test_end[t])
                    break
            if len(time)==1:
                if t ==0 or n == len(finder)-1:
                    time_end = test_end[t]
                else:
                    time_end = test_end[t-1]
                break
    print()
    print("Timestamp that can actually be used to trim due to gaps:")
    print("Start Time:", str(time_start), "| End Time:", str(time_end))
    
    return time_start, time_end