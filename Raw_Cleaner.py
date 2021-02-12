# -*- coding: utf-8 -*-
"""
Raw file cleaner 
"""
### Tools used for clean up
import numpy as np
import pandas as pd
import os


def file_to_df(path, file, f_skrow=4,h_skrow=1,raw=True):
    """
    This function takes in a path to a file then adds the correct headers 
    and makes the Timestamp the index and returns the dataframe
    
    inputs:
        path - directory of file
        file - wanted file to read in
        f_skrow - how many rows to skip for the data (default =  4)
        h_skrow - what row is the column names located
        raw - True if the timestamp is in the correct format
    output:
        df - pandas dataframe a usable format
    """    
    df = pd.read_csv(path+file, skiprows=f_skrow, na_values=['NAN', '00nan','000nan', "NaN"])
    headers_df = pd.read_csv(path+file,skiprows=h_skrow,nrows = 0)
    
    df.columns=list(headers_df)  #set the column names
    if raw:
        df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])
    
    return(df)

def cutter(df, time_start, time_end):
    """
    This function takes in a dataframe, start time, and endtime and uses it to 
    cut the dataframe 
    
    input: 
        df - dataframe with a "TIMESTAMP" index
        time_start - the time desired for the df to start 
                     (from time_stamp_matcher)
        time_end - the time desired for the df to end (from time_stamp_matcher)
    output:
        df- the trimmed dataframe with input time start/end
    """
    s_index = df.index[df["TIMESTAMP"]==time_start].tolist()[-1]
    e_index = df.index[df["TIMESTAMP"]==time_end].tolist()[0]
    
    df = df.truncate(before=s_index, after= e_index)
    df=df.reset_index(drop = True)
    
    return df

def day_trimmer(df, t_s, t_e):
    """
    Since the burns 20-34 have data loggers that contain multiple dates, we 
    the cutter function errors out. Therefor this is used to cut these files

    Parameters
    ----------
    df : pandas.DataFrame()
        Dataframe with with the "TIMESTAMP" column 
    t_s : string (pandas.TimeStamp)
        Desired starting time
    t_e : string (pandas.TimeStamp)
        Desired ending time

    Returns
    -------
    df : pandas.DataFrame()
        The output dataframe with the desired start and end time

    """
    
    mask =(df['TIMESTAMP'] >= t_s) & (df['TIMESTAMP'] <= t_e) 
    df=df.loc[mask]
    df=df.reset_index(drop = True)
    return df

def time_columns(df):
    """
    This function takes a df with a "TIMESTAMP" index/column and separates 
    the date and time atributes into different columns and creates a new df 
    with just the time columns separated
    
    input:
        df - pandas dataframe with "TIMESTAMP" index
        
    output:
        df_time - pandas dataframe with just the timestamp columns
    """
    #Created nan lists to append to quickly 
    fill_nan = np.nan
    year_lst  = list(np.full(len(df),fill_nan))
    month_lst = list(np.full(len(df),fill_nan))
    day_lst   = list(np.full(len(df),fill_nan))
    hour_lst  = list(np.full(len(df),fill_nan))
    min_lst   = list(np.full(len(df),fill_nan))
    second_lst= list(np.full(len(df),fill_nan))

    ### Parcing the timestamps and seperating them 
    time_stmp_lst = list(df["TIMESTAMP"].astype(str))
    for i in range(len(df)):
        time_step    = time_stmp_lst[i].replace("-", ",").replace(":",",").replace(" ",",").replace("/",',').split(",")
        year_lst[i]  = "{:.0f}".format(float(time_step[0])).zfill(4)
        month_lst[i] = "{:.0f}".format(float(time_step[1])).zfill(2)
        day_lst[i]   = "{:.0f}".format(float(time_step[2])).zfill(2)
        hour_lst[i]  = "{:.0f}".format(float(time_step[3])).zfill(2)
        min_lst[i]   = "{:.0f}".format(float(time_step[4])).zfill(2)
        second_lst[i]= "{:.1f}".format(float(time_step[5])).zfill(4)
    
    # add the lists to the DataFrame
    df_time = pd.DataFrame()
    df_time["YYYY"] = year_lst
    df_time["MM"]   = month_lst
    df_time["DD"]   = day_lst
    df_time["Hr"] = hour_lst
    df_time["Min"]  = min_lst
    df_time["Sec"]  = second_lst
    
    return df_time  


def continuous_df(df_raw, t_s, t_e, frequency = ".1S"):
    """
    This function takes a df and make sure that the timestamps are continous,
    If not it creates one that is, with NaN values is missing timestamps.
    
    Inputs:
        df_raw - the df with timestamestamps trimed 
        t_s - time that the df should start
        t_e - time that the df should end
        frequency - the frequency of the timestamps, def is 10Hz 
    
    Outputs:
        df/df_raw - the edited/non-edited continuous dataframe
    """
    
    t=list(pd.date_range(t_s, t_e, freq=frequency)) #timestamps wanted
    if len(t)==len(df_raw):
        print("There were 0 filled timestamps")
        
        return(df_raw)
    
    if len(t) != len(df_raw):
        col_order = list(df_raw.columns)
        df_raw = df_raw.set_index("TIMESTAMP") 


        df = pd.DataFrame(columns = list(df_raw.columns), index = t) #create NaN df
        df.update(df_raw) # Add the data onto NaN df


        df=df.reset_index(drop=True)
        df['TIMESTAMP']= t
        df = df[col_order]
        ### Show what was accomplished
        gaps = 0 
        for j in range(len(df)):
            if np.isnan(df["RECORD"][j]):
                gaps+=1
        print("There were",gaps,"filled timestamps")

        return df

def  initializing_df():
    
    """
    This function is to initialize the empty dataframes to append to and put
    here to minimize lines in the main file.
    """
    ### Sonic Headers that will be used (raw and output) 
    sonic_columns=["Ux_","Uy_","Uz_","Ts_", "diag_rmy_"]
    time_columns_lst=["YYYY","MM","DD","Hr","Min","Sec"]
    sonc_headers = ["U", "V", "W", "T", "DIAG"]
    
    ###Raw input headers and output headers for thermocouples
    t_c_lst_out = ["Temp_C(00cm)", "Temp_C(05cm)", "Temp_C(10cm)", \
       "Temp_C(20cm)", "Temp_C(30cm)", "Temp_C(50cm)", "Temp_C(100cm)"]
    t_c_lst_1 = ["Temp_C(1)", "Temp_C(2)", "Temp_C(3)", "Temp_C(4)",\
                  "Temp_C(5)", "Temp_C(6)", "Temp_C(7)"]
    t_c_lst_2 = ["Temp_C(8)", "Temp_C(9)", "Temp_C(10)", "Temp_C(11)",\
                 "Temp_C(12)", "Temp_C(13)", "Temp_C(14)"]
    
        ### Dataframe set up
    df_A1, df_A2, df_A3, df_A4 = pd.DataFrame(),pd.DataFrame(),pd.DataFrame(),pd.DataFrame()
    df_B1, df_B2, df_B3, df_B4 = pd.DataFrame(),pd.DataFrame(),pd.DataFrame(),pd.DataFrame()
    df_C1, df_C2, df_C3, df_C4 = pd.DataFrame(),pd.DataFrame(),pd.DataFrame(),pd.DataFrame()
    df_D1, df_D2, df_D3, df_D4 = pd.DataFrame(),pd.DataFrame(),pd.DataFrame(),pd.DataFrame()
    
    a_row_lst = [df_A1, df_A2, df_A3, df_A4]
    b_row_lst = [df_B1, df_B2, df_B3, df_B4]
    c_row_lst = [df_C1, df_C2, df_C3, df_C4]
    d_row_lst = [df_D1, df_D2, df_D3, df_D4 ]
    
    all_sonics = a_row_lst+ b_row_lst + c_row_lst +d_row_lst
    
    df_B1_tc, df_B2_tc, df_B3_tc, df_B4_tc = pd.DataFrame(),pd.DataFrame(),pd.DataFrame(),pd.DataFrame()
    df_C1_tc, df_C2_tc, df_C3_tc, df_C4_tc = pd.DataFrame(),pd.DataFrame(),pd.DataFrame(),pd.DataFrame()
    

    
    return sonic_columns, time_columns_lst, a_row_lst, b_row_lst, c_row_lst, \
        d_row_lst, sonc_headers, all_sonics, df_B1_tc, df_B2_tc, df_B3_tc, \
        df_B4_tc, df_C1_tc, df_C2_tc, df_C3_tc, df_C4_tc, t_c_lst_out, \
        t_c_lst_1, t_c_lst_2

def correction(all_sonics, all_tc_group, fill_nan, cor_lst, df_WGNover = "n"):
    """
    The funciton that put each file from the burns through a correction 
    funciton that removes data outside of the parameters set. 

    Parameters
    ----------
    all_sonics : list of pandas.DataFrame()'s
        The list of the soncis from A1-D4
    all_tc_group : list of pandas.DataFrame()'s
        The list of the thermocouple dataframes either B1-C4 or B1-C7, 
        depending on what burn is given.
    fill_nan : int/float/string 
        This is the value that is used replace the NaN values in the dataframe
    df_WGNover : pandas.DataFrame(), optional
        If there is a corresponding WGNover tower observation for the burn this 
        will be a pandas.Dataframe with that data, else it is a string, "n". 
        The default is "n".

    Returns
    -------
    all_sonics : list of pandas.DataFrame()'s
        The list of the soncis from A1-D4 CORRECTED
    all_tc_group : list of pandas.DataFrame()'s
        The list of the thermocouple dataframes either B1-C4 or B1-C7, 
        depending on what burn is given. CORRECTED
    df_WGNover : pandas.DataFrame(), optional
        If there is a corresponding WGNover tower observation for the burn this 
        will be a pandas.Dataframe with that data, else it is a string, "n". 
        The default is "n". CORRECTED

    """
    
    nam_snc=["A1", "A2", "A3", "A4", "B1", "B2", "B3", "B4", "C1","C2",\
                    "C3","C4","D1","D2","D3","D4"]
    if len(all_tc_group) == 14:
        nam_tc = ["B1", "B2", "B3", "B4","B5","B6","B7","C1","C2","C3","C4",\
                  "C5","C6","C7"]
    if len(all_tc_group) == 8:
        nam_tc = ["B1", "B2", "B3", "B4","C1","C2","C3","C4"]
    
    #Corrections
    m_speed, min_sn_T = cor_lst[0], cor_lst[1]
    max_sn_T, u_fctr = cor_lst[2], cor_lst[3]
    v_fctr, w_fctr = cor_lst[4], cor_lst[5]
    min_tc_T, max_tc_T = cor_lst[6], cor_lst[7]
    diag_check = cor_lst[8] 

    for df in range(len(all_sonics)):
        print("Sonic",nam_snc[df],":")
        all_sonics[df] = apply_sn_correction(all_sonics[df], u_fctr, v_fctr,\
                      w_fctr, m_speed, min_sn_T,max_sn_T, fill_nan,diag_check)
    
    for df in range(len(all_tc_group)):
        print("Thermocouple",nam_tc[df],":")
        all_tc_group[df] = apply_tc_correction(all_tc_group[df], min_tc_T, \
                               fill_nan, max_tc_T)
    
    if type(df_WGNover) is pd.DataFrame:
        print("WGNover tower:")
        df_WGNover = apply_sn_correction(df_WGNover,u_fctr,v_fctr,w_fctr,\
                             m_speed, min_sn_T,max_sn_T, fill_nan, diag_check)
    return all_sonics, all_tc_group, df_WGNover
    
def apply_sn_correction(df,u_fctr,v_fctr,w_fctr, m_speed,
                        min_sn_T,max_sn_T,fill_nan, diag_check):
    """
    This take a single pandas.Dataframe() and applys the parameter checks that 
    are inputed and replaces data not fittting these parameters with a desired
    NaN values and returns the data frame.

    Parameters
    ----------
    df : pandas.Dataframe()
        Dataframe with headers: Timecolumns, U, V, W, T 
    u_fctr : int or float
        To change the definition of wind to a meterology sense, we multiply
        by -1. If there is a tilt correction or other correction need to be 
        applied to all U value, this will take a number and multiply the column
    v_fctr : int or float 
        Same as u_fctr but for the V winds.
    w_fctr : int or float 
        Similar to the other *_fctr's but for the vertical wind speed. Default 
        is to not swap these columns. 
    m_speed : int or float (m/s)
        The max wind speed value desired, for 10x10 truss with RMY sonics the 
        instrument tollerence is |40m/s|
    min_sn_T : int or float (C)
        The minimun sonic temperature value desired, for 10x10 truss with RMY 
        sonics the instrument tollerence is -50C. Usually set to 0 since none
        of the 10x10 burns are on a day that hits that low of a temperature.
    max_sn_T : int or float (C)
        The maximum sonic temperature value desired, for 10x10 truss with RMY 
        sonics the instrument tollerence is +50C. Note that there are 
        observations that have a valid diagnostic code, but have a temperature
        above this threshold.
    fill_nan : int or float or string
        Desired NaN value for the dataset. 
    diag_check : "y" or "n"
        If "y" this will check the diag code in the data and remove all data at 
        that timestamp that is 0.0, this is from a RMY sonic manual that says 
        any code that is not 0.0 is invalid. The column will also be removed if
        "y".
        If "n" the diagnostic check will not be done and the DIAG column with 
        remain in the data.
    Returns
    -------
    df : pandas.Dataframe()
        The corrected dataframe so that it fits the parameters desired.

    """
    
    ##For loop for all the sonics
    if type(u_fctr) == int:
        df["U"] *= u_fctr
    if type(u_fctr) == float:
        df["U"] = [float(x)* u_fctr for x in df["U"]]
        
    if type(v_fctr) == int:
        df["V"] *= v_fctr
    if type(v_fctr) == float:
        df["V"] = [float(x)* v_fctr for x in df["V"]]
    
    if type(w_fctr) == int and w_fctr != 1:
        df["W"] *= w_fctr 
    if type(w_fctr) == float and int(w_fctr) != 1:
        df["W"] = [float(x)* w_fctr for x in df["W"]]
    
    count = 0
    for i in range(len(df)):
        if diag_check == "y":
            if df["DIAG"][i] != 0.0:
                df.at[i,["U","V","W","T"]] = fill_nan
                count += 4
                continue
        if df["U"][i] != "NaN":
            if df["U"][i] > m_speed or df["U"][i] < -1* m_speed:
                df.at[i, "U"] = fill_nan
                count += 1
        if df["V"][i] != "NaN":   
            if  df["V"][i]> m_speed or df["V"][i] < -1*m_speed:
                df.at[i, "V"] = fill_nan
                count += 1
        if df["W"][i] != "NaN":   
            if df['W'][i]> m_speed or df["W"][i] < -1*m_speed:
                df.at[i, "W"] = fill_nan
                count += 1
        if df["T"][i] != "NaN":  
            if df['T'][i] < min_sn_T or df["T"][i] > max_sn_T:
                df.at[i, "T"] = fill_nan
                count += 1
            
    if count ==0:
        print("Data fits these limits")
    if count != 0:
        print("Removed "+str(count)+" values" )
        
    df.fillna(value=fill_nan, inplace=True)
    if diag_check =="y":
        df = df.drop("DIAG", axis=1)
    return df

def apply_tc_correction(df, min_T, fill_nan, max_T = 10E6):
    """
    This function applys minimum and maximum temperature check that removes 
    values beyond that limit and replaces the nan value to the desired nan
    to the thermocouple truss df. 
    Parameters
    ----------
    df : pandas.Dataframe
        The dataframe containing the 7 thermal couples on the truss with the 
        time columns in the begining of the dataframes.
    min_T : int or float
        The minimum temperature check, if the temperature recorded is less than
        this, the value will be replaced by the nan value.
    fill_nan : int, float, or str
        The desired nan value set by the user
     min_T : int or float (Optional) 
        The max temperature check, if the temperature recorded is more than
        this, the value will be replaced by the nan value. TC's have a high 
        temperature tolerance, therefor the default is set to 10E6, if this is 
        not changed it will not perform that check. 
    Returns
    -------
    df : pandas.Dataframe
        The dataframe containing the 7 thermal couples on the truss with the 
        time columns in the begining of the dataframes with the min and max
        corrections.
    """
    
    if "YYYY" in df.columns: #seperated time columns
        tc_columns = list(df.columns[6:])
    if "TIMESTAMP" in df.columns: #single timestamp
        tc_columns = list(df.columns[1:])

    indx = []
    for i in range(len(df)):    
        for col in tc_columns:

            if float(df[col][i]) < min_T:
                df.at[i, col] = fill_nan
                indx.append(i)
            if max_T !=10E6:
                if float(df[col][i]) > max_T:
                    df.at[i, col] = fill_nan
                    indx.append(i)     
            
    if len(indx) ==0:
        print("Data fits these limits")
    if len(indx) != 0:
        print("Removed "+str(len(indx))+" Values" )
    
    df.fillna(value=fill_nan, inplace=True)
    
    return df

def saver(all_sonics, all_tc_group, df_WGNover, Burn, save_loc ="", \
          seperator = " ", file_type = ".txt"):
    """
    This funciton takes the sonics, thermocouples, and the WGNover 10m tower
    assuming they are in the correct order (SN: A1-D4 and TC: C1-D7* or D4*)
    and creates a burn dictionary with subdirectories for sonics and 
    thermalcouples. It won't overwrite the directories/files if you already
    have those in the location intended to save.
    Parameters
    ----------
    all_sonics : list of pandas.DataFrame()'s
        The list of the soncis from A1-D4
    all_tc_group : list of pandas.DataFrame()'s
        The list of the thermocouple dataframes either B1-C4 or B1-C7, 
        depending on what burn is given.
    df_WGNover : pandas.DataFrame()
        If there is a corresponding WGNover tower observation for the burn this 
        will be a pandas.Dataframe with that data, else it is a string, "n". 
        The default is "n".
    Burn : int
        The burn being processed, it is used in the naming of the files and 
        directories.
    save_loc : str
        This is the location to save the data out to. DEFAULT = "". The default 
        saves the data to the current working directory.
    Returns
    -------
    None.
    (Saves a data in current location or specified location)

    """
    ### Creating the directories to save the data
    if save_loc == "":
        cwd = os.getcwd()
    if save_loc != "":
        cwd = save_loc
        
    if "YYYY" in all_sonics[0].columns:
        tsp = "_SplitTimestamp"
    if "TIMESTAMP" in all_sonics[0].columns:
        tsp = "_FullTimestamp"
        
    save_dir = "Burn-"+str(Burn).zfill(2)+tsp
    os.mkdir(cwd+"/" + save_dir)
    
    tc_dir =cwd+"/" + save_dir + "/Thermalcouples"+tsp
    sonic_dir =cwd+"/" + save_dir + "/Sonics"+tsp
    
    os.mkdir(tc_dir)
    os.mkdir(sonic_dir)

    ### Save the Sonic data
    save_as_lst = ["A1", "A2", "A3", "A4", "B1", "B2", "B3", "B4", "C1","C2",\
                   "C3","C4","D1","D2","D3","D4"]
    for i in range(len(all_sonics)):
        sv_file = sonic_dir+'/'+save_as_lst[i]+"_UVWT_Burn-"+\
            str(Burn).zfill(2)+file_type
        all_sonics[i].to_csv(sv_file, sep=seperator,index=False)
    
    if type(df_WGNover) is pd.DataFrame:
        df_WGNover.to_csv(sonic_dir+'/WGNover_UVWT_Burn-'+str(Burn).zfill(2)+\
                          file_type,sep=seperator,index=False)

    ### Saving Thermal Couple dataframes
    if len(all_tc_group) == 14:
        save_as_lst= ["B1", "B2", "B3", "B4","B5","B6","B7","C1","C2","C3",\
                      "C4","C5","C6","C7"]
    if len(all_tc_group) == 8:
        save_as_lst = ["B1", "B2", "B3", "B4","C1","C2","C3","C4"]
    for i in range(len(all_tc_group)):
        all_tc_group[i] = all_tc_group[i].round(3)
        sv_file=tc_dir+'/'+save_as_lst[i]+"_ThermalCouple_Burn-" + \
            str(Burn).zfill(2)+file_type
        all_tc_group[i].to_csv(sv_file, sep=seperator,index=False)
    print("You now have the Burn "+str(Burn)+" sonics and thermocouple saved")
    
