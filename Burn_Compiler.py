# -*- coding: utf-8 -*-
"""
Created on Tue Jan 19 19:18:09 2021

@author: joeyp
"""

### Tools used for clean up
import pandas as pd
from Raw_Cleaner import  time_columns, file_to_df, cutter, \
    continuous_df, initializing_df, correction, saver
    
def master(base_path, Burn, save_loc,seperator,file_type, mk_contins, sep_time_cols, fill_nan, \
           mk_corcts, cor_lst):
    """
    Master takes all the functions and compilers and allows us to iterate each 
    burn and clean them to the set parameters that are fed into it. 

    Parameters
    ----------
    base_path : str 
        Location of the raw dataloggger files
    Burn : int
        Burn number desired to clean
    save_loc : str
        Save location of the burn cleaned. If save_loc = "", it will save in 
        the current directory
    mk_contins : str ('y'/'n')
        The argument to make the out data with continous timestamps. 
    sep_time_cols : str ('y'/'n')
        If "n" timestamp column will look like this: "2018-03-05 13:32:28.500"
        If "y" the columns will be split to this format: YYYY MM DD Hr Min Sec 
    fill_nan : str or int or float
        Value to replace all the nan values in the data
    mk_corcts : str ('y'/'n')
        If you have mk_corcts= "n" you do not need to change any of these below, 
        since the corrections will not be applied.
    cor_lst : list
         This is the list of correction parameters that will be applied if 
         mk_corcts = "y". What the list contains:
         [m_speed, min_sn_T, max_sn_T, u_fctr, v_fctr, w_fctr, min_tc_T, \
           max_tc_T, diag_check]
        

    Returns
    -------
    None.

    """
    print("Starting Burn",Burn)
    if Burn <=19:
        burn_path = base_path + "-"+str(Burn).zfill(2)+"/"
    if Burn >=20 and Burn <=22:
        burn_path = base_path + "s-20-to-22/"    
    if Burn >=23 and Burn <=26:
        burn_path = base_path + "s-23-to-26/"    
    if Burn >=27 and Burn <=30:
        burn_path = base_path + "s-27-to-30/"
    if Burn >=31 and Burn <=33:
        burn_path = base_path + "s-31-to-33/"
    if Burn >=34 and Burn <=35:
        burn_path = base_path + "s-34-to-35/"
    
    B_dict = {1:  ["2018-03-05 13:32:28.5", "2018-03-05 15:39:53.7"], \
              2:  ["2018-03-06 11:04:26.9", "2018-03-06 11:44:26.8"], \
              3:  ["2018-03-06 12:53:50.9", "2018-03-06 13:54:43.6"], \
              4:  ["2018-03-17 09:55:10.3", "2018-03-17 11:20:10.2"], \
              5:  ["2018-03-17 12:15:58.5", "2018-03-17 12:55:58.4"], \
              6:  ["2018-03-17 14:33:33.1", "2018-03-17 15:07:37.4"], \
              7:  ["2018-05-09 10:15:38.5", "2018-05-09 11:07:11.4"], \
              8:  ["2018-05-09 12:19:29.4", "2018-05-09 13:13:58.7"], \
              9:  ["2018-05-10 10:09:46.4", "2018-05-10 11:01:26.9"], \
              11: ["2018-05-11 08:51:20.7", "2018-05-11 10:09:05.1"], \
              12: ["2018-05-11 10:16:11.3", "2018-05-11 12:36:11.2"], \
              13: ["2018-05-11 12:40:17.4", "2018-05-11 14:30:17.3"], \
              18: ["2018-09-22 09:05:43.0", "2018-09-22 15:19:33.0"], \
              19: ["2018-09-22 15:26:16.0", "2018-09-22 18:25:21.0"], \
              20: ["2019-05-20 13:43:10.4", "2019-05-20 15:08:10.3"], \
              21: ["2019-05-20 15:24:07.6", "2019-05-20 17:04:07.5"], \
              22: ["2019-05-20 17:06:37.6", "2019-05-20 18:11:37.5"], \
              23: ["2019-05-21 10:52:39.7", "2019-05-21 13:07:39.6"], \
              24: ["2019-05-21 13:28:51.9", "2019-05-21 15:02:11.9"], \
              25: ["2019-05-21 15:02:12.0", "2019-05-21 16:38:51.9"], \
              26: ["2019-05-21 16:38:52.0", "2019-05-21 17:42:11.9"], \
              27: ["2019-05-22 09:47:38.2", "2019-05-22 11:32:38.1"], \
              28: ["2019-05-22 11:40:20.0", "2019-05-22 13:30:20.0"], \
              29: ["2019-05-22 13:30:20.1", "2019-05-22 14:55:20.0"], \
              30: ["2019-05-22 14:55:20.1", "2019-05-22 15:44:50.6"], \
              31: ["2019-05-29 10:00:00.0", "2019-05-29 12:10:00.0"], \
              32: ["2019-05-29 12:10:00.0", "2019-05-29 15:30:00.0"], \
              33: ["2019-05-29 15:30:00.1", "2019-05-29 16:36:33.9"], \
              34: ["2019-05-31 10:00:00.0", "2019-05-31 12:45:00.0"] }
    
    if Burn >0 and Burn <7:
        all_sonics, all_tc_group, df_WGNover = compiler1_6(burn_path, \
                   B_dict[Burn][0], B_dict[Burn][1], fill_nan, mk_contins, \
                       sep_time_cols)
    if Burn == 7 or Burn == 8:
        all_sonics, all_tc_group, df_WGNover = compiler7_8(burn_path, Burn, \
                   B_dict[Burn][0], B_dict[Burn][1], fill_nan, mk_contins, \
                       sep_time_cols) 
    
    if Burn >=9 and Burn <=13:
        all_sonics, all_tc_group, df_WGNover = compiler9_13(burn_path, Burn, \
                   B_dict[Burn][0], B_dict[Burn][1], fill_nan, mk_contins, \
                       sep_time_cols) 
    if Burn == 18 or Burn == 19:
        all_sonics, all_tc_group, df_WGNover = compiler18_19(burn_path, \
                   B_dict[Burn][0], B_dict[Burn][1], fill_nan, mk_contins, \
                       sep_time_cols)
    if Burn >19 and Burn <35:
        all_sonics, all_tc_group, df_WGNover = compiler20_35(burn_path, \
                   B_dict[Burn][0], B_dict[Burn][1], fill_nan, mk_contins, \
                       sep_time_cols)
    
    if mk_corcts =="y":
        all_sonics, all_tc_group, df_WGNover = correction(all_sonics, \
                    all_tc_group, fill_nan, cor_lst, df_WGNover)
    
    saver(all_sonics, all_tc_group, df_WGNover, Burn, save_loc, seperator,\
          file_type)
    print()


def compiler1_6(path, t_s, t_e, fill_nan, mk_contins = "y", sep_time_cols = "y"):
    """
    This compiler is used to extract the data from the raw data loggers, cut 
    each to the same starting and ending time, make the data a continous time
    column and fill nan values. Use this for 10x10m SERDP Burns 1-6

    Parameters
    ----------
    path : str
       Location of the Burn directory containing the raw datalogger files
    t_s : str or (pandas.Timestamp())
        starting timestamp that all the loggers contain and after the 
        repeated timestamps during datalogger's start-up
    t_e : str or (pandas.Timestamp())
        ending timestamp that all the data loggers contain
    fill_nan : int or str or float (np.nan also exceptable)
        the desired value to replace the NaN values
    mk_contins : "y" or "n", optional
        This is an option to make the dataframes with a continous timestamp 
        column filled with the desired nan value. 
        The default is "y" (to make the timestamps continuous).
    sep_time_cols : "y" or "n", optional
        This is an option to have a seperated timestamp column or a single
        timestamp column. If the output files will be loaded into excel, it's 
        recommended to have them seperated ("y"). The default is "y".
        
        HEADER option:
        "TIMESTAMP" ("n") or "YYYY", "MM","DD","Hr","Min","Sec" ("y")
    Returns
    -------
    all_sonics : list of pandas.DataFrame()'s
       This is the list of the output sonics, A1 through D4.
    all_tc_group : list of pandas.DataFrame()'s
        This is the list of thermocouples, B1-C4
    df_WGNover : pandas.DataFrame()
        This is the observational tower located outside the 10x10m truss.
    """

    files = ["TOA5_2878.WGNover10Hz.dat","TOA5_2879.ts_data.dat",\
             "TOA5_4390.ts_data.dat","TOA5_4975.ts_data.dat",\
             "TOA5_4976.ts_data.dat","TOA5_10442.ts_data.dat",\
             "TOA5_11585.ts_data.dat"]

    ### First Loading the files into the script
    df_2878, df_2879 = file_to_df(path, files[0]), file_to_df(path, files[1])
    df_4390, df_4975 = file_to_df(path, files[2]), file_to_df(path, files[3])
    df_4976, df_10442 = file_to_df(path, files[4]), file_to_df(path, files[5])
    df_11585 = file_to_df(path, files[6])

    
    if mk_contins.lower() != str("y"):      
        df_2878 = cutter(df_2878, t_s, t_e)
        df_2879 = cutter(df_2879, t_s, t_e)
        df_4390 = cutter(df_4390, t_s, t_e)
        df_4975 = cutter(df_4975, t_s, t_e)
        df_4976 = cutter(df_4976, t_s, t_e)
        df_10442 =cutter(df_10442, t_s, t_e)
        df_11585 = cutter(df_11585, t_s, t_e)
        
    if mk_contins.lower()  == "y":     
        fmt= "Datalogger {}:"
        print(fmt.format(files[0].split(".")[0].split("_")[1]))
        df_2878 = continuous_df(cutter(df_2878, t_s, t_e),  t_s, t_e)
        print(fmt.format(files[1].split(".")[0].split("_")[1]))
        df_2879 = continuous_df(cutter(df_2879, t_s, t_e),  t_s, t_e)
        print(fmt.format(files[2].split(".")[0].split("_")[1]))
        df_4390 = continuous_df(cutter(df_4390, t_s, t_e),  t_s, t_e)
        print(fmt.format(files[3].split(".")[0].split("_")[1]))
        df_4975 = continuous_df(cutter(df_4975, t_s, t_e),  t_s, t_e)
        print(fmt.format(files[4].split(".")[0].split("_")[1]))
        df_4976 = continuous_df(cutter(df_4976, t_s, t_e),  t_s, t_e)
        print(fmt.format(files[5].split(".")[0].split("_")[1]))
        df_10442 =continuous_df(cutter(df_10442, t_s, t_e), t_s, t_e)
        print(fmt.format(files[6].split(".")[0].split("_")[1]))
        df_11585 = continuous_df(cutter(df_11585, t_s, t_e),t_s, t_e)
        
 
    sonic_columns, time_columns_lst, a_row_lst, b_row_lst, c_row_lst, \
    d_row_lst, sonc_headers, df_B1_tc, df_B2_tc, df_B3_tc, \
    df_B4_tc, df_C1_tc, df_C2_tc, df_C3_tc, df_C4_tc, t_c_lst_out, \
    t_c_lst_1, t_c_lst_2= initializing_df()

    df_WGNover = pd.DataFrame()
    if sep_time_cols.lower() == "y":
        time_columns_lst=["YYYY","MM","DD","Hr","Min","Sec"]
        df_4975_time, df_2879_time = time_columns(df_4975), time_columns(df_2879)
        df_11585_time, df_4976_time = time_columns(df_11585), time_columns(df_4976)
        ### WG Nover 10hz
        df_2878_time = time_columns(df_2878)
        for t in time_columns_lst:
            df_WGNover[t] = df_2878_time[t]
    if sep_time_cols.lower() != "y":
        time_columns_lst = ["TIMESTAMP"]
        df_WGNover["TIMESTAMP"] = df_2878["TIMESTAMP"]
    
    
    for col in range(len(sonic_columns)):
        df_WGNover[sonc_headers[col]] = df_2878[sonic_columns[col]+"1"]
    
    for n in range(len(a_row_lst)):
        if sep_time_cols.lower() == "y":
            for i in range(len(time_columns_lst)):
                a_row_lst[n][time_columns_lst[i]]=df_4975_time[time_columns_lst[i]]
                b_row_lst[n][time_columns_lst[i]]=df_2879_time[time_columns_lst[i]]
                c_row_lst[n][time_columns_lst[i]]=df_11585_time[time_columns_lst[i]]
                d_row_lst[n][time_columns_lst[i]]=df_4976_time[time_columns_lst[i]]
        
        if sep_time_cols.lower() != "y":
            a_row_lst[n]["TIMESTAMP"]=df_4975["TIMESTAMP"]
            b_row_lst[n]["TIMESTAMP"]=df_2879["TIMESTAMP"]
            c_row_lst[n]["TIMESTAMP"]=df_11585["TIMESTAMP"]
            d_row_lst[n]["TIMESTAMP"]=df_4976["TIMESTAMP"]
        
        for i in range(len(sonic_columns)):
            a_row_lst[n][sonc_headers[i]] = df_4975[sonic_columns[i]+str(n+1)]
            b_row_lst[n][sonc_headers[i]] = df_2879[sonic_columns[i]+str(n+1)]
            c_row_lst[n][sonc_headers[i]] = df_11585[sonic_columns[i]+str(n+1)]
            d_row_lst[n][sonc_headers[i]] = df_4976[sonic_columns[i]+str(n+1)]
    
    all_sonics = a_row_lst+ b_row_lst + c_row_lst +d_row_lst
    ####################### Thermalcouples ##################################

        
    first_tc_group = [ df_B1_tc, df_B3_tc, df_C1_tc, df_C3_tc]
    secnd_tc_group = [ df_B2_tc, df_B4_tc, df_C2_tc, df_C4_tc]    
    df_tc_lst= [df_10442, df_2879, df_4390, df_11585]
    
    if sep_time_cols.lower() == "y":
        
        df_10442_time, df_4390_time = time_columns(df_10442), time_columns(df_4390)
        df_time_lst = [df_10442_time, df_2879_time, df_4390_time, df_11585_time] 
        
        for j in range(len(first_tc_group)):
            for t in range(len(time_columns_lst)):
                first_tc_group[j][time_columns_lst[t]]= df_time_lst[j][time_columns_lst[t]]
                secnd_tc_group[j][time_columns_lst[t]]= df_time_lst[j][time_columns_lst[t]]
    
    
    if sep_time_cols.lower() != "y":
    
        for j in range(len(first_tc_group)):
            first_tc_group[j]["TIMESTAMP"]= df_tc_lst[j]["TIMESTAMP"]
            secnd_tc_group[j]["TIMESTAMP"]= df_tc_lst[j]["TIMESTAMP"]     
            
    
    for j in range(len(first_tc_group)):
        for i in range(len(t_c_lst_1)):
            first_tc_group[j][t_c_lst_out[i]]= df_tc_lst[j][t_c_lst_1[i]]
            secnd_tc_group[j][t_c_lst_out[i]]= df_tc_lst[j][t_c_lst_2[i]]
        
        
    all_tc_group = [df_B1_tc, df_B2_tc, df_B3_tc, df_B4_tc,\
                    df_C1_tc, df_C2_tc, df_C3_tc, df_C4_tc ]

    for df in range(len(all_sonics)):
        all_sonics[df].fillna(value=fill_nan, inplace=True)
    for df in range(len(all_tc_group)):
        all_tc_group[df].fillna(value=fill_nan, inplace=True)

    df_WGNover.fillna(value=fill_nan, inplace=True)
    
    return all_sonics, all_tc_group, df_WGNover
        
        
def compiler7_8(path, Burn, t_s, t_e, fill_nan, mk_contins = "y", sep_time_cols = "y"):
    """
    This compiler is used to extract the data from the raw data loggers, cut 
    each to the same starting and ending time, make the data a continous time
    column and fill nan values. Use this for 10x10m SERDP Burns 7 and 8 where 
    there are two stacked trusses with two rows of sonics upsidedown.

    Parameters
    ----------
    path : str
       Location of the Burn directory containing the raw datalogger files
    t_s : str or (pandas.Timestamp())
        starting timestamp that all the loggers contain and after the 
        repeated timestamps during datalogger's start-up
    t_e : str or (pandas.Timestamp())
        ending timestamp that all the data loggers contain
    fill_nan : int or str or float (np.nan also exceptable)
        the desired value to replace the NaN values
    mk_contins : "y" or "n", optional
        This is an option to make the dataframes with a continous timestamp 
        column filled with the desired nan value. 
        The default is "y" (to make the timestamps continuous).
    sep_time_cols : "y" or "n", optional
        This is an option to have a seperated timestamp column or a single
        timestamp column. If the output files will be loaded into excel, it's 
        recommended to have them seperated ("y"). The default is "y".
        
        HEADER option:
        "TIMESTAMP" ("n") or "YYYY", "MM","DD","Hr","Min","Sec" ("y")
    Returns
    -------
    all_sonics : list of pandas.DataFrame()'s
       This is the list of the output sonics, A1 through D4.
    all_tc_group : list of pandas.DataFrame()'s
        This is the list of thermocouples, B1-C7
    df_WGNover : pandas.DataFrame()
        This is the observational tower located outside the 10x10m truss.
    """
    files = ["TOA5_2879.ts_data.dat",\
             "TOA5_3884.ts_data.dat", "TOA5_4390.ts_data.dat",\
             "TOA5_4975.ts_data.dat","TOA5_4976.ts_data.dat",
             "TOA5_10442.ts_data.dat", "TOA5_11584.ts_data.dat",\
             "TOA5_11585.ts_data.dat"]

    ### First Loading the files into the script
    df_2879, df_3884 = file_to_df(path, files[0]), file_to_df(path, files[1])
    df_4390, df_4975 = file_to_df(path, files[2]), file_to_df(path, files[3])
    df_4976, df_10442 = file_to_df(path, files[4]), file_to_df(path, files[5])
    df_11584, df_11585 = file_to_df(path, files[6]), file_to_df(path, files[7])
    
    if mk_contins.lower() != str("y"):      
        df_2879 = cutter(df_2879, t_s, t_e)
        df_3884 = cutter(df_3884, t_s, t_e)
        df_4390 = cutter(df_4390, t_s, t_e)
        df_4975 = cutter(df_4975, t_s, t_e)
        df_4976 = cutter(df_4976, t_s, t_e)
        df_10442 = cutter(df_10442, t_s, t_e)
        df_11584 = cutter(df_11584, t_s, t_e)
        df_11585 = cutter(df_11585, t_s, t_e)
        
    if mk_contins.lower()  == "y":     
        fmt= "Datalogger {}:"
        print(fmt.format(files[0].split(".")[0].split("_")[1]))
        df_2879 = continuous_df(cutter(df_2879, t_s, t_e), t_s, t_e)
        print(fmt.format(files[1].split(".")[0].split("_")[1]))
        df_3884 = continuous_df(cutter(df_3884, t_s, t_e), t_s, t_e)
        print(fmt.format(files[2].split(".")[0].split("_")[1]))
        df_4390 = continuous_df(cutter(df_4390, t_s, t_e), t_s, t_e)
        print(fmt.format(files[3].split(".")[0].split("_")[1]))
        df_4975 = continuous_df(cutter(df_4975, t_s, t_e), t_s, t_e)
        print(fmt.format(files[4].split(".")[0].split("_")[1]))
        df_4976 = continuous_df(cutter(df_4976, t_s, t_e), t_s, t_e)
        print(fmt.format(files[5].split(".")[0].split("_")[1]))
        df_10442 =continuous_df(cutter(df_10442, t_s, t_e), t_s, t_e)
        print(fmt.format(files[6].split(".")[0].split("_")[1]))
        df_11584 = continuous_df(cutter(df_11584, t_s, t_e), t_s, t_e)
        print(fmt.format(files[7].split(".")[0].split("_")[1]))
        df_11585 = continuous_df(cutter(df_11585, t_s, t_e), t_s, t_e)
    
    ### Initialized the list and dataframes to append to    
    sonic_columns, time_columns_lst, a_row_lst, b_row_lst, c_row_lst, \
    d_row_lst, sonc_headers, df_B1_tc, df_B2_tc, df_B3_tc, \
    df_B4_tc, df_C1_tc, df_C2_tc, df_C3_tc, df_C4_tc, t_c_lst_out, \
    t_c_lst_1, t_c_lst_2= initializing_df()
    
    cor_col = ["Ux_1", "Ux_2", "Ux_3", "Ux_4", "Uz_1","Uz_2","Uz_3","Uz_4"]
    
    #cor_col_W =["Ux_4", "Uz_1","Uz_2","Uz_3","Uz_4"]
    for i in range(len(cor_col)):
        df_4975[cor_col[i]] *= -1 #A Truss
        df_11584[cor_col[i]] *= -1 #C Truss 
        #df_10442 #B Truss
        #df_4390  #D Truss
    if sep_time_cols.lower() == "y":
        time_columns_lst=["YYYY","MM","DD","Hr","Min","Sec"]
        df_4975_time, df_2879_time = time_columns(df_4975), time_columns(df_2879)
        df_11585_time, df_4976_time = time_columns(df_11585), time_columns(df_4976)
        df_3884_time, df_11584_time = time_columns(df_3884), time_columns(df_11584)
        df_10442_time, df_4390_time = time_columns(df_10442), time_columns(df_4390)
        
    if sep_time_cols.lower() != "y":
        time_columns_lst = ["TIMESTAMP"]
    
    
    for n in range(len(a_row_lst)):
        if sep_time_cols.lower() == "y":
            for i in range(len(time_columns_lst)):
                a_row_lst[n][time_columns_lst[i]]=df_4975_time[time_columns_lst[i]]
                b_row_lst[n][time_columns_lst[i]]=df_10442_time[time_columns_lst[i]]
                c_row_lst[n][time_columns_lst[i]]=df_11584_time[time_columns_lst[i]]
                d_row_lst[n][time_columns_lst[i]]=df_4390_time[time_columns_lst[i]]
        
        if sep_time_cols.lower() != "y":
            a_row_lst[n]["TIMESTAMP"]=df_4975["TIMESTAMP"]
            b_row_lst[n]["TIMESTAMP"]=df_10442["TIMESTAMP"]
            c_row_lst[n]["TIMESTAMP"]=df_11584["TIMESTAMP"]
            d_row_lst[n]["TIMESTAMP"]=df_4390["TIMESTAMP"]
        
        for i in range(len(sonic_columns)):
            a_row_lst[n][sonc_headers[i]] = df_4975[sonic_columns[i]+str(n+1)]
            b_row_lst[n][sonc_headers[i]] = df_10442[sonic_columns[i]+str(n+1)]
            c_row_lst[n][sonc_headers[i]] = df_11584[sonic_columns[i]+str(n+1)]
            d_row_lst[n][sonc_headers[i]] = df_4390[sonic_columns[i]+str(n+1)]
    
    all_sonics = a_row_lst+ b_row_lst + c_row_lst +d_row_lst
    
    
    ####################### Thermalcouples ##################################

    df_B5_tc, df_B6_tc, df_B7_tc = pd.DataFrame(),pd.DataFrame(),pd.DataFrame()
    df_C5_tc, df_C6_tc, df_C7_tc = pd.DataFrame(),pd.DataFrame(),pd.DataFrame()
      
    first_tc_group = [ df_B2_tc, df_B3_tc, df_B6_tc, df_B7_tc,\
                       df_C2_tc, df_C3_tc, df_C6_tc, df_C7_tc]
    
    secnd_tc_group = [ df_B1_tc, df_B4_tc, df_B5_tc,\
                      df_C1_tc, df_C4_tc, df_C5_tc]
    df_tc_lst_1 = [df_4975, df_2879, df_3884, df_10442, df_11585, df_4976,\
                df_4390, df_11584]
    df_tc_lst_2 = [df_2879, df_4975, df_10442, df_4976, df_11585, df_11584]    
    
    if sep_time_cols.lower() == "y":
        ### Order that data fits the tc array
        df_time_lst_1 =[df_4975_time, df_2879_time, df_3884_time, df_10442_time,\
                    df_11585_time, df_4976_time, df_4390_time, df_11584_time]
        df_time_lst_2 = [df_2879_time, df_4975_time, df_10442_time, df_4976_time,\
                    df_11585_time, df_11584_time]
         
        ### Adding the time split time columns   
        for j in range(len(first_tc_group)):
            for t in range(len(time_columns_lst)):
                first_tc_group[j][time_columns_lst[t]]= df_time_lst_1[j][time_columns_lst[t]]
           
        for j in range(len(secnd_tc_group)):
            for t in range(len(time_columns_lst)):
                secnd_tc_group[j][time_columns_lst[t]]= df_time_lst_2[j][time_columns_lst[t]]
    
    ### Adding the timestamp column to the dataframe
    if sep_time_cols.lower() != "y":
        for j in range(len(first_tc_group)):
            first_tc_group[j]["TIMESTAMP"]= df_tc_lst_1[j]["TIMESTAMP"]   
        for j in range(len(secnd_tc_group)):
                secnd_tc_group[j]["TIMESTAMP"]= df_tc_lst_2[j]["TIMESTAMP"]    
   
    ###Adding the data to the df
    for j in range(len(first_tc_group)):
        for i in range(len(t_c_lst_1)):
            first_tc_group[j][t_c_lst_1[i]]= df_tc_lst_1[j][t_c_lst_1[i]]
    for j in range(len(secnd_tc_group)):
        for i in range(len(t_c_lst_2)):
            secnd_tc_group[j][t_c_lst_2[i]]= df_tc_lst_2[j][t_c_lst_2[i]]
    
    all_tc_group = [df_B1_tc, df_B2_tc, df_B3_tc, df_B4_tc, df_B5_tc,\
                    df_B6_tc, df_B7_tc,df_C1_tc, df_C2_tc, df_C3_tc, df_C4_tc,\
                    df_C5_tc, df_C6_tc, df_C7_tc]

    for df in range(len(all_sonics)):
        all_sonics[df].fillna(value=fill_nan, inplace=True)
    for df in range(len(all_tc_group)):
        all_tc_group[df].fillna(value=fill_nan, inplace=True)
    
    ###Observation tower for burn 8
    if Burn == 8:
        df_2878 = file_to_df(path,"TOA5_2878.WGNover10Hz.dat" )
        df_WGNover = pd.DataFrame()
        if mk_contins.lower() != str("y"):      
            df_2878 = cutter(df_2878, t_s, t_e)
        
        if mk_contins.lower()  == "y":     
            df_2878 = continuous_df(cutter(df_2878, t_s, t_e), t_s, t_e)
        
        if sep_time_cols.lower() == "y":
            df_2878_time = time_columns(df_2878)
            for t in time_columns_lst:
                df_WGNover[t] = df_2878_time[t]
        
        if sep_time_cols.lower() != "y":
            df_WGNover["TIMESTAMP"] = df_2878["TIMESTAMP"]
        
        for col in range(len(sonic_columns)):
            df_WGNover[sonc_headers[col]] = df_2878[sonic_columns[col]+"1"]
                
        df_WGNover.fillna(value=fill_nan, inplace=True)
    
    if Burn != 8: 
        df_WGNover = "n"
    
    return all_sonics, all_tc_group, df_WGNover      


def compiler9_13(path, Burn, t_s, t_e, fill_nan, mk_contins = "y", sep_time_cols = "y"):
    """
    This compiler is used to extract the data from the raw data loggers, cut 
    each to the same starting and ending time, make the data a continous time
    column and fill nan values. Use this for 10x10m SERDP Burns 9,11-13 with 
    9 and 13 having the WGNover tower.

    Parameters
    ----------
    path : str
       Location of the Burn directory containing the raw datalogger files
    t_s : str or (pandas.Timestamp())
        starting timestamp that all the loggers contain and after the 
        repeated timestamps during datalogger's start-up
    t_e : str or (pandas.Timestamp())
        ending timestamp that all the data loggers contain
    fill_nan : int or str or float (np.nan also exceptable)
        the desired value to replace the NaN values
    mk_contins : "y" or "n", optional
        This is an option to make the dataframes with a continous timestamp 
        column filled with the desired nan value. 
        The default is "y" (to make the timestamps continuous).
    sep_time_cols : "y" or "n", optional
        This is an option to have a seperated timestamp column or a single
        timestamp column. If the output files will be loaded into excel, it's 
        recommended to have them seperated ("y"). The default is "y".
        
        HEADER option:
        "TIMESTAMP" ("n") or "YYYY", "MM","DD","Hr","Min","Sec" ("y")
    Returns
    -------
    all_sonics : list of pandas.DataFrame()'s
       This is the list of the output sonics, A1 through D4.
    all_tc_group : list of pandas.DataFrame()'s
        This is the list of thermocouples, B1-C7
    df_WGNover : pandas.DataFrame()
        This is the observational tower located outside the 10x10m truss.
    """
    files = ["TOA5_2879.ts_data.dat",\
             "TOA5_3884.ts_data.dat", "TOA5_4390.ts_data.dat",\
             "TOA5_4975.ts_data.dat","TOA5_4976.ts_data.dat",
             "TOA5_10442.ts_data.dat", "TOA5_11584.ts_data.dat",\
             "TOA5_11585.ts_data.dat"]

    ### First Loading the files into the script
    df_2879 = file_to_df(path, files[0])
    df_3884, df_4390 = file_to_df(path, files[1]), file_to_df(path, files[2])
    df_4975, df_4976 = file_to_df(path, files[3]), file_to_df(path, files[4])
    df_10442 = file_to_df(path,files[5])
    df_11584, df_11585 = file_to_df(path, files[6]), file_to_df(path, files[7])
    
    if mk_contins.lower() != str("y"):      
        df_2879 = cutter(df_2879, t_s, t_e)
        df_3884 = cutter(df_3884, t_s, t_e)
        df_4390 = cutter(df_4390, t_s, t_e)
        df_4975 = cutter(df_4975, t_s, t_e)
        df_4976 = cutter(df_4976, t_s, t_e)
        df_10442 = cutter(df_10442, t_s, t_e)
        df_11584 = cutter(df_11584, t_s, t_e)
        df_11585 = cutter(df_11585, t_s, t_e)
        
    if mk_contins.lower()  == "y":     
        fmt= "Datalogger {}:"
        print(fmt.format(files[0].split(".")[0].split("_")[1]))
        df_2879 = continuous_df(cutter(df_2879, t_s, t_e), t_s, t_e)
        print(fmt.format(files[1].split(".")[0].split("_")[1]))
        df_3884 = continuous_df(cutter(df_3884, t_s, t_e), t_s, t_e)
        print(fmt.format(files[2].split(".")[0].split("_")[1]))
        df_4390 = continuous_df(cutter(df_4390, t_s, t_e), t_s, t_e)
        print(fmt.format(files[3].split(".")[0].split("_")[1]))
        df_4975 = continuous_df(cutter(df_4975, t_s, t_e), t_s, t_e)
        print(fmt.format(files[4].split(".")[0].split("_")[1]))
        df_4976 =continuous_df(cutter(df_4976, t_s, t_e), t_s, t_e)
        print(fmt.format(files[5].split(".")[0].split("_")[1]))
        df_10442 = continuous_df(cutter(df_10442, t_s, t_e), t_s, t_e)
        print(fmt.format(files[6].split(".")[0].split("_")[1]))
        df_11584 = continuous_df(cutter(df_11584, t_s, t_e), t_s, t_e)
        print(fmt.format(files[7].split(".")[0].split("_")[1]))
        df_11585 = continuous_df(cutter(df_11585, t_s, t_e), t_s, t_e)
    
    ### Initialized the list and dataframes to append to    
    sonic_columns, time_columns_lst, a_row_lst, b_row_lst, c_row_lst, \
    d_row_lst, sonc_headers, df_B1_tc, df_B2_tc, df_B3_tc, \
    df_B4_tc, df_C1_tc, df_C2_tc, df_C3_tc, df_C4_tc, t_c_lst_out, \
    t_c_lst_1, t_c_lst_2= initializing_df()
    
    if sep_time_cols.lower() == "y":
        time_columns_lst=["YYYY","MM","DD","Hr","Min","Sec"]
        df_2879_time = time_columns(df_2879)
        df_3884_time, df_4390_time = time_columns(df_3884), time_columns(df_4390)
        df_4975_time, df_4976_time = time_columns(df_4975), time_columns(df_4976)
        df_10442_time = time_columns(df_10442)
        df_11584_time, df_11585_time = time_columns(df_11584), time_columns(df_11585)
        
    if sep_time_cols.lower() != "y":
        time_columns_lst = ["TIMESTAMP"]
    
    
    for n in range(len(a_row_lst)):
        if sep_time_cols.lower() == "y":
            for i in range(len(time_columns_lst)):
                a_row_lst[n][time_columns_lst[i]]=df_10442_time[time_columns_lst[i]]
                b_row_lst[n][time_columns_lst[i]]=df_4975_time[time_columns_lst[i]]
                c_row_lst[n][time_columns_lst[i]]=df_11584_time[time_columns_lst[i]]
                d_row_lst[n][time_columns_lst[i]]=df_4390_time[time_columns_lst[i]]
        
        if sep_time_cols.lower() != "y":
            a_row_lst[n]["TIMESTAMP"]=df_10442["TIMESTAMP"]
            b_row_lst[n]["TIMESTAMP"]=df_4975["TIMESTAMP"]
            c_row_lst[n]["TIMESTAMP"]=df_11584["TIMESTAMP"]
            d_row_lst[n]["TIMESTAMP"]=df_4390["TIMESTAMP"]
        
        for i in range(len(sonic_columns)):
            a_row_lst[n][sonc_headers[i]] = df_10442[sonic_columns[i]+str(n+1)]
            b_row_lst[n][sonc_headers[i]] = df_4975[sonic_columns[i]+str(n+1)]
            c_row_lst[n][sonc_headers[i]] = df_11584[sonic_columns[i]+str(n+1)]
            d_row_lst[n][sonc_headers[i]] = df_4390[sonic_columns[i]+str(n+1)]
    
    all_sonics = a_row_lst+ b_row_lst + c_row_lst +d_row_lst
    
    
    ####################### Thermalcouples ##################################

    df_B5_tc, df_B6_tc, df_B7_tc = pd.DataFrame(),pd.DataFrame(),pd.DataFrame()
    df_C5_tc, df_C6_tc, df_C7_tc = pd.DataFrame(),pd.DataFrame(),pd.DataFrame()
      
    first_tc_group = [ df_B2_tc, df_B3_tc, df_B6_tc, df_B7_tc,\
                       df_C2_tc, df_C3_tc, df_C6_tc, df_C7_tc]
    
    secnd_tc_group = [ df_B1_tc, df_B4_tc, df_B5_tc,\
                      df_C1_tc, df_C4_tc, df_C5_tc]
    df_tc_lst_1 = [df_10442, df_2879, df_3884, df_4975, df_11585, df_4976,\
                df_4390, df_11584]
    df_tc_lst_2 = [df_2879, df_10442, df_4975, df_4976, df_11585, df_11584]    
    
    if sep_time_cols.lower() == "y":
        ### Order that data fits the tc array
        df_time_lst_1 =[df_10442_time, df_2879_time, df_3884_time, df_4975_time,\
                    df_11585_time, df_4976_time, df_4390_time, df_11584_time]
        df_time_lst_2 = [df_2879_time, df_10442_time, df_4975_time, df_4976_time,\
                    df_11585_time, df_11584_time]
         
        ### Adding the time split time columns   
        for j in range(len(first_tc_group)):
            for t in range(len(time_columns_lst)):
                first_tc_group[j][time_columns_lst[t]]= df_time_lst_1[j][time_columns_lst[t]]
           
        for j in range(len(secnd_tc_group)):
            for t in range(len(time_columns_lst)):
                secnd_tc_group[j][time_columns_lst[t]]= df_time_lst_2[j][time_columns_lst[t]]
    
    ### Adding the timestamp column to the dataframe
    if sep_time_cols.lower() != "y":
        for j in range(len(first_tc_group)):
            first_tc_group[j]["TIMESTAMP"]= df_tc_lst_1[j]["TIMESTAMP"]   
        for j in range(len(secnd_tc_group)):
                secnd_tc_group[j]["TIMESTAMP"]= df_tc_lst_2[j]["TIMESTAMP"]    
   
    ###Adding the data to the df
    for j in range(len(first_tc_group)):
        for i in range(len(t_c_lst_1)):
            first_tc_group[j][t_c_lst_1[i]]= df_tc_lst_1[j][t_c_lst_1[i]]
    for j in range(len(secnd_tc_group)):
        for i in range(len(t_c_lst_2)):
            secnd_tc_group[j][t_c_lst_2[i]]= df_tc_lst_2[j][t_c_lst_2[i]]
    
    all_tc_group = [df_B1_tc, df_B2_tc, df_B3_tc, df_B4_tc, df_B5_tc,\
                    df_B6_tc, df_B7_tc,df_C1_tc, df_C2_tc, df_C3_tc, df_C4_tc,\
                    df_C5_tc, df_C6_tc, df_C7_tc]

    for df in range(len(all_sonics)):
        all_sonics[df].fillna(value=fill_nan, inplace=True)
    for df in range(len(all_tc_group)):
        all_tc_group[df].fillna(value=fill_nan, inplace=True)
    
    ###Observation tower for burn 8
    if Burn == 9 or Burn == 13:
        df_2878 = file_to_df(path,"TOA5_2878.WGNover10Hz.dat" )
        df_WGNover = pd.DataFrame()
        if mk_contins.lower() != str("y"):      
            df_2878 = cutter(df_2878, t_s, t_e)
        
        if mk_contins.lower()  == "y":     
            df_2878 = continuous_df(cutter(df_2878, t_s, t_e), t_s, t_e)
        
        if sep_time_cols.lower() == "y":
            df_2878_time = time_columns(df_2878)
            for t in time_columns_lst:
                df_WGNover[t] = df_2878_time[t]
        
        if sep_time_cols.lower() != "y":
            df_WGNover["TIMESTAMP"] = df_2878["TIMESTAMP"]
        
        for col in range(len(sonic_columns)):
            df_WGNover[sonc_headers[col]] = df_2878[sonic_columns[col]+"1"]
                
        df_WGNover.fillna(value=fill_nan, inplace=True)
    
    if Burn == 11 or Burn == 12: 
        df_WGNover = "n"
    
    return all_sonics, all_tc_group, df_WGNover   




def compiler18_19(path, t_s, t_e, fill_nan, mk_contins = "y", \
                  sep_time_cols = "y"):
    """
    This compiler is used to extract the data from the raw data loggers, cut 
    each to the same starting and ending time, make the data a continous time
    column and fill nan values. Use this for 10x10m SERDP Burns 18 and 19.

    Parameters
    ----------
    path : str
       Location of the Burn directory containing the raw datalogger files
    t_s : str or (pandas.Timestamp())
        starting timestamp that all the loggers contain and after the 
        repeated timestamps during datalogger's start-up
    t_e : str or (pandas.Timestamp())
        ending timestamp that all the data loggers contain
    fill_nan : int or str or float (np.nan also exceptable)
        the desired value to replace the NaN values
    mk_contins : "y" or "n", optional
        This is an option to make the dataframes with a continous timestamp 
        column filled with the desired nan value. 
        The default is "y" (to make the timestamps continuous).
    sep_time_cols : "y" or "n", optional
        This is an option to have a seperated timestamp column or a single
        timestamp column. If the output files will be loaded into excel, it's 
        recommended to have them seperated ("y"). The default is "y".
        
        HEADER option:
        "TIMESTAMP" ("n") or "YYYY", "MM","DD","Hr","Min","Sec" ("y")
    Returns
    -------
    all_sonics : list of pandas.DataFrame()'s
       This is the list of the output sonics, A1 through D4.
    all_tc_group : list of pandas.DataFrame()'s
        This is the list of thermocouples, B1-C7
    df_WGNover : pandas.DataFrame()
        This is the observational tower located outside the 10x10m truss.
    """
    
    files = ["TOA5_2878.WGcontrol10Hz.dat", "TOA5_2879.ts_data.dat",\
              "TOA5_3884.ts_data.dat", "TOA5_4390.ts_data.dat", \
              "TOA5_4975.ts_data.dat", "TOA5_4976.ts_data.dat", \
              "TOA5_10442.ts_data.dat", "TOA5_11584.ts_data.dat",\
              "TOA5_11585.ts_data.dat"]
 
     ### First Loading the files into the script
    df_2878, df_2879 = file_to_df(path, files[0]), file_to_df(path, files[1])    
    df_3884, df_4390 = file_to_df(path, files[2]), file_to_df(path, files[3])    
    df_4975, df_4976 = file_to_df(path, files[4]), file_to_df(path, files[5])
    df_10442, df_11584 = file_to_df(path, files[6]), file_to_df(path, files[7])
    df_11585 = file_to_df(path, files[8])
     
    if mk_contins.lower() != str("y"):      
        df_2878 = cutter(df_2878, t_s, t_e)
        df_2879 = cutter(df_2879, t_s, t_e)
        df_3884 = cutter(df_3884, t_s, t_e)
        df_4390 = cutter(df_4390, t_s, t_e)
        df_4975 = cutter(df_4975, t_s, t_e)
        df_4976 = cutter(df_4976, t_s, t_e)
        df_10442 = cutter(df_10442, t_s, t_e)
        df_11584 = cutter(df_11584, t_s, t_e)
        df_11585 = cutter(df_11585, t_s, t_e)
        
    if mk_contins.lower()  == "y":     
        fmt= "Datalogger {}:"
        print(fmt.format(files[0].split(".")[0].split("_")[1]))
        df_2878 = continuous_df(cutter(df_2878, t_s, t_e), t_s, t_e, \
                                frequency ="1S")
        print(fmt.format(files[1].split(".")[0].split("_")[1]))
        df_2879 = continuous_df(cutter(df_2879, t_s, t_e), t_s, t_e)
        print(fmt.format(files[2].split(".")[0].split("_")[1]))
        df_3884 = continuous_df(cutter(df_3884, t_s, t_e), t_s, t_e)
        print(fmt.format(files[3].split(".")[0].split("_")[1]))
        df_4390 = continuous_df(cutter(df_4390, t_s, t_e), t_s, t_e)
        print(fmt.format(files[4].split(".")[0].split("_")[1]))
        df_4975 = continuous_df(cutter(df_4975, t_s, t_e), t_s, t_e)
        print(fmt.format(files[5].split(".")[0].split("_")[1]))
        df_4976 = continuous_df(cutter(df_4976, t_s, t_e), t_s, t_e)
        print(fmt.format(files[6].split(".")[0].split("_")[1]))
        df_10442 =continuous_df(cutter(df_10442, t_s, t_e), t_s, t_e)
        print(fmt.format(files[7].split(".")[0].split("_")[1]))
        df_11584 =continuous_df(cutter(df_11584, t_s, t_e), t_s, t_e)
        print(fmt.format(files[8].split(".")[0].split("_")[1]))
        df_11585 =continuous_df(cutter(df_11585, t_s, t_e), t_s, t_e)
    
    ### Initialized the list and dataframes to append to    
    sonic_columns, time_columns_lst, a_row_lst, b_row_lst, c_row_lst, \
    d_row_lst, sonc_headers, df_B1_tc, df_B2_tc, df_B3_tc, \
    df_B4_tc, df_C1_tc, df_C2_tc, df_C3_tc, df_C4_tc, t_c_lst_out, \
    t_c_lst_1, t_c_lst_2= initializing_df()
    df_WGNover = pd.DataFrame()
    
    if sep_time_cols.lower() == "y":
        time_columns_lst=["YYYY","MM","DD","Hr","Min","Sec"]
        df_2878_time = time_columns(df_2878)
        df_2879_time, df_4975_time  = time_columns(df_2879), time_columns(df_4975)
        df_4976_time, df_11585_time = time_columns(df_4976), time_columns(df_11585)
        df_10442_time, df_3884_time = time_columns(df_10442), time_columns(df_3884)
        df_11584_time, df_4390_time = time_columns(df_11584), time_columns(df_4390)
        
        for t in time_columns_lst:
            df_WGNover[t] = df_2878_time[t]
            
    if sep_time_cols.lower() != "y":
        time_columns_lst = ["TIMESTAMP"]
        df_WGNover["TIMESTAMP"] = df_2878["TIMESTAMP"]
    
    for col in range(len(sonic_columns)):
        df_WGNover[sonc_headers[col]] = df_2878[sonic_columns[col]+"1"]
    df_WGNover.fillna(value=fill_nan, inplace=True)
    
    for n in range(len(a_row_lst)):
        if sep_time_cols.lower() == "y":
            for i in range(len(time_columns_lst)):
                a_row_lst[n][time_columns_lst[i]]=df_2879_time[time_columns_lst[i]]
                b_row_lst[n][time_columns_lst[i]]=df_4975_time[time_columns_lst[i]]
                c_row_lst[n][time_columns_lst[i]]=df_4976_time[time_columns_lst[i]]
                d_row_lst[n][time_columns_lst[i]]=df_11585_time[time_columns_lst[i]]
        
        if sep_time_cols.lower() != "y":
            a_row_lst[n]["TIMESTAMP"]=df_2879["TIMESTAMP"]
            b_row_lst[n]["TIMESTAMP"]=df_4975["TIMESTAMP"]
            c_row_lst[n]["TIMESTAMP"]=df_4976["TIMESTAMP"]
            d_row_lst[n]["TIMESTAMP"]=df_11585["TIMESTAMP"]
        
        for i in range(len(sonic_columns)):
            a_row_lst[n][sonc_headers[i]] = df_2879[sonic_columns[i]+str(n+1)]
            b_row_lst[n][sonc_headers[i]] = df_4975[sonic_columns[i]+str(n+1)]
            c_row_lst[n][sonc_headers[i]] = df_4976[sonic_columns[i]+str(n+1)]
            d_row_lst[n][sonc_headers[i]] = df_11585[sonic_columns[i]+str(n+1)]
    
    all_sonics = a_row_lst+ b_row_lst + c_row_lst +d_row_lst
    ####################### Thermalcouples ##################################

    df_B5_tc, df_B6_tc, df_B7_tc = pd.DataFrame(),pd.DataFrame(),pd.DataFrame()
    df_C5_tc, df_C6_tc, df_C7_tc = pd.DataFrame(),pd.DataFrame(),pd.DataFrame()
      
    first_tc_group = [ df_B2_tc, df_B6_tc, df_B1_tc, df_B5_tc,\
                       df_C1_tc, df_C5_tc, df_C2_tc, df_C6_tc]
    
    secnd_tc_group = [ df_B4_tc, df_B3_tc, df_B7_tc,\
                      df_C3_tc, df_C7_tc, df_C4_tc]
    
    df_tc_lst_1 = [df_2879, df_10442, df_4975, df_3884, df_4976, df_11584,\
                df_11585, df_4390]
    df_tc_lst_2 = [df_2879, df_4975, df_3884, df_4976, df_11584, df_11585]    
    
    if sep_time_cols.lower() == "y":
        ### Order that data fits the tc array
        df_time_lst_1 =[df_2879_time, df_10442_time,df_4975_time,df_3884_time,\
                    df_4976_time, df_11584_time, df_11585_time, df_4390_time]
        df_time_lst_2 = [df_2879_time, df_4975_time,df_3884_time,df_4976_time,\
                   df_11584_time, df_11585_time]   
         
        ### Adding the time split time columns   
        for j in range(len(first_tc_group)):
            for t in range(len(time_columns_lst)):
                first_tc_group[j][time_columns_lst[t]]= df_time_lst_1[j][time_columns_lst[t]]
           
        for j in range(len(secnd_tc_group)):
            for t in range(len(time_columns_lst)):
                secnd_tc_group[j][time_columns_lst[t]]= df_time_lst_2[j][time_columns_lst[t]]
    
    ### Adding the timestamp column to the dataframe
    if sep_time_cols.lower() != "y":
        for j in range(len(first_tc_group)):
            first_tc_group[j]["TIMESTAMP"]= df_tc_lst_1[j]["TIMESTAMP"]   
        for j in range(len(secnd_tc_group)):
                secnd_tc_group[j]["TIMESTAMP"]= df_tc_lst_2[j]["TIMESTAMP"]    
   
    ###Adding the data to the df
    for j in range(len(first_tc_group)):
        for i in range(len(t_c_lst_1)):
            first_tc_group[j][t_c_lst_1[i]]= df_tc_lst_1[j][t_c_lst_1[i]]
    for j in range(len(secnd_tc_group)):
        for i in range(len(t_c_lst_2)):
            secnd_tc_group[j][t_c_lst_2[i]]= df_tc_lst_2[j][t_c_lst_2[i]]
    
    all_tc_group = [df_B1_tc, df_B2_tc, df_B3_tc, df_B4_tc, df_B5_tc,\
                    df_B6_tc, df_B7_tc,df_C1_tc, df_C2_tc, df_C3_tc, df_C4_tc,\
                    df_C5_tc, df_C6_tc, df_C7_tc]

    for df in range(len(all_sonics)):
        all_sonics[df].fillna(value=fill_nan, inplace=True)
    for df in range(len(all_tc_group)):
        all_tc_group[df].fillna(value=fill_nan, inplace=True)
    
    return all_sonics, all_tc_group, df_WGNover      
        

def compiler20_35(path, t_s, t_e, fill_nan, mk_contins = "y", sep_time_cols = "y"):
    """
    This compiler is used to extract the data from the raw data loggers, cut 
    each to the same starting and ending time, make the data a continous time
    column and fill nan values. Use this for 10x10m SERDP Burns 20-35.

    Parameters
    ----------
    path : str
       Location of the Burn directory containing the raw datalogger files
    t_s : str or (pandas.Timestamp())
        starting timestamp that all the loggers contain and after the 
        repeated timestamps during datalogger's start-up
    t_e : str or (pandas.Timestamp())
        ending timestamp that all the data loggers contain
    fill_nan : int or str or float (np.nan also exceptable)
        the desired value to replace the NaN values
    mk_contins : "y" or "n", optional
        This is an option to make the dataframes with a continous timestamp 
        column filled with the desired nan value. 
        The default is "y" (to make the timestamps continuous).
    sep_time_cols : "y" or "n", optional
        This is an option to have a seperated timestamp column or a single
        timestamp column. If the output files will be loaded into excel, it's 
        recommended to have them seperated ("y"). The default is "y".
        
        HEADER option:
        "TIMESTAMP" ("n") or "YYYY", "MM","DD","Hr","Min","Sec" ("y")
    Returns
    -------
    all_sonics : list of pandas.DataFrame()'s
       This is the list of the output sonics, A1 through D4.
    all_tc_group : list of pandas.DataFrame()'s
        This is the list of thermocouples, B1-C7
    df_WGNover : pandas.DataFrame()
        This is the observational tower located outside the 10x10m truss.
    """
    files = ["TOA5_4976.ts_data.dat", "TOA5_4975.ts_data.dat", \
             "TOA5_11585.ts_data.dat", "TOA5_2879.ts_data.dat", \
             "TOA5_4390.ts_data.dat", "TOA5_2005.ts_data.dat", \
             "TOA5_2878.ts_data.dat", "TOA5_11584.ts_data.dat",\
             "TOA5_10442.ts_data.dat"]

    ### First Loading the files into the script
    df_4976, df_4975  = file_to_df(path, files[0]), file_to_df(path, files[1])
    df_11585, df_2879 = file_to_df(path, files[2]), file_to_df(path, files[3])
    df_4390, df_2005  = file_to_df(path, files[4]), file_to_df(path, files[5])
    df_2878, df_11584 = file_to_df(path, files[6]), file_to_df(path, files[7])
    df_10442 = file_to_df(path, files[8])
    
    if mk_contins.lower() != str("y"):      
        df_4976  = (df_4976, t_s, t_e)
        df_4975  = cutter(df_4975, t_s, t_e)
        df_11585 = cutter(df_11585, t_s, t_e)
        df_2879  = cutter(df_2879, t_s, t_e)
        df_4390  = cutter(df_4390, t_s, t_e)
        df_2005  = cutter(df_2005, t_s, t_e)
        df_2878  = cutter(df_2878, t_s, t_e)
        df_11584 = cutter(df_11584, t_s, t_e)
        df_10442 = cutter(df_10442, t_s, t_e)
        
    if mk_contins.lower()  == "y":   
        fmt= "Datalogger {}:"
        print(fmt.format(files[0].split(".")[0].split("_")[1]))
        df_4976 = continuous_df(cutter(df_4976, t_s, t_e), t_s, t_e)
        print(fmt.format(files[1].split(".")[0].split("_")[1]))
        df_4975 = continuous_df(cutter(df_4975, t_s, t_e), t_s, t_e)
        print(fmt.format(files[2].split(".")[0].split("_")[1]))
        df_11585 = continuous_df(cutter(df_11585, t_s, t_e), t_s, t_e)
        print(fmt.format(files[3].split(".")[0].split("_")[1]))
        df_2879 = continuous_df(cutter(df_2879, t_s, t_e), t_s, t_e)
        print(fmt.format(files[4].split(".")[0].split("_")[1]))
        df_4390 = continuous_df(cutter(df_4390, t_s, t_e), t_s, t_e)
        print(fmt.format(files[5].split(".")[0].split("_")[1]))
        df_2005 =continuous_df(cutter(df_2005, t_s, t_e), t_s, t_e)
        print(fmt.format(files[6].split(".")[0].split("_")[1]))
        df_2878 = continuous_df(cutter(df_2878, t_s, t_e), t_s, t_e)
        print(fmt.format(files[7].split(".")[0].split("_")[1]))
        df_11584 = continuous_df(cutter(df_11584, t_s, t_e), t_s, t_e)
        print(fmt.format(files[8].split(".")[0].split("_")[1]))
        df_10442 = continuous_df(cutter(df_10442, t_s, t_e), t_s, t_e)
    
    ### Initialized the list and dataframes to append to    
    sonic_columns, time_columns_lst, a_row_lst, b_row_lst, c_row_lst, \
    d_row_lst, sonc_headers, df_B1_tc, df_B2_tc, df_B3_tc, \
    df_B4_tc, df_C1_tc, df_C2_tc, df_C3_tc, df_C4_tc, t_c_lst_out, \
    t_c_lst_1, t_c_lst_2= initializing_df()
    
    
    df_WGNover = pd.DataFrame()
    
    if sep_time_cols.lower() == "y":
        time_columns_lst=["YYYY","MM","DD","Hr","Min","Sec"]
        df_4976_time, df_4975_time  = time_columns(df_4976), time_columns(df_4975)
        df_11585_time, df_2879_time = time_columns(df_11585), time_columns(df_2879)
        df_4390_time, df_2005_time = time_columns(df_4390), time_columns(df_2005)
        df_2878_time, df_11584_time = time_columns(df_2878), time_columns(df_11584)
        ### WG Nover 10hz
        df_10442_time = time_columns(df_10442)
        for t in time_columns_lst:
            df_WGNover[t] = df_10442_time[t]
            
    if sep_time_cols.lower() != "y":
        time_columns_lst = ["TIMESTAMP"]
        df_WGNover["TIMESTAMP"] = df_10442["TIMESTAMP"]
    
    for col in range(len(sonic_columns)):
        df_WGNover[sonc_headers[col]] = df_10442[sonic_columns[col]+"1"]
    df_WGNover.fillna(value=fill_nan, inplace=True)
    
    for n in range(len(a_row_lst)):
        if sep_time_cols.lower() == "y":
            for i in range(len(time_columns_lst)):
                a_row_lst[n][time_columns_lst[i]]=df_4976_time[time_columns_lst[i]]
                b_row_lst[n][time_columns_lst[i]]=df_4975_time[time_columns_lst[i]]
                c_row_lst[n][time_columns_lst[i]]=df_11585_time[time_columns_lst[i]]
                d_row_lst[n][time_columns_lst[i]]=df_2879_time[time_columns_lst[i]]
            
        if sep_time_cols.lower() != "y":
            a_row_lst[n]["TIMESTAMP"]=df_4976["TIMESTAMP"]
            b_row_lst[n]["TIMESTAMP"]=df_4975["TIMESTAMP"]
            c_row_lst[n]["TIMESTAMP"]=df_11585["TIMESTAMP"]
            d_row_lst[n]["TIMESTAMP"]=df_2879["TIMESTAMP"]
        
        for i in range(len(sonic_columns)):
            a_row_lst[n][sonc_headers[i]] = df_4976[sonic_columns[i]+str(n+1)]
            b_row_lst[n][sonc_headers[i]] = df_4975[sonic_columns[i]+str(n+1)]
            c_row_lst[n][sonc_headers[i]] = df_11585[sonic_columns[i]+str(n+1)]
            d_row_lst[n][sonc_headers[i]] = df_2879[sonic_columns[i]+str(n+1)]
    
    all_sonics = a_row_lst+ b_row_lst + c_row_lst +d_row_lst
    ####################### Thermalcouples ##################################

    df_B5_tc, df_B6_tc, df_B7_tc = pd.DataFrame(),pd.DataFrame(),pd.DataFrame()
    df_C5_tc, df_C6_tc, df_C7_tc = pd.DataFrame(),pd.DataFrame(),pd.DataFrame()
      
    first_tc_group = [ df_B5_tc, df_B7_tc, df_B1_tc, df_B3_tc,\
                       df_C5_tc, df_C7_tc, df_C1_tc, df_C3_tc]
    
    secnd_tc_group = [ df_B6_tc, df_B2_tc, df_B4_tc,\
                      df_C6_tc, df_C2_tc, df_C4_tc]
    
    df_tc_lst_1 = [df_4976, df_4390, df_4975, df_2005, df_11585, df_2878,\
                df_2879, df_11584]
    
    df_tc_lst_2 = [df_4976, df_4975, df_2005, df_11585, df_2879, df_11584]
    
    if sep_time_cols.lower() == "y":
        ### Order that data fits the tc array
         df_time_lst_1 =[df_4976_time, df_4390_time, df_4975_time, df_2005_time,\
                df_11585_time, df_2878_time, df_2879_time, df_11584_time]
         df_time_lst_2 = [df_4976_time, df_4975_time, df_2005_time, df_11585_time,\
               df_2879_time, df_11584_time]  
         
        ### Adding the time split time columns   
         for j in range(len(first_tc_group)):
            for t in range(len(time_columns_lst)):
               first_tc_group[j][time_columns_lst[t]]= df_time_lst_1[j][time_columns_lst[t]]
           
         for j in range(len(secnd_tc_group)):
                for t in range(len(time_columns_lst)):
                    secnd_tc_group[j][time_columns_lst[t]]= df_time_lst_2[j][time_columns_lst[t]]
        
    ### Adding the timestamp column to the dataframe
    if sep_time_cols.lower() != "y":
        for j in range(len(first_tc_group)):
            first_tc_group[j]["TIMESTAMP"]= df_tc_lst_1[j]["TIMESTAMP"]   
        for j in range(len(secnd_tc_group)):
                secnd_tc_group[j]["TIMESTAMP"]= df_tc_lst_2[j]["TIMESTAMP"]    
   
    ###Adding the data to the df
    for j in range(len(first_tc_group)):
        for i in range(len(t_c_lst_1)):
            first_tc_group[j][t_c_lst_1[i]]= df_tc_lst_1[j][t_c_lst_1[i]]
    for j in range(len(secnd_tc_group)):
        for i in range(len(t_c_lst_2)):
            secnd_tc_group[j][t_c_lst_2[i]]= df_tc_lst_2[j][t_c_lst_2[i]]
    
    all_tc_group = [df_B1_tc, df_B2_tc, df_B3_tc, df_B4_tc, df_B5_tc,\
                    df_B6_tc, df_B7_tc,df_C1_tc, df_C2_tc, df_C3_tc, df_C4_tc,\
                    df_C5_tc, df_C6_tc, df_C7_tc]

    for df in range(len(all_sonics)):
        all_sonics[df].fillna(value=fill_nan, inplace=True)
    for df in range(len(all_tc_group)):
        all_tc_group[df].fillna(value=fill_nan, inplace=True)
    
    return all_sonics, all_tc_group, df_WGNover      
 
    