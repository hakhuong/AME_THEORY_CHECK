
#%%
import pandas as pd
import numpy as np
import os
import time
from amethystway.const_list import *
from amethystway.data_etl import *
from amethystway.date_utils import *
from amethystway.trading_blocks import *
from amethystway.metric_generator import *
import datetime as dt
import pygsheets

from datetime import datetime, time

#%%
backtestdb_name = "update18"
db_folder_name = 'DATABASEv18'
universe_name = "FIREANT"

local_dtb_dir = get_local_dtb_dir(backtestdb_name)

# ********************************************************* 
#-----------------------CONTEXT----------------------------
# ********************************************************* 

# time_now = time.time()
date_start = np.datetime64('2023-05-31')
# date_end = np.datetime64('2023-05-24')
date_end = dt.date.today()
date_end = np.datetime64(date_end.strftime('%Y-%m-%d'))

period_sampling = 1
period_rb = 1
# exchange_name = "HSX"
context_order = {'TYPE': backtestdb_name,
                 'LEVEL': 1,
                 'UNIVERSE': universe_name,
                 'DATE_START': date_start,
                 'DATE_END': date_end,
                 'PERIOD_SAMPLING': period_sampling,
                 'PERIOD_RB': period_rb}

context = Context(context_order)
date_eval_all = context.get_date_eval_all()

local_dtb_dir = get_local_dtb_dir(backtestdb_name)
ticker_universe_list = load_ticker_universe(local_dtb_dir, universe_name)

#%%
# ********************************************************* 
#--------------LOAD INDICATOR & TRANSFORM------------------
# ********************************************************* 

onedrive_dir = os.path.expandvars("%OneDriveConsumer%")
# Get list of files in folder
files_path =  os.path.join(onedrive_dir, f"Amethyst Invest\Database\{db_folder_name}\OPERATION_DATA_CAPTURE")
files = os.listdir(files_path)



ema_pha_in_name  = "PHA-EMA_60D"
ema_pha_out_name  = "PHA-EMA_20D"
ranking_pca_rv_name  = 'PCA-RV_60D'
ranking_pha_sd_reci_name = 'PHA-SD_9D-RECI'
ranking_drawdown_name = 'DRAWDOWN'
vnindex_rv_name = 'VNINDEX_PRICE_CLOSE-RV_120D'
ppat_name = 'PPAT-A1'
vol_name = "VOLM-MA_70D-B_1D-RH"

pca_b1d_name = 'PCA-B_1D'
pca_name = 'PRICE-LAST'
pha_name = 'PRICE_HIGH-LAST'

indicator_list = [ema_pha_in_name, ema_pha_out_name, ranking_drawdown_name]

ema_pha_in_filename_list = []
ema_pha_out_filename_list = []
ranking_pca_rv_indicator_filename_list =[]
ranking_pha_sd_reci_indicator_filename_list = []
ranking_drawdown_indicator_filename_list = []
vnindex_rv_filename_list = []
ppat_filename_list = []
vol_filename_list = []

pca_b1d_filename_list = []
pca_filename_list = []
pha_filename_list = []

model_files_list = [] # selected date + after 15:00:00



#%% 
#----------------------------------------------------------
###   RETRIEVE FA INDICATORS  ###
#----------------------------------------------------------
'''
Required input's name format for FA indicators: 
- Contains "-AM_20xxQx-" after indicator name 
    Example: "PPAT-A1-AM_2023Q2-TIME_STAMP_230503_222447-TIME_CAPTURE_230503_222449"


This section identify "-AM_" for FA indicators, put all all in all_fa_quarter_df with format "K_TICKER, K_AM_QUARTER_STR, <indicator_name>"
all_fa_quarter_df then is combined with date_count_ref_base_df to generate all_fa_indi_df with format "K_TICKER, K_DATE_TRADING, <indicator_name>"

Each FA indicator is then assigned to their own object 
'''

# --- PPAT ----- TEMP 

ppat_name = 'PPAT-A1'

fa_list = [ppat_name] # list of fa indicators used 
fa_filename_list = [] # life of fa file name to append later 

# generate list of AM quarter used 
date_count_ref = load_date_count_ref(local_dtb_dir)
selected_date_df = crop_df_by_dates(date_count_ref, date_start, date_end)
quarter_list = selected_date_df['AM_QUARTER_STR'].unique()

# go through each file name in files to identify FA name and quarter used 
for file_name in files: 
    if '-AM_' in file_name: # identifier of fa indicator
        for fa in fa_list:
            if (file_name.split('-AM_')[0] in fa_list) & (file_name.split('-AM_')[1][:6] in quarter_list): 
                fa_filename_list.append(file_name)

# Generate filename_list for each FA indicator 
ppat_filename_list = [x for x in fa_filename_list if  x.split('-AM_')[0] == ppat_name]

# Go through each indicator list in all_fa_filename_list to read and create all_fa_quarter_df, which consitst of FA df of all FA indicators for all selected quarters 
all_fa_filename_list = [ppat_filename_list]
all_fa_quarter_df = [] 
for file_list in all_fa_filename_list: # file_list = list of file_name for each indicator 
    # print(file_list)
    single_indi_df = pd.DataFrame() # df contains all dates of an indicator 

    for file_name in file_list: # join different date's df of 1 indicator 
        single_df = pd.read_hdf(files_path + '\\' +  file_name)
        single_df = single_df[single_df.columns[:-2]]
        single_df[single_df.columns[-1]] = single_df[single_df.columns[-1]].apply(lambda x: float(x))
        single_indi_df = single_indi_df.append(single_df) # indicator df with all dates  

    if len(all_fa_quarter_df) == 0:
        all_fa_quarter_df = single_indi_df.copy()
        # display(all_fa_df)

    elif single_df.columns[-1] not in all_fa_quarter_df.columns: # merge different inficators into 1 df 
        all_fa_quarter_df = all_fa_quarter_df.merge(single_indi_df, on = [K_TICKER, 'AM_QUARTER_STR'], how = 'outer')


# Generate all_fa_indi_df to with DATE_TRADING. This is the final FA table 
date_count_ref_base_df = date_count_ref[[K_DATE_TRADING, 'AM_QUARTER_STR']]
all_fa_indi_df = date_count_ref_base_df.merge(all_fa_quarter_df, on = 'AM_QUARTER_STR', how = 'left')

# Assign to object 
ppat = Indicator()
ppat.assign_indicator_df(all_fa_indi_df[[K_TICKER, K_DATE_TRADING, ppat_name]])
ppat.crop_df_by_dates(date_start, date_end)
ppat_df = ppat.get_df()

#%%
#----------------------------------------------------------
###   RETRIEVE NON FA INDICATORS  ###
#----------------------------------------------------------

'''
Identify indicator captured between selected date, selected time (generated from 15:00-15:30, except for volume -- 9:00-10:00)
Each file name is assigned to corresponding list (<indicatorName>_filename_list)

Output: all_nonfa_indi_df

all_indi_df = all_nonfa_indi_df + all_fa_indi_df

'''

model_files_list = [] 
for date in date_eval_all: 
    date_str = np.datetime_as_string(date)[:-3]
    date_obj = dt.datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%f').date()  # convert date string to datetime object
    date_identifier = date_obj.strftime('%y%m%d')
    # print(date_identifier)
    for file_name in files:
        # print(file_name)


        if "-TIME_STAMP_" in file_name:
            date_obj = file_name.split("-TIME_STAMP_")[1].split("-")[0].split('_')[0]
        else:
            date_obj = file_name[-16:-10]

        if date_identifier  == date_obj: # Xét nếu date_identifier xuất hiện trong tên --- phải thêm xét sau TIME STAMP 
            # if 'VOLM-MA_70D-B_1D-RH-230427_094526' in file_name:
            #     print('------------', file_name)            
        
            if "-TIME_STAMP_" in file_name:
                timestamp_str = file_name.split("-TIME_STAMP_")[1].split("-")[0].split('_')[1]  # extract timestamp from filename

            else:
                timestamp_str = file_name.split("_")[-1][:-3]  # extract timestamp from filename
                # if 'VOLM' in file_name:
                #     print(file_name)

            timestamp_obj = datetime.strptime(timestamp_str, '%H%M%S').time()  # convert timestamp string to datetime object

            if (vol_name in file_name) & (timestamp_obj > time(9, 0, 0)) &  (timestamp_obj < time(10, 0, 0)): # Nếu là VOLM thì xét data 9am
                # if 'VOLM-MA_70D-B_1D-RH-230427_094526' in file_name:
                #     print('------------', timestamp_obj)   
                model_files_list.append(file_name)

            elif (timestamp_obj > time(15, 0, 0)) &  (timestamp_obj < time(15, 30, 0)):
                model_files_list.append(file_name)

            # print(file_name)


ema_pha_in_filename_list = [x for x in model_files_list if  x.startswith(ema_pha_in_name)]
ema_pha_out_filename_list = [x for x in model_files_list if  x.startswith(ema_pha_out_name)]
# pca_gr_1d_filename_list = [x for x in model_files_list if  x.startswith(pca_gr_1d_name)]
ranking_pca_rv_indicator_filename_list = [x for x in model_files_list if  x.startswith(ranking_pca_rv_name)]
ranking_pha_sd_reci_indicator_filename_list = [x for x in model_files_list if  x.startswith(ranking_pha_sd_reci_name)]
ranking_drawdown_indicator_filename_list = [x for x in model_files_list if  x.startswith(ranking_drawdown_name)]
vol_indicator_filename_list = [x for x in model_files_list if  (x.startswith(vol_name) & ("VOLM-MA_70D-B_1D-RH-RB_DATE" not in x))]
pca_b1d_filename_list = [x for x in model_files_list if  x.startswith(pca_b1d_name)]
vnindex_rv_filename_list = [x for x in model_files_list if  x.startswith(vnindex_rv_name)]
pca_filename_list =  [x for x in model_files_list if  x.startswith(pca_name)]
pha_filename_list =  [x for x in model_files_list if  x.startswith(pha_name)]



#%%
# Append daily captured data from a list of file names to a df 

all_filename_list = [ema_pha_in_filename_list, ema_pha_out_filename_list,\
                    ranking_pca_rv_indicator_filename_list, ranking_pha_sd_reci_indicator_filename_list,\
                    ranking_drawdown_indicator_filename_list, vnindex_rv_filename_list, \
                    pca_b1d_filename_list, pca_filename_list, pha_filename_list, vol_indicator_filename_list ] 

all_nonfa_indi_df = pd.DataFrame() # df contains all dates of all indicator 

for file_list in all_filename_list:
    print(file_list)
    single_indi_df = pd.DataFrame() # df contains all dates of an indicator 
    for file_name in file_list: 
        single_df = pd.read_hdf(files_path + '\\' +  file_name)
        single_df = single_df[single_df.columns[:-1]]
        single_indi_df = single_indi_df.append(single_df) # indicator df ưith all dates  
    if len(all_nonfa_indi_df) == 0:
        all_nonfa_indi_df = single_indi_df.copy()
        # display(all_nonfa_indi_df)
    elif single_df.columns[-1] not in all_nonfa_indi_df.columns:
        all_nonfa_indi_df = all_nonfa_indi_df.merge(single_indi_df, on = [K_TICKER, K_DATE_TRADING], how = 'outer')
        # display(all_indi_df)

all_indi_df = all_nonfa_indi_df.merge(all_fa_indi_df, on = [K_TICKER, K_DATE_TRADING], how = 'left' )
all_indi_df


#%%
#*********************************************************
###   ASSIGN INDICATOR TO OBJECTS FOR MODELLING  ###
#*********************************************************

'''
Each indicator is assigned to object for VISPA 
'''
#..........................................................
###       PHA           ##
#..........................................................

# factor_name ='PHA'

pha = Indicator()
pha.assign_indicator_df(all_indi_df[[K_TICKER, K_DATE_TRADING, pha_name]])
pha_name = pha.get_name_value()
pha_df = pha.get_df()

# EMA_PHA
ema_pha_in_param = 60
ema_pha_in = Indicator()
ema_pha_in.assign_indicator_df(all_indi_df[[K_TICKER, K_DATE_TRADING, ema_pha_in_name]])
ema_pha_in_df = ema_pha_in.get_df()

ema_pha_out_param = 20
ema_pha_out = Indicator()
ema_pha_out.assign_indicator_df(all_indi_df[[K_TICKER, K_DATE_TRADING, ema_pha_out_name]])
ema_pha_out_df = ema_pha_out.get_df()

#
ema_pha_in_div_df = pha_df.merge(ema_pha_in_df, on = [K_TICKER, K_DATE_TRADING], how = 'inner')
ema_pha_in_div = Indicator()
ema_pha_in_div.assign_indicator_df(ema_pha_in_div_df)
ema_pha_in_div.generate_indicator_div(name = None, numerator = pha_name, denominator =  ema_pha_in_name)
ema_pha_in_div_df = ema_pha_in_div.get_df()
ema_pha_in_div_name = ema_pha_in_div.get_name_value()

#
ema_pha_out_div_df = pha_df.merge(ema_pha_out_df, on = [K_TICKER, K_DATE_TRADING], how = 'inner')
ema_pha_out_div = Indicator()
ema_pha_out_div.assign_indicator_df(ema_pha_out_div_df)
ema_pha_out_div.generate_indicator_div(name = None, numerator = pha_name, denominator = ema_pha_out_name)
ema_pha_out_div_df = ema_pha_out_div.get_df()
ema_pha_out_div_name = ema_pha_out_div.get_name_value()


#..........................................................
###       PCA           ##
#..........................................................

#  PCA data from database 
pca = Indicator()
pca.assign_indicator_df(all_indi_df[[K_TICKER, K_DATE_TRADING, pca_name]])
pca_df = pca.get_df() 


pca_b1d = Indicator()
pca.assign_indicator_df(all_indi_df[[K_TICKER, K_DATE_TRADING, pca_b1d_name]])
pca_b1d_df = pca.get_df() 


# PCA_GR-1D
pca_pca_b1d_df = pca_df.merge(pca_b1d_df, on = [K_TICKER, K_DATE_TRADING], how = 'outer')

pca_gr_1d = Indicator()
pca_gr_1d.assign_indicator_df(pca_pca_b1d_df)
pca_gr_1d.generate_indicator_div(name = None, numerator = pca_name, denominator =  pca_b1d_name)
pca_gr_1d_df = pca_gr_1d.get_df()
pca_gr_1d_name = pca_gr_1d.get_name_value()

# Generate Return indicator 
return_indicator = Indicator()
return_indicator.assign_indicator_df(pca_gr_1d_df)
return_indicator.shift_indicator_backward(1)
i_return_name = return_indicator.get_name_value()
i_return_df = return_indicator.get_df()

#%%
#..........................................................
###       RANKING INDICATOR           ##
#..........................................................

# PCA-RV_60D  
ranking_pca_rv_indicator = Indicator()
ranking_pca_rv_indicator.assign_indicator_df(all_indi_df[[K_TICKER, K_DATE_TRADING, ranking_pca_rv_name]])
# ranking_pca_rv_name = ranking_pca_rv_indicator.get_name_value()
ranking_pca_rv_df = ranking_pca_rv_indicator.get_df()

# PHA-SD_9D-RECI
# ranking_pha_sd_reci_win = 9
ranking_pha_sd_reci_indicator = Indicator()
ranking_pha_sd_reci_indicator.assign_indicator_df(all_indi_df[[K_TICKER, K_DATE_TRADING, ranking_pha_sd_reci_name]])
ranking_pha_sd_reci_df = ranking_pha_sd_reci_indicator.get_df()
# ranking_pha_sd_reci_name = ranking_pha_sd_reci_indicator.get_name_value()

# DRAWDOWN
ranking_drawdown_indicator = Indicator()
ranking_drawdown_indicator.assign_indicator_df(all_indi_df[[K_TICKER, K_DATE_TRADING, ranking_drawdown_name]])
ranking_drawdown_df = ranking_drawdown_indicator.get_df()
# ranking_drawdown_name = ranking_drawdown_indicator.get_name_value()

#..........................................................
###       VNINDEX CONTEXT           ##
#..........................................................

vnindex_rv = Indicator()
vnindex_rv.assign_indicator_df(all_indi_df[[K_TICKER, K_DATE_TRADING, vnindex_rv_name]])

#----------------------------------------------------------
###   VOLM ###
#----------------------------------------------------------

vol = Indicator()
vol.assign_indicator_df(all_indi_df[[K_TICKER, K_DATE_TRADING, vol_name]])

#%%
#*********************************************************
###   VISPA MODELLING  ###
#*********************************************************

#----------------------------------------------------------
### SIGNAL IN ###
#----------------------------------------------------------


#-------- Signal in 1: Volume > 0.6 --------#
s_in_1_dict = {
    "INDICATOR": vol_name,  # Thanh khoan
    "TYPE": "compare",
    "METHOD": "bounded",
    "LIM": [0.6, 1],
    'PERIOD_SAMPLING': 1
}

s_in_1_obj = SignalCompare(s_in_1_dict)
s_in_1_obj.context = context
s_in_1_obj.generate_signal_from_context_indicator_run(context = context, indicator = vol)
s_in_1_df = s_in_1_obj.get_df()

#-------- Signal in 2: PPAT-A1 > 1.1 --------#
s_in_2_dict = {
    "INDICATOR": ppat_name,  # Thanh khoan
    "TYPE": "compare",
    "METHOD": "greater_than_equal",
    "LIM": 1.1,
    'PERIOD_SAMPLING': 1
}

s_in_2_obj = SignalCompare(s_in_2_dict)
s_in_2_obj.context = context
s_in_2_obj.generate_signal_from_context_indicator_run(context = context, indicator = ppat)
s_in_2_df = s_in_2_obj.get_df()

#-------- Signal in 3: PHA > EMA 60D --------#
s_in_3_dict = {
    "INDICATOR": ema_pha_in_div_name,  # Thanh khoan
    "TYPE": "compare",
    "METHOD": "greater_than",
    "LIM": 1,
    'PERIOD_SAMPLING': 1
}

s_in_3_obj = SignalCompare(s_in_3_dict)
s_in_3_obj.context = context
s_in_3_obj.generate_signal_from_context_indicator_run(context = context, indicator = ema_pha_in_div)
s_in_3_df = s_in_3_obj.get_df()

#-------- Signal in 4: VNINDEX không ở đáy RV-120D --------#
s_in_4_dict = {
    "INDICATOR": vnindex_rv_name,  # Thanh khoan
    "TYPE": "compare",
    "METHOD": "greater_than",
    "LIM": 0.1,
    'PERIOD_SAMPLING': 1
}

s_in_4_obj = SignalCompare(s_in_4_dict)
s_in_4_obj.context = context
s_in_4_obj.generate_signal_from_context_indicator_run(context = context, indicator = vnindex_rv)
s_in_4_df = s_in_4_obj.get_df()

#----------------------------------------------------------
### SIGNAL OUT ###
#----------------------------------------------------------

#-------- Signal out 1: PHA < PHA 20D --------#
s_out_1_dict = {
    "INDICATOR": ema_pha_out_div_name,  # Thanh khoan
    "TYPE": "compare",
    "METHOD": "smaller_than",
    "LIM": 1,
    'PERIOD_SAMPLING': 1
}

s_out_1_obj = SignalCompare(s_out_1_dict)
s_out_1_obj.context = context
s_out_1_obj.generate_signal_from_context_indicator_run(context = context, indicator = ema_pha_out_div)
s_out_1_df = s_out_1_obj.get_df()

#-------- Signal out 2: Phanh PRICE floor  --------#
s_out_2_dict = {
    "INDICATOR": pca_gr_1d_name,  # Thanh khoan
    "TYPE": "compare",
    "METHOD": "smaller_than",
    "LIM": 0.94,
    'PERIOD_SAMPLING': 1
}

s_out_2_obj = SignalCompare(s_out_2_dict)
s_out_2_obj.context = context
s_out_2_obj.generate_signal_from_context_indicator_run(context = context, indicator = pca_gr_1d)
s_out_2_df = s_out_2_obj.get_df()

#-------- Signal out 3: PCA-RV_60D < 0.9  --------#
s_out_3_dict = {
    "INDICATOR": ranking_pca_rv_name,  # Thanh khoan
    "TYPE": "compare",
    "METHOD": "smaller_than_equal",
    "LIM": 0.9,
    'PERIOD_SAMPLING': 1
}

s_out_3_obj = SignalCompare(s_out_3_dict)
s_out_3_obj.context = context
s_out_3_obj.generate_signal_from_context_indicator_run(context = context, indicator = ranking_pca_rv_indicator)
s_out_3_df = s_out_3_obj.get_df()

#----------------------------------------------------------
### SIGNAL COMBINE FOR SIGNAL IN & SIGNAL OUT  ###
#----------------------------------------------------------

#----COMBINED SIGNAL IN---#
s_in_list = [s_in_1_obj, s_in_2_obj, s_in_3_obj, s_in_4_obj]
s_in_logic = '1AND2AND3AND4'
s_in_period_sampling = 1 
# s_out_input_tuple = (s_dict_list, s_logic, s_period_sampling)

s_in_obj = SignalCombine()
s_in_obj.generate_signal_from_logic(s_in_list, s_in_logic)


#----COMBINED SIGNAL OUT---#
s_out_list = [s_out_1_obj, s_out_2_obj, s_out_3_obj]
s_out_logic = '1OR2OR3'
s_out_period_sampling = 1 

s_out_obj = SignalCombine()
s_out_obj.generate_signal_from_logic(s_out_list, s_out_logic)


#----------------------------------------------------------
### WATCHLIST / POSITION  ###
#----------------------------------------------------------
watchlist_pos = Position()
# watchlist_pos.generate_position_from_s_io_out_first(s_in_obj, s_out_obj)
watchlist_pos.generate_position_from_s_io_in_first(s_in_obj, s_out_obj)
watchlist_pos_df = watchlist_pos.get_df()

#----------------------------------------------------------
### ASSIGN  ###
#----------------------------------------------------------
ranking_indi_list = [ranking_pca_rv_indicator, ranking_pha_sd_reci_indicator,  ranking_drawdown_indicator]
total_slot = 15
tsplus_length = 3
assign_obj = Assign()
assign_obj = apply_assign_HAI_v3_new(watchlist_pos, ranking_indi_list, total_slot, tsplus_length, gr_1d_indi_run = pca_gr_1d)

assign_df = assign_obj.get_df()

#----------------------------------------------------------
###   RETURN  ###
#----------------------------------------------------------
return_obj = Return()
return_obj.generate_return_from_assign(assign_obj)
return_df = return_obj.get_df()



#%%
# ********************************************************* 
# --------------------UPLOAD TO GGSHEET--------------------
# ********************************************************* 
dep6_history_portf_df = assign_df[[K_DATE_TRADING, K_TICKER, K_ASSIGN]].copy()

project_dir = os.getcwd()
save_dir = os.path.join(project_dir, "ggsheet_key")
save_file = "ha-pricesync-364403-de04eb239752.json"
client_ggsheet = pygsheets.authorize(service_account_file=os.path.join(save_dir, save_file))

table_name = 'DEP6_THEORY_PORTFOLIO'

def upload_df_to_ggsheet(client_ggsheet, table_name, df, sheet_name):
    '''
    Uploade a df to a sheet in a ggsheet file, using a pygsheets client
    '''
    spreadsht = client_ggsheet.open(table_name)
    worksht = spreadsht.worksheet("title", sheet_name)
    worksht.clear()
    worksht.set_dataframe(df, (1, 1))
    print(f"DF has been uploaded to ggsheet file {table_name}, sheet {sheet_name}")


def get_time_stamp_str():
    '''
    Get time stamp from current date
    '''
    import time
    time_now = time.time()
    time_stamp_str = dt.datetime.fromtimestamp(time_now).strftime(r"%y%m%d_%H%M%S")
    return time_stamp_str

def add_time_stamp_to_df(df1):
    '''(data_df[K_DATE_TRADING]=='2023-04-17')
    Add a time stamp column to a df
    '''
    df = df1.copy()
    time_stamp_str = get_time_stamp_str()
    df['TIME_STAMP'] = time_stamp_str
    return df 


dep6_history_portf_df = add_time_stamp_to_df(dep6_history_portf_df)
upload_df_to_ggsheet(client_ggsheet, table_name, dep6_history_portf_df, "PORTFOLIO")

return_upload_df = add_time_stamp_to_df(return_df[[K_DATE_TRADING, K_RETURN_PORTFOLIO, K_TICKER_COUNT]].copy())
upload_df_to_ggsheet(client_ggsheet, table_name, return_upload_df, "PORT_RETURN")
# %%
