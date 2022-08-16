'''
###################################################
I'm talking about drawing a line in the sand, dude!
###################################################
'''
######################################
### --- Import All the Things! --- ###
######################################

import pandas as pd
import numpy as np
import os
import fnmatch
import timeit
import sys
#from tkinter import Tk, Label, Frame, Entry, Button, LabelFrame, Radiobutton, Text, Scrollbar, IntVar, Checkbutton, Menu, Variable, Toplevel
#from tkinter import filedialog as fd
#from threading import Thread
import warnings
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)
pd.options.mode.chained_assignment = None

#######################
### --- /Import --- ###
#######################

def read_variables(cwd):    
    #global variables
    os.chdir(cwd)
    for file in os.listdir(os.getcwd()):
        if fnmatch.fnmatch(file, 'variable*.xlsx'):
            variables = pd.read_excel(file)    
    return(variables)

def read_me(path):
    timer_start = timeit.default_timer()
    #print('Reading in {}...'.format(path))
    #process_txt.insert('end-1c','Reading in {}...\n'.format(path))
    
    df = pd.read_hdf(path,'key')
    
    timer_end = timeit.default_timer() - timer_start
    #print('Time to read in: {:.2f}'.format(timer_end))
    #process_txt.insert('end-1c','Time to read in: {:.2f}\n'.format(timer_end))
    #print('Time to read in: {:.2f}\n'.format(timer_end))
    return(df)

def leads_extractor(date_start, date_end, records, time_col):
    #process_txt.insert('end-1c','Extracting date range...\n')
    os.chdir(database_folder)
    #timer_start = timeit.default_timer()
    
    date_df = pd.read_csv(records, parse_dates=['Min','Max'])
    
    lower = date_df.loc[(date_df['Min']<=date_start)].iloc[-1].name
    if date_df.loc[(date_df['Max']>=date_end)].empty:
        date_end = date_df['Max'].max()
    upper = date_df.loc[(date_df['Max']>=date_end)].iloc[0].name+1
    
    selection = date_df.iloc[lower:upper]
    to_concat = []
    for i in selection['Filename']:
        j = read_me(i)
        to_concat.append(j)
    db = pd.concat(to_concat,sort=False)
    #name = 'Leads'
    date_end = date_end
    db = db.loc[(db[time_col]>=date_start) & (db[time_col]<=date_end)].reset_index(drop=True)
    #process_txt.insert('end-1c','Writing out {} file from {} to {}\n'.format(name, db[time_col].min(), db[time_col].max()))
    #os.chdir(output_lbl.cget('text'))
    
    #db.to_csv('{}_out_{}_to_{}.csv'.format(name,date_start.strftime('%Y-%m-%d'),date_end.strftime('%Y-%m-%d')),index=False)
    #timer_done = timeit.default_timer() - timer_start
    #process_txt.insert('end-1c','Time to complete: {}\n'.format(timer_done)) 
    return(db)

# HBS value-based bidding upload tool
# - For last 90 days of leads sourced to Google, we want gclid and projected value of each lead

# input: variables file
global cwd
cwd = os.getcwd()
variables = read_variables(cwd)
database_folder = variables['Database folder'][0]
leads_db_path = variables['Leads Database'][0]
value_in = variables['lead value input file'][0]
value_out = variables['lead value output'][0]
conv_name = variables['Conversion Name'][0]
#no_data = variables['No Data Fill Amount'][0]

output_cols = ['Google Click ID', 
               'Conversion Name', 
               'Conversion Time', 
               #'Attributed Credit', 
               'Conversion Value', 
               'Conversion Currency']

# input: leads (90 days)
#end = pd.to_datetime('today').normalize()- pd.Timedelta(1, 'days')
#start = end - pd.Timedelta(90, 'days')
start = variables['VBB Date Range'][0]
end = variables['VBB Date Range'][1]

print('Reading in leads...')
leads = leads_extractor(start, end, leads_db_path, 'action_timestamp')
# input: lead projections

print('Cleaning GCLIDs...')
# clean - get only Google entries
leads = leads.loc[leads['Category'].str.contains('GAW', na=False)]
leads = leads.loc[leads['Action Type']!='Apply']
leads.reset_index(drop=True, inplace=True)
leads = leads[['Category', 'Source', 'action_timestamp', 'landing_url', 'referrer']]
# clean - get gclid from landing_url or referrer fields
leads['gclid'] = leads['landing_url'].replace('.*&gclid=','', regex=True)
leads.loc[leads['gclid'].str.contains('https://', na=False), 'gclid'] = np.nan
leads['gclid'] = leads['gclid'].where(leads['gclid'].notnull(), leads['referrer'].replace('.*&gclid=','', regex=True))
leads['gclid'] = leads['gclid'].replace('&.*','', regex=True)
leads.loc[leads['gclid'].str.contains('https://', na=False), 'gclid'] = np.nan
leads.loc[leads['gclid'].str.contains('http://', na=False), 'gclid'] = np.nan

leads = leads.loc[leads['gclid'].notnull()]
leads.reset_index(drop=True, inplace=True)

# merge projections to gclid
value = pd.read_excel(value_in, sheet_name=0)
value = value[['Category', 'Source', 'Testing Value/Lead']]
#value['Testing Value/Lead'] = value['Testing Value/Lead'].where(~value['Testing Value/Lead'].str.contains('Not', case=False, na=False), no_data)
leads_out = pd.merge(leads, value, how='left', left_on=['Category', 'Source'], right_on=['Category', 'Source'])
# make output
leads_out.rename(columns={'gclid':'Google Click ID',
                          'action_timestamp':'Conversion Time',
                          'Testing Value/Lead':'Conversion Value'}, inplace=True)
leads_out['Conversion Name'] = conv_name
#leads_out['Attributed Credit'] = 1
leads_out['Conversion Currency'] = 'USD'

leads_out = leads_out.filter(output_cols)
leads_out = leads_out.loc[leads_out['Conversion Value'].notnull()]
leads_out.reset_index(drop=True, inplace=True)

#sort and drop dupe gclids
leads_out.sort_values('Conversion Time', ascending=True, inplace=True)
leads_out.drop_duplicates(subset='Google Click ID', keep='first', inplace=True)
leads_out.reset_index(drop=True, inplace=True)
#write out
print('Writing output...')
pd.DataFrame(columns=['Parameters:TimeZone=-0600']).to_csv(value_out, index=False)
leads_out.to_csv(value_out, header=True, index=False, mode='a')
input("Press any key to end...")



