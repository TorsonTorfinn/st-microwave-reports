import pandas as pd
from io import BytesIO
from pathlib import Path
import re
import streamlit as st
from backend.functions import get_region


def mw_links_alarm(alarm_file, progress_callback=None):
    if progress_callback:
        progress_callback(10)
    
    mw_df = pd.read_excel(alarm_file, skiprows=1)

    if 'pe' in mw_df.columns:
        mw_df = mw_df.rename(columns={'pe': 'NE Type'})  

    mw_df['Rx'] = [-50 for _ in range(len(mw_df))]

    #группируюпо 'NE' =ср 'Rx' и первых значений ост. столбцов
    mw_df = mw_df.groupby('NE').agg({
    'Rx': 'mean',
    'Raised Time': 'first',
    'NE Type': 'first',
    'Severity': 'first',
    'Alarm Code': 'first'
    }).reset_index()


    mw_df['Sites List'] = [[] for _ in range(len(mw_df))]
    mw_df['LINK'] = pd.Series(dtype='object')

    if progress_callback:
        progress_callback(25)

    regex = re.compile(r'^[A-Za-z]{2}\d{4}$|^[A-Za-z]{3}\d{3}$|^[A-Za-z]{4}\d{2}')

    for idx in mw_df.index:
        mw_df['Sites List'][idx] = re.split(r'[-_., ]', mw_df['NE'][idx])
        mw_df['Sites List'][idx] = [i for i in mw_df['Sites List'][idx] if regex.search(i)]

    mw_df['Sites List STR'] = mw_df['Sites List'].apply(lambda x: ','.join(x))
    min_rx_idx = mw_df.groupby('Sites List STR')['Rx'].idxmin()
    mw_df = mw_df.loc[min_rx_idx]
    mw_df = mw_df.drop(columns=['Sites List STR'])
    mw_df = mw_df.sort_values(by=['NE']).reset_index()

    if progress_callback:
        progress_callback(60)

    def check_two_df(list1, list2):
        return list1[0] in list2[1:] and list2[0] in list1[1:]

    for idx in mw_df.index:
        if not pd.isna(mw_df['LINK'][idx]):
            continue
        link_name = mw_df['NE'][idx]
        site_list = mw_df['Sites List'][idx]
        mw_df['LINK'][idx] = link_name
        for j in range(idx+1, len(mw_df)):
            if not pd.isna(mw_df['LINK'][j]):
                continue
            if check_two_df(site_list, mw_df['Sites List'][j]):
                mw_df['LINK'][j] = link_name
            else:
                continue
    
    if progress_callback:
        progress_callback(80)

    rsl_first_idx = mw_df.groupby('LINK')['Rx'].head(1).index
    mw_df = mw_df.loc[rsl_first_idx]
    mw_df = mw_df.reset_index()
    mw_df = mw_df.drop(columns=['level_0', 'index', 'Rx'])

    mw_report_df = mw_df.copy()
    mw_report_df['Request Type'] = 'MW'
    mw_report_df['Sub Type'] = 'MW links alarms'
    mw_report_df['Link name'] = mw_report_df['NE']
    mw_report_df['Site ID'] = mw_report_df['NE'].str[:6]
    mw_report_df['Region'] = mw_report_df['Site ID'].apply(get_region)
    mw_report_df['Port'] = '-'
    mw_report_df['Link Type'] = 'NR'
    mw_report_df['Description'] = 'MW Links Alarms: ' + mw_report_df['Alarm Code']
    mw_report_df['Value'] = '-'
    mw_report_df['Time'] = mw_report_df['Raised Time']
    mw_report_df['Severity'] = mw_report_df['Severity']
    mw_report_df['Action to'] = 'FLM'

    mw_report_df = mw_report_df[
        ['Request Type', 
         'Sub Type', 
         'Link name', 
         'Site ID', 
         'Region', 
         'Port', 
         'Link Type', 
         'Description', 
         'Value', 
         'Time', 
         'Severity', 
         'Action to']
    ]


    excel_buffer = BytesIO()
    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
        mw_report_df.to_excel(writer, sheet_name='NR MW Links Alarms(FILTERED)', index=False)

    if progress_callback:
        progress_callback(100)

    st.write(mw_df)

    return excel_buffer, None



def rtn_links_alarms(alarm_file, progress_callback=None):
    if progress_callback:
        progress_callback(10)

    df = pd.read_excel(alarm_file, engine='openpyxl', parse_dates=['First Occurred (ST)'], skiprows=5)
    mwAlarmDF = df[['Alarm Source', 'First Occurred (ST)', 'Severity']]

    if progress_callback:
        progress_callback(30)

    def sort_links(alarm_source):
        regex_form = re.compile(r'^[A-Za-z]{2}\d{4}$|^[A-Za-z]{3}\d{3}$|^[A-Za-z]{4}\d{2}')
        alarm_source['Links'] = [[] for _ in range(len(alarm_source))]

        for i in alarm_source.index:
            alarm_source['Links'][i] = re.split(r'[-_., ]', alarm_source['Alarm Source'][i])
            alarm_source['Links'][i] = [j for j in alarm_source['Links'][i] if regex_form.search(j)]
        
        min_link_length = 2
        alarm_source = alarm_source[alarm_source['Links'].apply(len) >= min_link_length]

        alarm_source['Sorted Links'] = alarm_source['Links'].apply(lambda x: tuple(sorted(x)))
        alarm_source = alarm_source.drop_duplicates(subset=['Sorted Links'])
        alarm_source = alarm_source.drop(columns=['Sorted Links'])

        return alarm_source
    
    if progress_callback:
        progress_callback(70)
    

    rtn_mw_alarm = sort_links(mwAlarmDF)

    rtn_report_df = rtn_mw_alarm.copy()
    rtn_report_df['Request Type'] = "MW"
    rtn_report_df['Sub Type'] = 'MW links alarms'
    rtn_report_df['Link name'] = rtn_report_df['Alarm Source']
    rtn_report_df['Site ID'] = rtn_report_df['Alarm Source'].str[:6]
    rtn_report_df['Region'] = rtn_report_df['Site ID'].apply(get_region)
    rtn_report_df['Port'] = "-"
    rtn_report_df['Link Type'] = 'RTN'
    rtn_report_df['Description'] = 'NMS is not available'
    rtn_report_df['Value'] = '-'
    rtn_report_df['Time'] = rtn_report_df['First Occurred (ST)']
    rtn_report_df['Severity'] = rtn_report_df['Severity']
    rtn_report_df['Action to'] = 'FLM'

    rtn_report_df = rtn_report_df[
        ['Request Type', 
         'Sub Type', 
         'Link name', 
         'Site ID', 
         'Region', 
         'Port', 
         'Link Type', 
         'Description', 
         'Value', 
         'Time', 
         'Severity', 
         'Action to']
    ]    

    if progress_callback:
        progress_callback(100)

    st.write(rtn_report_df)

    excel_buffer = BytesIO()
    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
        rtn_report_df.to_excel(writer, sheet_name="RTN MW Links Alarms(FILTERED)", index=False)

    return excel_buffer, None
