import pandas as pd
from io import BytesIO
from pathlib import Path
import re
import streamlit as st
from backend.functions import get_region


def zenic_links_alarm(alarm_file, progress_callback=None):
    if progress_callback:
        progress_callback(10)

    df = pd.read_excel(alarm_file, engine='openpyxl', parse_dates=['Occurrence Time'])
    zenic_alarms = df[['Alarm Severity', 'ME', 'Occurrence Time', 'Position', 'Alarm Code Name']]

    if progress_callback:
        progress_callback(20)

    def sort_zenic_links(zenic_alarms_df_sort):
        regex_form = re.compile(r'^[A-Za-z]{2}\d{4}$|^[A-Za-a]{3}\d{3}$|^[A-Za-a]{4}\d{2}')
        zenic_alarms['Links'] = [ [] for _ in range(len(zenic_alarms_df_sort))]

        if progress_callback:
            progress_callback(30)

        for i in zenic_alarms.index:
            zenic_alarms_df_sort['Links'][i] = re.split(r'[-_., ]', zenic_alarms_df_sort['ME'][i])
            zenic_alarms_df_sort['Links'][i] = [ j for j in zenic_alarms_df_sort['Links'][i] if regex_form.search(j)]

        if progress_callback:
            progress_callback(40)

        min_link_length = 2
        zenic_alarms_df_sort = zenic_alarms_df_sort[zenic_alarms_df_sort['Links'].apply(len) >= min_link_length]

        zenic_alarms_df_sort['Sorted Links'] = zenic_alarms_df_sort['Links'].apply(lambda x: tuple(sorted(x))) 
        zenic_alarms_df_sort = zenic_alarms_df_sort.drop_duplicates(subset=['Sorted Links'])
        zenic_alarms_df_sort = zenic_alarms_df_sort.drop(columns=['Sorted Links', 'Links'])

        if progress_callback:
            progress_callback(65)

        return zenic_alarms_df_sort
    
    alarms_df = sort_zenic_links(zenic_alarms)

    zenic_alarms_df = alarms_df.copy()
    zenic_alarms_df['Request Type'] = 'MW'
    zenic_alarms_df['Sub Type'] = 'MW links alarms'
    zenic_alarms_df['Link name'] = alarms_df['ME']
    zenic_alarms_df['Site ID'] = alarms_df['ME'].str[:6]
    zenic_alarms_df['Region'] = alarms_df['ME'].apply(get_region)
    zenic_alarms_df['Port'] = alarms_df['Position']
    zenic_alarms_df['Link Type'] = 'NR'
    zenic_alarms_df['Description'] = 'Alarms: ' + alarms_df['Alarm Code Name']
    zenic_alarms_df['Value'] = '-'
    zenic_alarms_df['Time'] = alarms_df['Occurrence Time']
    zenic_alarms_df['Severity'] = alarms_df['Alarm Severity']
    zenic_alarms_df['Action to'] = 'FLM'

    if progress_callback:
        progress_callback(85)

    zenic_alarms_df = zenic_alarms_df[
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
    
    st.write(zenic_alarms_df)
    
    excel_buffer = BytesIO()
    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
        zenic_alarms_df.to_excel(writer, sheet_name="Zenic MW Links Alarm (Filtered)", index=False)
    
    return excel_buffer, None



        

