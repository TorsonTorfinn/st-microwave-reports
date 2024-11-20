import pandas as pd
from io import BytesIO
from pathlib import Path
import re
import streamlit as st


def get_severity(value):
    if value >= -50:
        return 'Minor'
    elif value <= -60:
        return 'Critical'
    else:
        return 'Major'


def get_region(site_id):
    region_mapping = {
    'AN': 'ANDIJAN', 
    'BH': 'BUKHARA', 
    'DZ': 'DJIZZAK', 
    'FR': 'FERGANA', 
    'KR': 'KARAKALPAKISTAN', 
    'KS': 'KASHKADARYA', 
    'KH': 'KHOREZM', 
    'NM': 'NAMANGAN', 
    'NV': 'NAVOI', 
    'SM': 'SAMARKAND', 
    'SR': 'SIRDARYA', 
    'SU': 'SURKHANDARYA', 
    'TS': 'TASHKENT'
    }
    return region_mapping.get(site_id[:2], 'UNKNOWN ID')



def process_link_status(nr_report_df, stand_file, atoll_file):
    # Загрузка данных из файлов stand_file и atoll_file
    stand = pd.read_excel(stand_file, usecols=["NE Name(A)", "NE Name(Z)"], sheet_name='pm_cm link_data')
    atoll = pd.read_excel(atoll_file, usecols=["SITE_A", "SITE_B"])

    stand['NE Name(A)'] = pd.concat([stand['NE Name(A)'], stand['NE Name(Z)']], ignore_index=True)
    stand['NE Name(Z)'] = pd.concat([stand['NE Name(Z)'], stand['NE Name(A)']], ignore_index=True)

    # Создание столбца NE Name(A-Z) в stand для комбинирования имен NE
    stand['NE Name(A-Z)'] = stand['NE Name(A)'].str[:6] + "-" + stand["NE Name(Z)"].str[:6]
    stand = stand.drop_duplicates(subset=['NE Name(A-Z)'])
    
    # Создание двух столбцов с комбинациями имен для сравнения в atoll
    atoll['A-B'] = atoll["SITE_A"] + "-" + atoll["SITE_B"]
    atoll['B-A'] = atoll["SITE_B"] + "-" + atoll["SITE_A"]
    atoll_combined = pd.concat([atoll['A-B'], atoll['B-A']], ignore_index=True)
    
    # Создание набора всех активных ссылок из трех столбцов stand и двух столбцов atoll
    active_links_set = set(stand['NE Name(A)']).union(set(stand['NE Name(Z)']), set(stand['NE Name(A-Z)']), set(atoll_combined))
    # active_links_list = list(stand['NE Name(A)']) + list(stand['NE Name(Z)']) + list(stand['NE Name(A-Z)']) + list(atoll_combined)


    # Присвоение статуса в зависимости от совпадений в данных
    nr_report_df['Link Status'] = nr_report_df['Link name'].apply(
        lambda x: 'ACTIVE' if x in active_links_set else 'STAND-BY'
    )
    

    # Обновление столбца Severity для ссылок со статусом 'STAND-BY' на 'Minor'
    nr_report_df.loc[nr_report_df['Link Status'] == 'STAND-BY', 'Severity'] = 'Minor'

    nr_report_df.loc[(nr_report_df['Link Status'] == 'STAND-BY') & (nr_report_df['Link name'].str[:13].isin([link[:13] for link in active_links_set])),'Link Status'] = 'ACTIVE'
    
    return nr_report_df

# damnnnn


def process_mw_tt(mw_tt_file):
    mw_tt_df = pd.read_excel(mw_tt_file, skiprows=1)
    mw_tt_df['SITES'] = [[] for _ in range(len(mw_tt_df))]

    regex_mw = re.compile(r'^[A-Za-z]{2}\d{4}$|^[A-Za-z]{3}\d{3}$|^[A-Za-z]{4}\d{2}')

    for i in mw_tt_df.index:
        mw_tt_df['SITES'][i] = re.split(r'[-_., ]', mw_tt_df['SITE_ID'][i])
        mw_tt_df['SITES'][i] = [j for j in mw_tt_df['SITES'][i] if regex_mw.search(j)]
    
    mw_tt_df['MW_SITES_COMBINED'] = mw_tt_df['SITES'].apply(lambda x: "-".join(x))
    mw_tt_df['MW_SITES_REVERSED'] = mw_tt_df['MW_SITES_COMBINED'].apply(lambda x: "-".join(x.split('-')[::-1]))

    return mw_tt_df



def nr_report(files, mw_tt_file, stand_file, atoll_file, progress_callback=None):
    if progress_callback: # updating the progress
        progress_callback(10)

    # Чтение файлов, загруженных через Streamlit
    dataframes = [pd.read_excel(file, skiprows=range(0, 5), sheet_name='sheet1') for file in files]

    # Определение переменных на основе имени файла
    new_nr, old_nr1, old_nr2 = None, None, None
    for file, df in zip(files, dataframes):
        if "NR8120" in file.name:
            old_nr1 = df
        elif "NR8250" in file.name:
            old_nr2 = df
        elif "checkpoint" in file.name:
            new_nr = df

    if any(var is None for var in (new_nr, old_nr1, old_nr2)):
        return None, "Unable to find all required files matching the given criteria."
    
    if progress_callback: # updating the progress
        progress_callback(20)
    
    # Очистка столбцов
    columns_to_drop_new = ['Index', 'End Time', 'Query Granularity', 'Neighbor NE Ip', 'Neighbor NE Port', 'IPADDRESS', 'LINK NAME']
    columns_to_drop_old = ['Index', 'End Time', 'Query Granularity', 'IP Address', 'Neighbor NE IP', 'Neighbor NE Port', 'LINK NAME']
    
    new_nr = new_nr.drop(columns=[col for col in columns_to_drop_new if col in new_nr.columns], axis=1)
    old_nr1 = old_nr1.drop(columns=[col for col in columns_to_drop_old if col in old_nr1.columns], axis=1)
    old_nr2 = old_nr2.drop(columns=[col for col in columns_to_drop_old if col in old_nr2.columns], axis=1)


    # Объединение DataFrame
    all_nr = pd.concat([new_nr, old_nr1, old_nr2], ignore_index=True)
    all_nr['Start Time'] = pd.to_datetime(all_nr['Start Time']).dt.date
    all_nr['Link Name'] = all_nr['NE Location'].str.split(',').str[-1]

    # with pd.ExcelWriter(NR_REPORT, engine='openpyxl') as writer:
    #     all_nr.to_excel(writer, sheet_name='Test Page', index=False)

    if progress_callback: # updating the progress
        progress_callback(30)

    # Агрегация данных
    all_nr_agg = all_nr[['Link Name', 'MO Location', 'Mean Transmitted Power(dBm)', 'Mean Received Signal Level(dBm)']].groupby(['Link Name', 'MO Location']).agg(
        tsl_count=pd.NamedAgg(column='Mean Transmitted Power(dBm)', aggfunc='mean'),
        rsl_count=pd.NamedAgg(column='Mean Received Signal Level(dBm)', aggfunc='mean')
    ).reset_index()

    if progress_callback: # updating the progress
        progress_callback(40)

    # Фильтрация значений
    all_nr_agg = all_nr_agg[~all_nr_agg['tsl_count'].between(-100, 0)]
    all_nr_agg = all_nr_agg[~all_nr_agg['rsl_count'].between(-100, -80)]
    all_nr_agg = all_nr_agg[~all_nr_agg['rsl_count'].between(-48, 0)]

    # st.write(all_nr_agg)
    
    all_nr_agg = all_nr_agg.reset_index()

    if progress_callback: # updating the progress
        progress_callback(50)

    all_nr_agg['Site List'] = [[] for _ in range(len(all_nr_agg))]
    all_nr_agg['LINK'] = pd.Series(dtype='object')

    if progress_callback: # updating the progress
        progress_callback(60)

    regex = re.compile(r'^[A-Za-z]{2}\d{4}$|^[A-Za-z]{3}\d{3}$|^[A-Za-z]{4}\d{2}')

    for i in all_nr_agg.index:
        all_nr_agg['Site List'][i] = re.split(r'[-_., ]', all_nr_agg['Link Name'][i])
        all_nr_agg['Site List'][i] = [j for j in all_nr_agg['Site List'][i] if regex.search(j)]

    all_nr_agg['Site List Str'] = all_nr_agg['Site List'].apply(lambda x: ','.join(x))

    min_rsl_idx = all_nr_agg.groupby('Site List Str')['rsl_count'].idxmin()
    all_nr_agg = all_nr_agg.loc[min_rsl_idx]

    if progress_callback: # updating the progress
        progress_callback(60)
    
    all_nr_agg = all_nr_agg.drop(columns=['Site List Str', 'tsl_count'])
    all_nr_agg = all_nr_agg.sort_values(by=['Link Name']).reset_index()

    if progress_callback: # updating the progress
        progress_callback(80)


    def check_two_array(list1, list2):
        return list1[0] in list2[1:] and list2[0] in list1[1:]
    

    for i in all_nr_agg.index:
        if not pd.isna(all_nr_agg['LINK'][i]):
            continue
        link_name = all_nr_agg['Link Name'][i]
        site_lst = all_nr_agg['Site List'][i]
        all_nr_agg['LINK'][i] = link_name
        for j in range(i + 1, len(all_nr_agg)):
            if not pd.isna(all_nr_agg['LINK'][j]):
                continue
            if check_two_array(site_lst, all_nr_agg['Site List'][j]):
                all_nr_agg['LINK'][j] = link_name
            else:
                continue

    all_nr_agg = all_nr_agg.drop(columns=['level_0', 'index'])

    rsl_first_idx = all_nr_agg.groupby('LINK')['rsl_count'].head(1).index
    all_nr_agg = all_nr_agg.loc[rsl_first_idx]

    all_nr_agg['Site List Combined'] = all_nr_agg['Site List'].apply(lambda x: '-'.join(x))

    process_mw = process_mw_tt(mw_tt_file)

    all_nr_agg = all_nr_agg[~all_nr_agg['Link Name'].isin(process_mw['SITE_ID'])]
    all_nr_agg = all_nr_agg[~all_nr_agg['LINK'].isin(process_mw['SITE_ID'])]
    all_nr_agg = all_nr_agg[~all_nr_agg['Site List Combined'].isin(process_mw['SITE_ID'])]

    all_nr_agg = all_nr_agg[~all_nr_agg['Link Name'].isin(process_mw['MW_SITES_COMBINED'])]
    all_nr_agg = all_nr_agg[~all_nr_agg['LINK'].isin(process_mw['MW_SITES_COMBINED'])]
    all_nr_agg = all_nr_agg[~all_nr_agg['Site List Combined'].isin(process_mw['MW_SITES_COMBINED'])]

    all_nr_agg = all_nr_agg[~all_nr_agg['Link Name'].isin(process_mw['MW_SITES_REVERSED'])]
    all_nr_agg = all_nr_agg[~all_nr_agg['LINK'].isin(process_mw['MW_SITES_REVERSED'])]
    all_nr_agg = all_nr_agg[~all_nr_agg['Site List Combined'].isin(process_mw['MW_SITES_REVERSED'])]


    nr_report_df = all_nr_agg.copy()
    nr_report_df['Request Type'] = 'MW'
    nr_report_df['Sub Type'] = 'Bad Rx level'
    nr_report_df['Link name'] = nr_report_df['LINK']
    nr_report_df['Site ID'] = nr_report_df['LINK'].str[:6]
    nr_report_df['Region'] = nr_report_df['Site ID'].apply(get_region)
    nr_report_df['Port'] = nr_report_df['MO Location']
    nr_report_df['Link Type'] = 'NR'
    nr_report_df['Description'] = 'Bad Rx Level. Авария на: ' + nr_report_df['Site ID']
    nr_report_df['Value'] = nr_report_df['rsl_count'].round(2)
    nr_report_df['Time'] = '-'
    nr_report_df['Severity'] = nr_report_df['Value'].apply(get_severity)
    nr_report_df['Action to'] = 'FLM'
    nr_report_df = nr_report_df[['Request Type', 'Sub Type', 'Link name', 'Site ID', 'Region', 'Port', 'Link Type', 'Description', 'Value', 'Time', 'Severity', 'Action to']]

    # Вызов process_link_status для добавления статуса линков
    nr_report_df = process_link_status(nr_report_df, stand_file, atoll_file)

    # saving результат в buffer Excel
    excel_buffer = BytesIO()
    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
        nr_report_df.to_excel(writer, sheet_name='NR Report Page', index=False)
        all_nr_agg.to_excel(writer, sheet_name='Combined Page', index=False)
        
    # with pd.ExcelWriter(NR_REPORT, engine='openpyxl', mode='a') as writer:
    #     all_nr_agg.to_excel(writer, sheet_name='Test Page 2', index=False)

    if progress_callback: # updating the progress
        progress_callback(100)

    st.write(nr_report_df)
    
    return excel_buffer, None



def rtn_report(files, mw_tt_file, progress_callback=None):
    if progress_callback:
        progress_callback(10)

    if isinstance(files, list):
        files = files[0]   # Для RTN берем первый файл

    df = pd.read_excel(files, skiprows=7, sheet_name='Sheet1')
    rtn_df = None
    if "History_Performance_Data" in files.name:
            rtn_df = df

    if progress_callback:
        progress_callback(20)

    rtn_df['End Time'] = pd.to_datetime(rtn_df['End Time']).dt.date
    rtn_df = rtn_df[(rtn_df['Performance Event'] == 'TSL_AVG(dbm)') | (rtn_df['Performance Event'] == 'RSL_AVG(dbm)')]

    rtn_df = pd.pivot_table(rtn_df, values='Value CUR', index='Monitored Object', columns=['Performance Event'], aggfunc='mean').reset_index()

    rtn_df = rtn_df[~rtn_df['TSL_AVG(dbm)'].between(-100, 0)]
    rtn_df = rtn_df[~rtn_df['RSL_AVG(dbm)'].between(-100, -77)]
    rtn_df = rtn_df[~rtn_df['RSL_AVG(dbm)'].between(-49, 0)]

    if progress_callback:
        progress_callback(50)

    rtn_df['RTN Site List'] = [[] for _ in range(len(rtn_df))]
    rtn_df['RTN LINK'] = pd.Series(dtype='object')

    if progress_callback:
        progress_callback(60)

    regexRTN = re.compile(r'^[A-Za-z]{2}\d{4}$|^[A-Za-z]{3}\d{3}$|^[A-Za-a]{4}\d{2}')

    for i in rtn_df.index:
        rtn_df['RTN Site List'][i] = re.split(r'[-_.(): ]', rtn_df['Monitored Object'][i])
        rtn_df['RTN Site List'][i] = [j for j in rtn_df['RTN Site List'][i] if regexRTN.search(j)]

    rtn_df['RTN Site List Str'] = rtn_df['RTN Site List'].apply(lambda x: '-'.join(x))
    min_rsl_idx = rtn_df.groupby('RTN Site List Str')['TSL_AVG(dbm)'].idxmin()
    rtn_df = rtn_df.loc[min_rsl_idx]
    rtn_df = rtn_df.reset_index()


    def checks_two_array(list1, list2):
        return list1[0] in list2[1:] and list2[0] in list1[1:]
    

    for i in rtn_df.index:
        if not pd.isna(rtn_df['RTN LINK'][i]):
            continue
        link_name = rtn_df['Monitored Object'][i]
        site_list = rtn_df['RTN Site List'][i]
        rtn_df['RTN LINK'][i] = link_name
        for j in range(i+1, len(rtn_df)):
            if not pd.isna(rtn_df['RTN LINK'][j]):
                continue
            if checks_two_array(site_list, rtn_df['RTN Site List'][j]):
                rtn_df['RTN LINK'][j] = link_name
            else:
                continue
    
    rtn_df = rtn_df.drop(columns=['index', 'TSL_AVG(dbm)',], axis=1)

    rsl_first_idx = rtn_df.groupby('RTN LINK')['RSL_AVG(dbm)'].head(1).index
    rtn_df = rtn_df.loc[rsl_first_idx]

    part_for_drop = [
    '-ODU-1(RTNRF-1)-RTNRF:1', '-MXXI4B-1(IF)-RTNRF:1',
    '-DMD4-1(IF1)-RTNRF:1', '-DMD4-2(IF2)-RTNRF:1', 
    '-MODU-2(RTNRF-2)-RTNRF:1', '-MODU-1(RTNRF-1)-RTNRF:2',
    '-MODU-2(RTNRF-2)-RTNRF:2', '-MODU-1(RTNRF-1)-RTNRF:1', 
    '-DMD4-1(NM1500-NM1127_A)-RTNRF:1', '-DMD4-2(NM1500-NM1127_B)-RTNRF:1', 
    '-DMD4-1(NM1507-NM1127)-RTNRF:1', '-DMD4-2(NM1507-NM1127)-RTNRF:1', 
    '-MXXI4B-1(SM0358-SM2496_Eband)-RTNRF:1', '-MXXI4B-1(SM2285-SM2496_Eband)-RTNRF:1', 
    '-MXXI4B-1(SM2496-SM0358_Eband)-RTNRF:1', '-MXXI4B-1(SM2496-SM2627_Eband)-RTNRF:1', 
    '-MXXI4B-1(SM2421-SM2496_Eband)-RTNRF:1', '-MXXI4B-1(SM2496-SM2285_Eband)-RTNRF:1', 
    '-MXXI4B-1(SM2627SM2496_Eband)-RTNRF:1', '-MXXI4B-1(SM2496-SM2421_Eband)-RTNRF:1', 
    '-DMD4-1(NM2294-NM0566_1)-RTNRF:1', '-DMD4-2(NM2294-NM0566_2)-RTNRF:1', 
    '-DMD4-1(NM0566-NM2294_1)-RTNRF:1', '-DMD4-2(NM0566-NM2294_2)-RTNRF:1', 
    '-DMD4-1(NM1501-NM1084)-RTNRF:1', '-DMD4-2(NM1501-NM1084)-RTNRF:1', 
    '-ODU-1(SR1005-SR0592 XPIC V)-RTNRF:1', '-ODU-1(SR1005-SR0592 XPIC H)-RTNRF:1', 
    '-DMD4-1(NM0566-NM2480_1)-RTNRF:1', '-DMD4-2(NM0566-NM2480_2)-RTNRF:1', 
    '-DMD4-1(NM2480-NM0566)-RTNRF:1', '-DMD4-2(NM2480-NM0566)-RTNRF:1', 
    '-DMD4-1(NM1205-NM0160_A)-RTNRF:1', '-DMD4-2(NM1205-NM0160_B)-RTNRF:1', 
    '-ODU-1(SR0592-SR1005 XPIC V)-RTNRF:1', '-ODU-1(SR0592-SR1005 XPIC H)-RTNRF:1',
    ]

    for idx in rtn_df.index:
        link_value = rtn_df.at[idx, 'RTN LINK']
        for part in part_for_drop:
            link_value = link_value.replace(part, '')
        rtn_df.at[idx, 'RTN LINK'] = link_value
    
    if progress_callback:
        progress_callback(80)

    process_mw_rtn = process_mw_tt(mw_tt_file)

    rtn_df = rtn_df[~rtn_df['Monitored Object'].isin(process_mw_rtn['SITE_ID'])]
    rtn_df = rtn_df[~rtn_df['RTN LINK'].isin(process_mw_rtn['SITE_ID'])]
    rtn_df = rtn_df[~rtn_df['RTN Site List Str'].isin(process_mw_rtn['SITE_ID'])]

    rtn_df = rtn_df[~rtn_df['Monitored Object'].isin(process_mw_rtn['MW_SITES_COMBINED'])]
    rtn_df = rtn_df[~rtn_df['RTN LINK'].isin(process_mw_rtn['MW_SITES_COMBINED'])]
    rtn_df = rtn_df[~rtn_df['RTN Site List Str'].isin(process_mw_rtn['MW_SITES_COMBINED'])]

    rtn_df = rtn_df[~rtn_df['Monitored Object'].isin(process_mw_rtn['MW_SITES_REVERSED'])]
    rtn_df = rtn_df[~rtn_df['RTN LINK'].isin(process_mw_rtn['MW_SITES_REVERSED'])]
    rtn_df = rtn_df[~rtn_df['RTN Site List Str'].isin(process_mw_rtn['MW_SITES_REVERSED'])]

    rtn_report_df = rtn_df.copy()
    rtn_report_df['Request Type'] = 'MW'
    rtn_report_df['Sub Type'] = 'Bad Rx level'
    rtn_report_df['Link name'] = rtn_report_df['RTN LINK']
    rtn_report_df['Site ID'] = rtn_report_df['RTN LINK'].str[:6]
    rtn_report_df['Region'] = rtn_report_df['Site ID'].apply(get_region)
    rtn_report_df['Port'] = '-'
    rtn_report_df['Link Type'] = 'RTN'
    rtn_report_df['Description'] = 'Bad Rx Level. Авария на: ' + rtn_report_df['Site ID']
    rtn_report_df['Value'] = rtn_report_df['RSL_AVG(dbm)'].round(2)
    rtn_report_df['Time'] = '-'
    rtn_report_df['Severity'] = rtn_report_df['Value'].apply(get_severity)
    rtn_report_df['Action to'] = 'FLM'
    rtn_report_df = rtn_report_df[['Request Type', 'Sub Type', 'Link name', 'Site ID', 'Region', 'Port', 'Link Type', 'Description', 'Value', 'Time', 'Severity', 'Action to']]


    excel_buffer = BytesIO()
    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
        rtn_report_df.to_excel(writer, sheet_name='RTN Report Page', index=False)
        rtn_df.to_excel(writer, sheet_name='Combined Page', index=False)

    if progress_callback:
        progress_callback(100)

    st.write(rtn_report_df)
    
    return excel_buffer, None



def mss_report(files, mw_tt_file, progress_callback=None):
    if progress_callback:
        progress_callback(10)

    if isinstance(files, list):
        files = files[0]   # Для RTN берем первый файл

    df = pd.read_csv(files, sep=',')
    mss_df = df
    mss_df.to_excel(files)

    if progress_callback:
        progress_callback(20)
    
    mss_df = mss_df.drop( # droppin' useless columns for bad rx report
    columns=[
        'Time Logged', 'Elapsed Time', 'Elapsed Time Periodic',
        'Period End Time', 'Period End Time Periodic', 'Suspect Interval Flag', 
        'Average Level Periodic (dBm)', 'Granularity Period', 'Granularity Period Periodic', 
        'Maximum Level (dBm)', 
        'Maximum Level Periodic (dBm)', 
        'Minimum Level (dBm)', 
        'Minimum Level Periodic (dBm)', 
        'Num Suppressed Intervals', 'Num Suppressed Intervals Periodic', 
        'Design vs Actual Deviation (dB)', 
        'Design vs Actual Deviation Periodic (dB)', 
        'Install vs Actual Deviation (dB)', 
        'Install vs Actual Deviation Periodic (dB)', 'History Created','Periodic Time', 'Record Type','Suspect'
    ], axis=1)

    if progress_callback:
        progress_callback(30)

    
    mss_df['Time Captured'] = pd.to_datetime(mss_df['Time Captured'].str.split(' ').str[0],errors='coerce').dt.date # reformattin this Series

    if progress_callback:
        progress_callback(40)

    mss_df = mss_df.pivot_table(
    index=['Monitored Object', 'Site Name'],
    columns='Time Captured',
    values='Average Level (dBm)',
    aggfunc='mean'
    )

    if progress_callback:
        progress_callback(50)

    mss_df['Mean RSL'] = mss_df.mean(axis=1)
    mss_df = mss_df[~mss_df['Mean RSL'].between(-100, -80)]
    mss_df = mss_df[~mss_df['Mean RSL'].between(-48, 0)]

    if progress_callback:
        progress_callback(80)

    mss_df = mss_df.reset_index()
    mss_df = mss_df.drop_duplicates(subset='Site Name', keep="first")

    process_mw_mss = process_mw_tt(mw_tt_file)

    mss_df['Site Name Str'] = mss_df['Site Name'].str[:6]

    mss_df = mss_df[~mss_df['Site Name'].isin(process_mw_mss['SITE_ID'])]
    mss_df = mss_df[~mss_df['Site Name Str'].isin(process_mw_mss['SITE_ID'])]
    mss_df = mss_df[~mss_df['Site Name'].isin(process_mw_mss['MW_SITES_COMBINED'])]
    mss_df = mss_df[~mss_df['Site Name'].isin(process_mw_mss['MW_SITES_REVERSED'])]

    mss_report_df = mss_df.copy()
    mss_report_df['Request Type'] = 'MW'
    mss_report_df['Sub Type'] = 'Bad Rx level'
    mss_report_df['Link name'] = mss_report_df['Site Name']
    mss_report_df['Site ID'] = mss_report_df['Site Name'].str[:6]
    mss_report_df['Region'] = mss_report_df['Site ID'].apply(get_region)
    mss_report_df['Port'] = mss_report_df['Monitored Object']
    mss_report_df['Link Type'] = 'MSS'
    mss_report_df['Description'] = 'Bad Rx Level. Авария на: ' + mss_report_df['Site ID']
    mss_report_df['Value'] = mss_report_df['Mean RSL'].round(2)
    mss_report_df['Time'] = '-'
    mss_report_df['Severity'] = mss_report_df['Value'].apply(get_severity)
    mss_report_df['Action to'] = 'FLM'
    mss_report_df = mss_report_df[['Request Type', 'Sub Type', 'Link name', 'Site ID', 'Region', 'Port', 'Link Type', 'Description', 'Value', 'Time', 'Severity', 'Action to']]

    excel_buffer = BytesIO()
    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
        mss_report_df.to_excel(writer, sheet_name='MSS Report Page', index=False)
        mss_df.to_excel(writer, sheet_name='Combined Page', index=False)
    
    if progress_callback:
        progress_callback(100)
    
    st.write(mss_df)

    return excel_buffer, None




