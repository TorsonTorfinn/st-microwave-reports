import streamlit as st
from datetime import datetime
from backend.mwLinksFunctions import mw_links_alarm, rtn_links_alarms
from backend.linksAlarmFunc import zenic_links_alarm


st.title('MicroWave Links Alarm Report')
st.sidebar.warning('Select Report Type')

vendor_pick = st.radio('Pick the Type:', ['NR', 'RTN'])

match vendor_pick:
    case 'NR':
        uploaded_file = st.file_uploader(f'Upload a file for {vendor_pick} MicroWave Links Alarms Report:')
        if uploaded_file:
            progress_bar = st.progress(0)

            def upd_progress_bar(progress):
                progress_bar.progress(progress)
            
            st.write(st.session_state)

            if 'excel_data' not in st.session_state:
                excel_data, error = zenic_links_alarm(uploaded_file, upd_progress_bar)
                st.session_state.excel_data = excel_data
                st.session_state.error = error

                if st.session_state.error:
                    st.error(st.session_state.error)
                else:
                    st.success(f"MWLinksAlarms file processed successfuly!")
                    current_date = datetime.now().strftime("%Y-%m-%d")
                    st.download_button(
                        label=f"Download MWLinksAlarm Report",
                        data=st.session_state.excel_data.getvalue(),
                        file_name=f"MWLinksAlarm_Report_{current_date}.xlsx",
                        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                )
    
    case 'RTN':
        uploaded_file = st.file_uploader(f'Upload a file for {vendor_pick} MicroWave Links Alarms Report:')
        if uploaded_file:
            progress_bar = st.progress(0)

            def upd_progress_bar(progress):
                progress_bar.progress(progress)
            
            st.write(st.session_state)

            if 'excel_data' not in st.session_state:
                excel_data, error = rtn_links_alarms(uploaded_file, upd_progress_bar)
                st.session_state.excel_data = excel_data
                st.session_state.error = error

                if st.session_state.error:
                    st.error(st.session_state.error)
                else:
                    st.success(f"MWLinksAlarms file processed successfuly!")
                    current_date = datetime.now().strftime("%Y-%m-%d")
                    st.download_button(
                        label=f"Download MWLinksAlarm Report",
                        data=st.session_state.excel_data.getvalue(),
                        file_name=f"MWLinksAlarm_Report_{current_date}.xlsx",
                        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                )