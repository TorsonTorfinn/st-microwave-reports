import streamlit as st
from backend.functions import nr_report, rtn_report, mss_report, process_mw_tt
from datetime import datetime

st.title('Bad Rx Lvl Report')
st.sidebar.warning('Select Report Type')

stand_file = st.file_uploader('Upload NR Series RadioLink Info File')
atoll_file = st.file_uploader('Upload BIT Links Atoll File')

mw_tt_file = st.file_uploader('Upload Proccessing Works MWTT File')

if mw_tt_file:
    process_mw_tt(mw_tt_file)
    report_type = st.selectbox('Select Link Type', ['None', 'NR', 'RTN', 'MSS'], index=0)

    if report_type != 'None':
        uploaded_files = st.file_uploader(f'Upload Excel Files for {report_type} Report', accept_multiple_files=True)

        if uploaded_files and report_type == 'NR':
            progress_bar = st.progress(0) # прогресс-бар

            def update_progress(progress):
                progress_bar.progress(progress)

            # obrabotk файлов и result в session_state
            if 'excel_data' not in st.session_state:
                excel_data, error = nr_report(uploaded_files, mw_tt_file, stand_file, atoll_file, update_progress)  # Выполняем обработку
                st.session_state.excel_data = excel_data
                st.session_state.error = error

            # cehck for errors
            if st.session_state.error:
                st.error(st.session_state.error)
            else:
                st.success(f"{report_type} Files processed successfully!")

                current_date = datetime.now().strftime("%Y-%m-%d")

                st.download_button(
                    label=f"Download {report_type} Report",
                    data=st.session_state.excel_data.getvalue(),
                    file_name=f'{report_type}_Report_{current_date}.xlsx',
                    mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                )
        
        elif uploaded_files and report_type == 'RTN':
            progress_bar = st.progress(0)

            def update_progress(progress):
                progress_bar.progress(progress)
            
            if 'excel_data' not in st.session_state:
                excel_data, error = rtn_report(uploaded_files, mw_tt_file, update_progress)  # Выполняем обработку
                st.session_state.excel_data = excel_data
                st.session_state.error = error
            
            if st.session_state.error:
                st.error(st.session_state.error)
            else:
                st.success(f"{report_type} Files processed successfully!")

                current_date = datetime.now().strftime("%Y-%m-%d")

                # btn для скачивания Excel файла с датой в имени
                st.download_button(
                    label=f"Download {report_type} Report",
                    data=st.session_state.excel_data.getvalue(),
                    file_name=f'{report_type}_Report_{current_date}.xlsx',
                    mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                )
            
        elif uploaded_files and report_type == 'MSS':
            progress_bar = st.progress(0)

            def update_progress(progress):
                progress_bar.progress(progress)
            
            st.write(st.session_state)

            if 'excel_data' not in st.session_state:
                excel_data, error = mss_report(uploaded_files, mw_tt_file, update_progress)
                st.session_state.excel_data = excel_data
                st.session_state.error = error
            
            if st.session_state.error:
                st.error(st.session_state.error)
            else:
                st.success(f"{report_type} Files processed successfully!")

                current_date = datetime.now().strftime("%Y-%m-%d")

                st.download_button(
                    label=f"Download {report_type} Report",
                    data=st.session_state.excel_data.getvalue(),
                    file_name=f'{report_type}_Report_{current_date}.xlsx',
                    mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                )