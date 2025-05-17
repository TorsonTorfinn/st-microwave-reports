import streamlit as st
import pandas as pd
from io import BytesIO
from backend.nce_alarms_api import response_func

st.title("NCE Alarms Report")

def json_to_dataframe(alarms_json):
    """Преобразует JSON из API в DataFrame."""
    if not alarms_json or "alarm" not in alarms_json:
        return pd.DataFrame()

    return pd.json_normalize(alarms_json["alarm"])


# Получение данных
alarms_json = response_func()
alarms_df = json_to_dataframe(alarms_json)

if not alarms_df.empty:
    # Отображение таблицы
    st.write("Active Alarms Data:")
    st.dataframe(alarms_df)

    # Кнопка для скачивания Excel файла
    excel_buffer = BytesIO()
    with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
        alarms_df.to_excel(writer, sheet_name="Alarms", index=False)
    excel_buffer.seek(0)

    st.download_button(
        label="Download Alarms Report",
        data=excel_buffer,
        file_name="NCE_Alarms_Report.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
else:
    st.error("No active alarms found.")
