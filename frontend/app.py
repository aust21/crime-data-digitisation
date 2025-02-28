import streamlit as st
import pandas as pd
# from components.sidebar import load_sidebar
from components.styles import apply_styles
from data.fetch import fetch_aggrigated_by_cat
from data.aws import download_files_aws
from configuration import original_file

# Apply Styles
apply_styles()

# Sidebar
# load_sidebar()

# Title
st.markdown(
    "<h1 style='text-align: center; color: white;'>Crime Stats</h1>",
    unsafe_allow_html=True
)

# Dropdown to select aggregation method
option = st.selectbox(
    "Select Aggregation",
    ["Original Stats", "Aggregated by category"]
)

# Process Data Based on Selection
if option == "Original Stats":
    df_result = original_file
elif option == "Aggregated by category":
    df_result = fetch_aggrigated_by_cat()

# DataFrame
st.dataframe(df_result, use_container_width=True)
