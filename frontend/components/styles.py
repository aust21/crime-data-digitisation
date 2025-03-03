import streamlit as st

def apply_styles():
    st.set_page_config(
        page_title='Crime Data'
        )
    page_bg_img = """
    <style>
    [data-testid="stAppViewContainer"] {
        background-color: #3f3b45;
    }
    [data-testid="stHeader"] {
        background-color: #3f3b45;
    }
    [data-testid="stToolbar"] {
        background-color: #3f3b45;
    }
    [data-testid="stSidebar"] {
        background-color: #f0f2f6;
    }
    [data-testid="stDataFrame"] {
        background-color: #3f3b45;
    }
    .main {
        background-color: #f0f2f6;
    }
    </style>
    """
    st.markdown(page_bg_img, unsafe_allow_html=True)
