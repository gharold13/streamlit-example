from collections import namedtuple
import altair as alt
import math
import pandas as pd
import streamlit as st
import snowflake.connector
import sys
from io import StringIO
from snowflake.connector.pandas_tools import write_pandas

"""
# HTS POC - dev branch
Edit `/streamlit_app.py` to customize this app to your heart's desire :heart:
If you have any questions, checkout our [documentation](https://docs.streamlit.io) and [community
forums](https://discuss.streamlit.io).
In the meantime, below is an example of what you can do with just a few lines of code:
"""

def init_connection():
    return snowflake.connector.connect(
        **st.secrets["snowflake"], client_session_keep_alive=True
    )

conn = init_connection()

@st.cache_data
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        dat = cur.fetchall()
        df = pd.DataFrame(dat, columns=[col[0] for col in cur.description])
        return df
    
uploaded_file = st.file_uploader("Choose a file")

if uploaded_file is not None:
    # Can be used wherever a "file-like" object is accepted:
    dataframe = pd.read_csv(uploaded_file)
    st.write(dataframe)
    st.button(label='Upload File', on_click=write_pandas(conn, uploaded_file, 'FACT_SPEND_TEST'))

#df = run_query("SELECT * from FOOD_INSPECTIONS_FULL")

#st.dataframe(df)

st.echo(sys.version)
