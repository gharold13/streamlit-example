from collections import namedtuple
import altair as alt
import math
import pandas as pd
import streamlit as st
import snowflake.connector
import sys
import csv
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
    # columns = ['location', 'campaign', 'spend_date', 'spend']
    dataframe = pd.read_csv(uploaded_file, skiprows=1)
    dataframe.columns = [it.upper() for it in dataframe.columns]    
    st.write(dataframe)
    if st.button(label='Upload File'):
        write_pandas(conn, 
                     df=dataframe, 
                     table_name='FACT_SPEND_TEST', 
                     database=st.secrets['snowflake']['database'], 
                     schema=st.secrets['snowflake']['schema'])

#df = run_query("SELECT * from FOOD_INSPECTIONS_FULL")

#st.dataframe(df)

st.echo(sys.version)
