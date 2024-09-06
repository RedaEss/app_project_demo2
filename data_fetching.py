import streamlit as st
import pandas as pd


def fetch_data(_supabase_client):
    try:
        # Fetch data from Supabase tables
        response_status = _supabase_client.table('status_station_demo2').select('*').execute()
        response_station = _supabase_client.table('station_information_demo2').select('*').execute()

        # Convert responses to DataFrames
        df_status = pd.DataFrame(response_status.data)
        df_station = pd.DataFrame(response_station.data)

        # Convert latitude and longitude to float
        df_station['lat'] = df_station['lat'].astype(float)
        df_station['lon'] = df_station['lon'].astype(float)

        # Convert created_at to datetime and remove timezone information
        df_status['created_at'] = pd.to_datetime(df_status['created_at']).dt.tz_localize(None)
        df_status['last_reported'] = pd.to_datetime(df_status['last_reported'], utc=True) + pd.Timedelta(hours=2)

        df_status['created_at'] = df_status['created_at'].dt.floor('min')


        # Return the preprocessed DataFrames
        return df_status, df_station
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return pd.DataFrame(), pd.DataFrame()
