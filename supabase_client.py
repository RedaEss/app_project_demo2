from supabase import create_client, Client
import streamlit as st

def create_supabase_client():
    # Fetch Supabase URL and key from Streamlit secrets
    supabase_url = st.secrets["SUPABASE_URL"]
    supabase_key = st.secrets["SUPABASE_KEY"]
    # Create and return the Supabase client
    return create_client(supabase_url, supabase_key)
