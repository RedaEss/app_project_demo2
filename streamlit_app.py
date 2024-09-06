import pydeck as pdk
import streamlit as st
from supabase_client import create_supabase_client
from data_fetching import fetch_data
from data_processing import filter_time_period, aggregate_data, calculate_percentages, filter_data, prepare_table_data
from map_creation import create_map
# from linechart_plot import plot_linechart
import pandas as pd
# from prepare_linechart_df import prepare_linechart_df
from scatterplot_plot import prepare_scatterplot_df, plot_scatterplot
import requests
from requests.auth import HTTPBasicAuth


st.set_page_config(
    page_title="Final project - Data Sc & Eng - Lead - Réda ES-SAKHI",
    layout="wide"
)
# Initialize Supabase client
supabase_client = create_supabase_client()

# Fetch and preprocess data

df_status, df_station = fetch_data(supabase_client)

unique_times = df_status['created_at'].dt.strftime('%Y-%m-%d %H:%M').unique()

# Determine the oldest and most recent values from the 'created_at' column
first_report = df_status['created_at'].min().strftime('%Y-%m-%d %H:%M')
last_report = df_status['created_at'].max().strftime('%Y-%m-%d %H:%M')

# # Prepare data for the scatter plot
# scatterplot_df = prepare_scatterplot_df(df_status)

# # Plot the scatter chart
# # st.title("Nombre de vélo disponible en moyenne par jour et par heure")
# st.plotly_chart(plot_scatterplot(scatterplot_df))



# Airflow API details
AIRFLOW_API_URL = "http://localhost:8080/api/v1/dags/bike_data_dag_demo2"

# Load Airflow credentials from secrets
USERNAME = st.secrets["airflow"]["username"]
PASSWORD = st.secrets["airflow"]["password"]

# Function to unpause the DAG
def unpause_dag():
    response = requests.patch(
        f"{AIRFLOW_API_URL}",
        auth=HTTPBasicAuth(USERNAME, PASSWORD),
        json={"is_paused": False}
    )
    return response

# Function to stop (pause) the DAG
def pause_dag():
    response = requests.patch(
        f"{AIRFLOW_API_URL}",
        auth=HTTPBasicAuth(USERNAME, PASSWORD),
        json={"is_paused": True}
    )
    return response



# Sidebar for time period selection
with st.sidebar:

    
    # st.title("Choisir une période")
    st.sidebar.text("Choisir une période, date, heure")
    start_time = st.selectbox("Début", unique_times, index=unique_times.tolist().index(first_report))
    end_time = st.selectbox("Fin", unique_times, index=unique_times.tolist().index(last_report) )

    
    if pd.to_datetime(start_time) > pd.to_datetime(end_time):
        st.warning("Choisir une date de début avant ladate de fin")
    
    
    
    

    # st.title("Station en fonctionnement")
    # st.sidebar.text("Station en fonctionnement")
    station_fonctionnement = st.selectbox(
        "Station en fonctionnement",
        ("All", "Oui", "Non", "Incident")
    )

    # st.title("Borne de paiement disponible")
    # st.sidebar.text("Borne de paiement disponible")
    borne_paiement = st.selectbox(
        "Borne de paiement disponible",
        ("All", "Oui", "Non", "Incident")
    )



    st.write("Choisir une valeur ou un plage de valeurs pour le : ")
    moyenne_ebike_disponible_range = st.slider(
        "Nombre de vélo disponible en moyenne",
        0, 76, (0, 76)
    )

    st.sidebar.text("Dernière mise à jour :")
    st.sidebar.text(last_report)

    st.write("Mettre à jour la disponibilité des vélos électriques par station [Vélib Métropole](https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/gbfs.json)")

    # "Start Updating" button to unpause the DAG
    if st.button("Lancer mise à jour"):
        response = unpause_dag()
        if response.status_code == 200:
            st.sidebar.success("Lancement de la mise à jour!")
        else:
            st.sidebar.error(f"Failed to unpause DAG: {response.text}")

    # "Stop Updating" button to pause the DAG
    if st.button("Arrêter mise à jour"):
        response = pause_dag()
        if response.status_code == 200:
            st.sidebar.success("Arrêt de la mise à jour!")
        else:
            st.sidebar.error(f"Failed to pause DAG: {response.text}")


# Check if the start_time is more recent than the end_time
if pd.to_datetime(start_time) > pd.to_datetime(end_time):
    st.warning("The start_time is more recent than the end_time. Please adjust the filters.")
else:
# filtered_df: DataFrame filtered based on the selected time period
    filtered_df = filter_time_period(df_status, start_time, end_time)

    # if filtered_df is None :
    #     st.info ("Please verify the time period selected", icon = "i")
    #     st.stop



    # Aggregate data
    # df_moyenne_velo_disponible: DataFrame containing aggregated data for each station
    df_moyenne_velo_disponible = aggregate_data(filtered_df, df_station)

    # Calculate percentages
    # df_moyenne_velo_disponible: DataFrame with additional columns for percentage of ebikes and mechanical bikes
    df_moyenne_velo_disponible = calculate_percentages(df_moyenne_velo_disponible)

    # Filter data based on sidebar inputs
    # filtered_df_moyenne_velo_disponible: DataFrame filtered based on percentage ebike range, station functionality, and payment terminal availability
    filtered_df_moyenne_velo_disponible = filter_data(df_moyenne_velo_disponible, 
                                                    #   percentage_ebike_range, 
                                                    station_fonctionnement, borne_paiement, moyenne_ebike_disponible_range)

    # # Create and display the map
    # # The map is created using the filtered data and displayed in the Streamlit app
    # create_map(df_station, filtered_df_moyenne_velo_disponible)

        # Calculate the average of moyenne_ebike_disponible
    average_ebike_disponible = filtered_df_moyenne_velo_disponible['moyenne_ebike_disponible'].mean()

    # Filter the DataFrame to get stations with 0 electric bikes available
    stations_zero_ebike = filtered_df_moyenne_velo_disponible[filtered_df_moyenne_velo_disponible['moyenne_ebike_disponible'] == 0]

    nb_station = len(stations_zero_ebike)

    # Use Streamlit's columns to display the metric and the table side by side
    col1, col2 = st.columns([1, 2])

    with col1:
        
        st.metric(label="Nombre de vélo électrique disponible en moyenne :", value=f"{average_ebike_disponible:.2f}")
        # st.metric(value=f"{average_ebike_disponible:.2f}")

    with col2:
        # st.write("Station avec 0 vélo électrique")
        st.metric(label="Nombre de station avec 0 vélo électrique :", value=f"{nb_station}")
        st.dataframe(stations_zero_ebike[[
            # 'station_id', 
            'name', 'moyenne_ebike_disponible']],
                    column_config={
                        # "station_id": st.column_config.NumberColumn(
                        #     "Id de la station",
                        #     format="%d"
                        # ),
                        "name": st.column_config.Column(
                            "Nom de la station"
                        ),
                        "moyenne_ebike_disponible": st.column_config.NumberColumn(
                            "Vélo électrique disponible en moyenne",
                            step=".2f"
                        ),
                    }
                    )
        
    # Create and display the map
    # The map is created using the filtered data and displayed in the Streamlit app
    
    st.title("Vélib Carte - bornes et disponiblité des vélos")
    create_map(df_station, filtered_df_moyenne_velo_disponible)

    filtered_table = prepare_table_data(filtered_df_moyenne_velo_disponible)

    # Display the filtered data in the main panel
    st.title("Vélib Data - bornes et disponiblité des vélos")
    with st.expander("Cliquer ici pour dérouler le tableau"):
        st.dataframe(filtered_table,
                    column_config={
                        "station_id":st.column_config.NumberColumn(
                            "Id de la sation",
                            format="%d"

                        ),
                        "name":st.column_config.Column(
                            "Nom de la sation",
                            #  format="%d"

                        ),
                        "capacity":st.column_config.NumberColumn(
                            "Capacité de la station"
                        ),
                        "moyenne_bike_disponible":st.column_config.NumberColumn(
                            "Vélo disponible en moyenne",
                            step=".2f"
                        ),
                        "moyenne_ebike_disponible":st.column_config.NumberColumn(
                            "Vélo électrique disponible en moyenne",
                            step=".2f"
                        ),
                        "moyenne_mbike_disponible":st.column_config.NumberColumn(
                            "Vélo mécanique disponible en moyenne",
                            step=".2f"
                        ),
                        "pourcentage_ebike":st.column_config.NumberColumn(
                            "'%' de Vélo électrique disponible en moyenne",
                            format="%d%%"
                        ),
                        "pourcentage_mbike":st.column_config.NumberColumn(
                            "'%' de Vélo mécanique disponible en moyenne",
                            format="%d%%"
                        ),
                        "moyenne_fonctionnement":st.column_config.Column(
                            "Station en fonctionnement",
                            
                        ),
                        "borne_paiement":st.column_config.Column(
                            "Borne de paiement disponible",
                            #  format="%.2f%%"
                        ),
                        "lon":st.column_config.Column(
                            "Coord. Géo - Longitude",
                            #  format="%.2f%%"
                        ),
                        "lat":st.column_config.Column(
                            "Coord. Géo - Latitude",
                            #  format="%.2f%%"
                        ),
                            
                    }
                    )


    # Define the color scale
    color_range = [
        [50, 205, 50],  # Lime color low values
        [254, 217, 118],  # Yellow for medium values
        [254, 178, 76],   # Orange for higher values
        [253, 141, 60],   # Dark orange for very high values
        [240, 59, 32],    # Red for the highest values
    ]

    st.title("Carte de disponiblité des vélos électrique")
    st.markdown("""
    ### Description de la Carte
    disponibilité des e-bikes dans différentes zones géographiques. Les couleurs représentent le nombre moyen de e-bikes disponibles dans chaque zone.

    
    
    """)

        # Display the color palette
    color_palette_html = """
    <div style="display: flex; align-items: center;">
        <div style="width: 20px; height: 20px; background-color: rgb(50, 205, 50); margin-right: 10px;"></div>
        <span>Faible disponibilité des e-bikes</span>
    </div>
    <div style="display: flex; align-items: center;">
        <div style="width: 20px; height: 20px; background-color: rgb(254, 217, 118); margin-right: 10px;"></div>
        <span>Disponibilité moyenne</span>
    </div>
    <div style="display: flex; align-items: center;">
        <div style="width: 20px; height: 20px; background-color: rgb(254, 178, 76); margin-right: 10px;"></div>
        <span>Disponibilité élevée</span>
    </div>
    <div style="display: flex; align-items: center;">
        <div style="width: 20px; height: 20px; background-color: rgb(240, 59, 32); margin-right: 10px;"></div>
        <span>Très haute disponibilité</span>
    </div>
    """

    st.markdown(color_palette_html, unsafe_allow_html=True)



    # Check if the filtered_table is empty
    if filtered_table.empty:
        st.warning("No data available for the selected filters. Please adjust the filters.")
    else:
        hexagon_layer = pdk.Layer(
            "HexagonLayer",
            filtered_table,
            get_position=["lon", "lat"],
            get_weight="moyenne_ebike_disponible",
            auto_highlight=True,
            elevation_scale=0,  # No elevation for 2D appearance
            pickable=True,
            extruded=False,  # Disable 3D extrusion
            coverage=1,
            radius=200,  # Adjust the size of the hexagons (in meters)
            color_range=color_range,  # Define the color scale
            opacity = 0.5
        )

        # ViewState with increased zoom level
        view_state = pdk.ViewState(
            latitude=filtered_table['lat'].mean(),
            longitude=filtered_table['lon'].mean(),
            zoom=10,  # Increased zoom level to center the map
            pitch=0,  # No pitch for a top-down 2D view
        )

        # Create the deck.gl map
        r = pdk.Deck(layers=[hexagon_layer],
                    initial_view_state=view_state,
                    )

        # Display the map in Streamlit
        st.pydeck_chart(r)


# Prepare data for the scatter plot
scatterplot_df = prepare_scatterplot_df(df_status)

# Plot the scatter chart
# st.title("Nombre de vélo disponible en moyenne par jour et par heure")
st.plotly_chart(plot_scatterplot(scatterplot_df))