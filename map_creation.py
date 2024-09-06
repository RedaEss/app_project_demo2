import folium
from folium.plugins import MarkerCluster
from streamlit_folium import folium_static

def get_marker_color(percentage_ebike):
    # Determine marker color based on percentage_ebike
    if percentage_ebike < 5:
        return '#FF0000'  # Red
    elif percentage_ebike < 10:
        return '#FFA500'  # Orange
    elif percentage_ebike < 15:
        return '#FFFF00'  # Yellow
    elif percentage_ebike < 20:
        return '#008000'  # Green
    elif percentage_ebike < 25:
        return '#0000FF'  # Blue
    elif percentage_ebike < 30:
        return '#800080'  # Purple
    elif percentage_ebike < 40:
        return '#FFC0CB'  # Pink
    elif percentage_ebike < 50:
        return '#00FFFF'  # Cyan
    else:
        return '#FF00FF'  # Magenta

def create_map(df_station, filtered_df_moyenne_velo_disponible):
    # Create a map centered on the mean latitude and longitude of the stations
    m = folium.Map(location=[df_station['lat'].mean(), df_station['lon'].mean()], zoom_start=12)

    # Add markers for each station using MarkerCluster
    marker_cluster = MarkerCluster().add_to(m)

    for _, row in filtered_df_moyenne_velo_disponible.iterrows():
        popup_content = f"""
        <b>Nom de la station:</b> {row['name']}<br>
        <b>Capacité de la station:</b> {row['capacity']}<br>
        <b>Nombre de vélo disponible en moyenne:</b> {int(row['moyenne_bike_disponible'])}<br>
        <b>Nombre de vélo électrique disponible en moyenne:</b> {int(row['moyenne_ebike_disponible'])}<br>
        <b>Nombre de vélo mécanique disponible en moyenne:</b> {int(row['moyenne_mbike_disponible'])}<br>
        <b>% de vélo électrique disponible:</b> {row['pourcentage_ebike']}%<br>
        <b>% de vélo mécanique disponible:</b> {row['pourcentage_mbike']}%
        """
        marker_color = get_marker_color(row['pourcentage_ebike'])
        folium.Marker(
            location=[row['lat'], row['lon']],
            popup=folium.Popup(popup_content, max_width=300),
            tooltip=row['name'],
            icon=folium.Icon(color='green', icon_color=marker_color)
        ).add_to(marker_cluster)

    # Display the map in Streamlit
    folium_static(m)
