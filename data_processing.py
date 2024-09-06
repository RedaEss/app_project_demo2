import pandas as pd

def null_or_mean(series):
    # Custom aggregation function to return mean or 0 if all values are null
    if series.isnull().all():
        return 0
    else:
        return series.mean()

# def filter_time_period(df_status, time_period):
#     # Filter the DataFrame based on the selected time period
#     if time_period == "Dimanche Soir":
#         start_time = pd.Timestamp("2024-09-01T18:00:00").tz_localize(None)
#         end_time = pd.Timestamp("2024-09-01T23:00:00").tz_localize(None)
#         filtered_df = df_status[(df_status['created_at'] >= start_time) & (df_status['created_at'] <= end_time)]
#     elif time_period == "Lundi Midi":
#         start_time = pd.Timestamp("2024-09-02T12:00:00").tz_localize(None)
#         end_time = pd.Timestamp("2024-09-02T13:00:00").tz_localize(None)
#         filtered_df = df_status[(df_status['created_at'] >= start_time) & (df_status['created_at'] <= end_time)]
#     else:
#         filtered_df = df_status
#     # Return the filtered DataFrame
#     return filtered_df


def filter_time_period(df_status, start_time, end_time):
    # Ensure start_time and end_time are pandas Timestamps
    start_time = pd.to_datetime(start_time)
    end_time = pd.to_datetime(end_time)

    # Check if start_time is more recent than end_time
    if start_time > end_time:
        raise ValueError("The start_time is more recent than the end_time.")

    # Filter the DataFrame based on the selected time period
    # if start_time == end_time:
    #     filtered_df = df_status[df_status['created_at'] == start_time]
    else:
        filtered_df = df_status[(df_status['created_at'] >= start_time) & (df_status['created_at'] <= end_time)]
        # filtered_df = df_status[(df_status['created_at'] > start_time) & (df_status['created_at'] <= end_time)]

    # Return the filtered DataFrame
    return filtered_df



def aggregate_data(filtered_df, df_station):
    # Merge filtered data with station information
    merged_df = pd.merge(filtered_df, df_station, on='station_id', how='left')

    # Aggregate data
    df_moyenne_velo_disponible = merged_df.groupby(['station_id', 'name', 'lat', 'lon', 'capacity']).agg(
        moyenne_bike_disponible=('num_bikes_available', 'mean'),
        moyenne_ebike_disponible=('ebike', 'mean'),
        moyenne_mbike_disponible=('mechanical', 'mean'),
        moyenne_fonctionnement=('is_installed', null_or_mean),
        borne_paiement=('is_renting', null_or_mean)
    ).reset_index()

    # Return the aggregated DataFrame
    return df_moyenne_velo_disponible

def calculate_percentages(df_moyenne_velo_disponible):
    # Calculate percentages
    df_moyenne_velo_disponible['pourcentage_ebike'] = ((df_moyenne_velo_disponible['moyenne_ebike_disponible'] / df_moyenne_velo_disponible['capacity']).fillna(0).round(2)) * 100
    df_moyenne_velo_disponible['pourcentage_mbike'] = ((df_moyenne_velo_disponible['moyenne_mbike_disponible'] / df_moyenne_velo_disponible['capacity']).fillna(0).round(2)) * 100
    # Return the DataFrame with calculated percentages
    return df_moyenne_velo_disponible

def filter_data(df_moyenne_velo_disponible, 
                # percentage_ebike_range,
                station_fonctionnement, borne_paiement, moyenne_ebike_disponible_range):
    # Filter the DataFrame based on the selected percentage_ebike range
    filtered_df_moyenne_velo_disponible = df_moyenne_velo_disponible[
        # (df_moyenne_velo_disponible['pourcentage_ebike'] >= percentage_ebike_range[0]) &
        # (df_moyenne_velo_disponible['pourcentage_ebike'] <= percentage_ebike_range[1]) &
        (df_moyenne_velo_disponible['moyenne_ebike_disponible'] >= moyenne_ebike_disponible_range[0]) &
        (df_moyenne_velo_disponible['moyenne_ebike_disponible'] <= moyenne_ebike_disponible_range[1])
    ]

    # Filter based on "Station en fonctionnement"
    if station_fonctionnement != "All":
        if station_fonctionnement == "Oui":
            filtered_df_moyenne_velo_disponible = filtered_df_moyenne_velo_disponible[filtered_df_moyenne_velo_disponible['moyenne_fonctionnement'] == 1]
        elif station_fonctionnement == "Non":
            filtered_df_moyenne_velo_disponible = filtered_df_moyenne_velo_disponible[filtered_df_moyenne_velo_disponible['moyenne_fonctionnement'] == 0]
        else:
            filtered_df_moyenne_velo_disponible = filtered_df_moyenne_velo_disponible[(filtered_df_moyenne_velo_disponible['moyenne_fonctionnement'] != 0) & (filtered_df_moyenne_velo_disponible['moyenne_fonctionnement'] != 1)]

    # Filter based on "Borne de paiement disponible"
    if borne_paiement != "All":
        if borne_paiement == "Oui":
            filtered_df_moyenne_velo_disponible = filtered_df_moyenne_velo_disponible[filtered_df_moyenne_velo_disponible['borne_paiement'] == 1]
        elif borne_paiement == "Non":
            filtered_df_moyenne_velo_disponible = filtered_df_moyenne_velo_disponible[filtered_df_moyenne_velo_disponible['borne_paiement'] == 0]
        else:
            filtered_df_moyenne_velo_disponible = filtered_df_moyenne_velo_disponible[(filtered_df_moyenne_velo_disponible['borne_paiement'] != 0) & (filtered_df_moyenne_velo_disponible['borne_paiement'] != 1)]

    # Return the filtered DataFrame
    return filtered_df_moyenne_velo_disponible

def prepare_table_data(filtered_df_moyenne_velo_disponible):
    # Select and reorder the columns
    filtered_table_data = filtered_df_moyenne_velo_disponible[['station_id', 'name', 'capacity', 'moyenne_ebike_disponible', 'moyenne_mbike_disponible', 'pourcentage_ebike', 'pourcentage_mbike', 'moyenne_fonctionnement', 'borne_paiement', 'lat','lon']].copy()

    # # Create the 'adresse' column using 'lat' and 'lon' columns
    # filtered_table_data['adresse'] = filtered_df_moyenne_velo_disponible.apply(lambda row: f"Latitude: {row['lat']}, Longitude: {row['lon']}", axis=1)

    # Transform the values of 'moyenne_fonctionnement'
    filtered_table_data['moyenne_fonctionnement'] = filtered_table_data['moyenne_fonctionnement'].apply(lambda x: "Oui" if x == 1 else ("Non" if x == 0 else "Incident"))

    # Transform the values of 'borne_paiement'
    filtered_table_data['borne_paiement'] = filtered_table_data['borne_paiement'].apply(lambda x: "Oui" if x == 1 else ("Non" if x == 0 else "Incident"))

    return filtered_table_data