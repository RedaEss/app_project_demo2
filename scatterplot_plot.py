import pandas as pd
import plotly.express as px

def prepare_scatterplot_df(df_status):
    # Extract hour and week_day from created_at
    # df_status['hour'] = df_status['created_at'].dt.hour
    # df_status['week_day'] = df_status['created_at'].dt.day_name()

    # # Group by hour and week_day and calculate the mean of ebike
    # sp_df = df_status.groupby(['hour', 'week_day']).agg(
    #     sp_moyenne_ebike=('ebike', 'mean')
    # ).round(2).reset_index()

    sp_df = df_status.groupby(['created_at']).agg(
    sp_moyenne_ebike=('ebike','mean')
    ).round(2).reset_index()

    ##
    df_status['created_at'] = pd.to_datetime(df_status['created_at']).dt.tz_localize(None)

    sp_df['week_day']=sp_df['created_at'].dt.day_name()
    sp_df['hour'] = sp_df['created_at'].dt.hour + sp_df['created_at'].dt.minute / 60

    return sp_df

def plot_scatterplot(sp_df):
    fig = px.scatter(sp_df, x='hour', y='sp_moyenne_ebike', color='week_day', title='Nombre de vélo disponible en moyenne par jour et par heure')

    # Customize the x-axis to have ticks every 10 minutes
    fig.update_layout(
        xaxis=dict(
            title='heure de la journée',
            tickmode='array',
            # tickvals=[i / 6 for i in range(0, 24 * 6 + 1, 1)],  # Ticks every 10 minutes
            tickvals=[i / 6 for i in range(0, 24 * 6 + 1, 1)],  # Ticks every 10 minutes
            ticktext=[f'{int(i // 6)}:{i % 6 * 10:02d}' for i in range(0, 24 * 6 + 1, 1)],  # Format HH:MM
            tickformat='%H:%M'
        ),
        yaxis=dict(
            title='Nombre de vélo disponible en moyenne'
        )
    )

    return fig
