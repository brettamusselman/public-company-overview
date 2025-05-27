from flask import Flask
import dash
from dash import html, dcc, Input, Output, State
import dash_table
import plotly.graph_objs as go
import pandas as pd
from datetime import datetime
from google.cloud import bigquery

# Initialize BigQuery client
project_id = "public-company-overview"
client = bigquery.Client(project=project_id)

# Get available tickers dynamically
def get_available_tickers():
    query = "SELECT DISTINCT Ticker FROM `public-company-overview.pco_dataset.fct__hist_ticker`"
    df = client.query(query).to_dataframe()
    return df['Ticker'].tolist()

# Function to get stock data from BigQuery
def get_stock_data_from_bq(ticker, start_date, end_date, granularity):
    table_name = 'pco_dataset.fct__hist_ticker'
    interval_table = 'pco_dataset.dim__interval'
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')

    if granularity == 'H':
        base_unit = 'hour'
    elif granularity == 'D':
        base_unit = 'day'
    elif granularity == 'M':
        base_unit = 'month'
    elif granularity == 'Y':
        base_unit = 'year'  # Optional: check if present
    else:
        base_unit = 'day'

    query = f"""
        SELECT
            fct.Ticker,
            PARSE_DATE('%Y%m%d', CAST(DateDimKey AS STRING)) AS date,
            fct.Open, fct.High, fct.Low, fct.Close, fct.Volume,
            fct.FileTimestamp, fct.DBTLoadedAtStaging,
            fct.DataSource,
            dim.BaseUnit,
            dim.IntervalValue,
            dim.IntervalDescription
        FROM `public-company-overview.pco_dataset.fct__hist_ticker` fct
        FULL JOIN `public-company-overview.pco_dataset.dim__interval` dim
            ON fct.IntervalDimKey = dim.IntervalDimKey
        WHERE fct.Ticker = '{ticker}'
            AND PARSE_DATE('%Y%m%d', CAST(DateDimKey AS STRING)) BETWEEN '{start_str}' AND '{end_str}'
            AND dim.BaseUnit = '{base_unit}'
        ORDER BY date

    """
    df = client.query(query).to_dataframe()
    return df

# Create Dash app
def create_dash_app(server: Flask) -> dash.Dash:
    dash_app = dash.Dash(__name__, server=server, url_base_pathname='/')

    # Load available tickers once at startup
    tickers = get_available_tickers()

    dash_app.layout = html.Div([
        html.H1("📊 Ticker Overview Dashboard", style={'textAlign': 'center'}),

        html.Div([
            dcc.Dropdown(
                id='ticker-dropdown',
                options=[{'label': t, 'value': t} for t in tickers],
                placeholder='Select a Ticker',
                style={'width': '30%'}
            ),
            dcc.DatePickerRange(
                id='date-picker',
                min_date_allowed=datetime(2010, 1, 1),
                max_date_allowed=datetime(2025, 12, 31),
                start_date=datetime(2010, 1, 1),
                end_date=datetime(2025, 12, 31)
            ),
            dcc.Dropdown(
                id='granularity-dropdown',
                options=[
                    {'label': 'Hourly', 'value': 'H'},
                    {'label': 'Daily', 'value': 'D'},
                    {'label': 'Monthly', 'value': 'M'},
                    {'label': 'Yearly', 'value': 'Y'}
                ],
                value='D',
                placeholder='Select Granularity',
                style={'width': '30%'}
            )
        ], style={'display': 'flex', 'gap': '10px', 'justifyContent': 'center', 'marginBottom': '20px'}),

        dcc.Graph(id='stock-chart'),

        html.H3("🔍 Data Table Preview", style={'textAlign': 'center'}),
        dash_table.DataTable(
            id='stock-table',
            page_size=10,
            style_table={'overflowX': 'auto'},
            style_cell={'textAlign': 'center', 'padding': '5px'},
            style_header={'backgroundColor': 'lightgrey', 'fontWeight': 'bold'}
        )
    ])

    @dash_app.callback(
        Output('date-picker', 'max_date_allowed'),
        Input('date-picker', 'start_date'),
        Input('granularity-dropdown', 'value')
    )
    def update_max_date_allowed(start_date, granularity):
        start_date = pd.to_datetime(start_date)
        if granularity == 'H':
            max_date = start_date + pd.Timedelta(days=14)
            if max_date > datetime(2023, 12, 31):
                max_date = datetime(2023, 12, 31)
            return max_date
        else:
            return datetime(2023, 12, 31)

    @dash_app.callback(
        Output('stock-chart', 'figure'),
        Output('stock-table', 'data'),
        Output('stock-table', 'columns'),
        Input('ticker-dropdown', 'value'),
        Input('date-picker', 'start_date'),
        Input('date-picker', 'end_date'),
        Input('granularity-dropdown', 'value')
    )
    def update_chart_and_table(ticker, start_date, end_date, granularity):
        if not ticker:
            return (
                go.Figure(layout=go.Layout(title="Select a ticker")),
                [], []
            )

        df = get_stock_data_from_bq(ticker, pd.to_datetime(start_date), pd.to_datetime(end_date), granularity)

        if df.empty:
            return (
                go.Figure(layout=go.Layout(title="No data for selected range")),
                [], []
            )

        # Normalize volume for color weight

        df['volume_norm'] = (df['Volume'] - df['Volume'].min()) / (df['Volume'].max() - df['Volume'].min() + 1e-9)

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df['date'],
            y=df['Close'],
            mode='lines+markers',
            marker=dict(color=df['volume_norm'], colorscale='Blues', size=6),

            name=ticker
        ))
        fig.update_layout(

            title=f"{ticker} - Historical Prices (volume weight in color)",

            xaxis_title="Date",

            yaxis_title="Close Price ($)"

        )

        # Table data

        table_data = df.to_dict('records')

        table_columns = [{"name": i, "id": i} for i in df.columns]


        return fig, table_data, table_columns

    return dash_app

# Entrypoint
if __name__ == '__main__':
    server = Flask(__name__)
    app = create_dash_app(server)
    server.run(debug=True, host='0.0.0.0', port=8050)
