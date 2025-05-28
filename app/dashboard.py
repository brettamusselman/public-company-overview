from flask import Flask
import dash
from dash import html, dcc, Input, Output
from dash import dash_table
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


# Get available data sources
def get_available_data_sources():
    query = "SELECT DISTINCT DataSource FROM `public-company-overview.pco_dataset.fct__hist_ticker`"
    df = client.query(query).to_dataframe()
    return df['DataSource'].dropna().tolist()


# Function to get stock data from BigQuery
def get_stock_data_from_bq(ticker, start_date, end_date, granularity):
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')


    base_unit = {
        'H': 'hour',
        'D': 'day',
        'M': 'month',
        'Y': 'year'
    }.get(granularity, 'day')


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


    # Load available tickers and data sources
    tickers = get_available_tickers()
    data_sources = get_available_data_sources()


    dash_app.layout = html.Div([
        html.H1("📊 Ticker Overview Dashboard", style={'textAlign': 'center', 'marginBottom': '20px'}),


        html.Div([
            dcc.Dropdown(
                id='ticker-dropdown',
                options=[{'label': t, 'value': t} for t in tickers],
                placeholder='Select a Ticker',
                style={'minWidth': '180px', 'maxWidth': '200px'}
            ),
            dcc.DatePickerRange(
                id='date-picker',
                min_date_allowed=datetime(2010, 1, 1),
                max_date_allowed=datetime(2025, 12, 31),
                start_date=datetime(2010, 1, 1),
                end_date=datetime(2025, 12, 31),
                display_format='MM/DD/YYYY',
                style={'padding': '6px', 'borderRadius': '4px', 'border': '1px solid #ccc'}
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
                placeholder='Granularity',
                style={'minWidth': '120px', 'maxWidth': '150px'}
            ),
            dcc.Input(
                id='interval-value-input',
                placeholder='Interval (e.g., 1d, 2h)',
                type='text',
                style={'minWidth': '100px', 'maxWidth': '120px'}
            ),
            dcc.Dropdown(
                id='data-source-dropdown',
                options=[{'label': ds, 'value': ds} for ds in data_sources],
                placeholder='Data Source',
                style={'minWidth': '120px', 'maxWidth': '150px'}
            ),
            dcc.Dropdown(
                id='y-axis-dropdown',
                options=[
                    {'label': 'Open', 'value': 'Open'},
                    {'label': 'High', 'value': 'High'},
                    {'label': 'Low', 'value': 'Low'},
                    {'label': 'Close', 'value': 'Close'}
                ],
                value='Close',
                placeholder='Y-axis Value',
                style={'minWidth': '100px', 'maxWidth': '120px'}
            )
        ], style={
            'display': 'flex',
            'gap': '12px',
            'justifyContent': 'center',
            'alignItems': 'center',
            'flexWrap': 'wrap',
            'marginBottom': '20px'
        }),


        # NEW: Volume norm range slider
        html.Div([
            html.Label("Volume Norm Range:", style={'marginRight': '10px'}),
            html.Div(
                dcc.RangeSlider(
                    id='volume-norm-slider',
                    min=0,
                    max=1,
                    step=0.01,
                    value=[0, 1],
                    marks={0: '0', 0.5: '0.5', 1: '1'},
                    tooltip={"placement": "bottom", "always_visible": True},
                    allowCross=False
                ),
                style={'width': '60%', 'margin': 'auto'}
            )
        ], style={'textAlign': 'center', 'marginBottom': '20px'}),


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
        Output('stock-chart', 'figure'),
        Output('stock-table', 'data'),
        Output('stock-table', 'columns'),
        Input('ticker-dropdown', 'value'),
        Input('date-picker', 'start_date'),
        Input('date-picker', 'end_date'),
        Input('granularity-dropdown', 'value'),
        Input('interval-value-input', 'value'),
        Input('data-source-dropdown', 'value'),
        Input('y-axis-dropdown', 'value'),
        Input('volume-norm-slider', 'value')
    )
    def update_chart_and_table(ticker, start_date, end_date, granularity, interval_value, data_source, y_axis_value, volume_norm_range):
        if not ticker:
            return go.Figure(layout=go.Layout(title="Select a ticker")), [], []


        df = get_stock_data_from_bq(ticker, pd.to_datetime(start_date), pd.to_datetime(end_date), granularity)


        if interval_value:
            if interval_value not in df['IntervalValue'].dropna().unique():
                return go.Figure(layout=go.Layout(title="No data for selected interval value")), [], []
            else:
                df = df[df['IntervalValue'] == interval_value]


        if data_source:
            df = df[df['DataSource'] == data_source]


        if df.empty or y_axis_value not in df.columns:
            return go.Figure(layout=go.Layout(title="No data for selected filters")), [], []


        df['volume_norm'] = (df['Volume'] - df['Volume'].min()) / (df['Volume'].max() - df['Volume'].min() + 1e-9)


        # Filter by volume_norm range
        df = df[(df['volume_norm'] >= volume_norm_range[0]) & (df['volume_norm'] <= volume_norm_range[1])]


        if df.empty:
            return go.Figure(layout=go.Layout(title="No data for selected volume norm range")), [], []


        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df['date'],
            y=df[y_axis_value],
            mode='lines+markers',
            marker=dict(color=df['volume_norm'], colorscale='Blues', size=6),
            name=ticker
        ))
        fig.update_layout(
            title=f"{ticker} - {y_axis_value} Prices (volume weight in color)",
            xaxis_title="Date",
            yaxis_title=f"{y_axis_value} Price ($)"
        )


        table_data = df.to_dict('records')
        table_columns = [{"name": i, "id": i} for i in df.columns]


        return fig, table_data, table_columns


    return dash_app


# Entrypoint
if __name__ == '__main__':
    server = Flask(__name__)
    app = create_dash_app(server)
    server.run(debug=True, host='0.0.0.0', port=8050)



