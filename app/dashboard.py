from flask import Flask
import dash
from dash import html, dcc, Input, Output, State, dash_table
import plotly.graph_objs as go
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from datetime import datetime
from google.cloud import bigquery
from bq.bq import BQ_Client

bq = BQ_Client()

def get_available_tickers():
    return bq.get_available_tickers()

def get_available_data_sources():
    return bq.get_available_sources()

def get_stock_data_from_bq(ticker, start_date, end_date, granularity):
    return bq.get_single_stock(ticker, start_date, end_date, granularity)

def create_dash_app(server: Flask) -> dash.Dash:
    external_stylesheets = ['https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css']
    dash_app = dash.Dash(__name__, server=server, url_base_pathname='/dash/', external_stylesheets=external_stylesheets)

    tickers = get_available_tickers()
    data_sources = get_available_data_sources()

    dash_app.layout = html.Div(className='container-fluid', children=[
        html.Div(className='d-flex', children=[
            html.Button("Open Filters", id="open-filters", className="btn btn-secondary my-2 me-2"),
            html.Button("Close Filters", id="close-filters", className="btn btn-secondary my-2", style={'display': 'none'})
        ]),
        html.Div(className='d-flex flex-row', style={'height': '100%', 'width': '100%'}, children=[
            html.Div(id='sidebar', className='filters-box position-sticky', style={
                'top': '0', 'height': '100vh', 'overflowY': 'auto',
                'minWidth': '250px', 'maxWidth': '250px',
                'display': 'none', 'flex': '0 0 250px'
            }, children=[
                html.H5("🔧 Filters", className='text-center mb-3'),
                dcc.Dropdown(id='ticker-dropdown',
                    options=[{'label': t, 'value': t} for t in tickers],
                    placeholder='Select a Ticker',
                    className='form-select mb-2'
                ),
                dcc.DatePickerRange(
                    id='date-picker',
                    min_date_allowed=datetime(2010, 1, 1),
                    max_date_allowed=datetime(2025, 12, 31),
                    start_date=datetime(2010, 1, 1),
                    end_date=datetime(2025, 12, 31),
                    display_format='MM/DD/YYYY',
                    className='form-control mb-2'
                ),
                dcc.Dropdown(id='granularity-dropdown',
                    options=[
                        {'label': 'Hourly', 'value': 'H'},
                        {'label': 'Daily', 'value': 'D'},
                        {'label': 'Monthly', 'value': 'M'},
                        {'label': 'Yearly', 'value': 'Y'}
                    ],
                    value='D', placeholder='Granularity',
                    className='form-select mb-2'
                ),
                dcc.Input(id='interval-value-input',
                    placeholder='Interval (e.g., 1d, 2h)',
                    type='text', className='form-control mb-2'
                ),
                dcc.Dropdown(id='data-source-dropdown',
                    options=[{'label': ds, 'value': ds} for ds in data_sources],
                    placeholder='Data Source',
                    className='form-select mb-2'
                ),
                dcc.Dropdown(id='y-axis-dropdown',
                    options=[
                        {'label': 'Open', 'value': 'Open'},
                        {'label': 'High', 'value': 'High'},
                        {'label': 'Low', 'value': 'Low'},
                        {'label': 'Close', 'value': 'Close'}
                    ],
                    value='Close', placeholder='Y-axis Value',
                    className='form-select mb-2'
                ),
                dcc.Input(id='moving-avg-window',
                    placeholder='Enter Moving Average Window (e.g., 30)', type='number',
                    className='form-control mb-2'
                ),
                html.Label("Volume Norm Range:", className='fw-bold mt-2'),
                dcc.RangeSlider(id='volume-norm-slider', min=0, max=1, step=0.01, value=[0,1],
                    marks={0: '0', 0.5: '0.5', 1: '1'},
                    tooltip={"placement": "bottom", "always_visible": True}, className='mb-2'
                ),
                html.Label("Forecast Horizon (days):", className='fw-bold mt-2'),
                dcc.Input(
                    id='forecast-horizon',
                    placeholder='Forecast Horizon (e.g., 30)',
                    type='number',
                    value=30,
                    className='form-control mb-2'
                )
            ]),
            html.Div(className='flex-fill p-2', style={'minWidth': 0}, children=[
                dcc.Graph(id='candlestick-chart'),
                dcc.Graph(id='moving-avg-chart'),
                dcc.Graph(id='returns-histogram'),
                dcc.Graph(id='forecast-graph'),
                html.H3("🔍 Data Table Preview", className='text-center mt-3'),
                dash_table.DataTable(id='stock-table', page_size=10,
                    style_table={'overflowX': 'auto'},
                    style_cell={'textAlign': 'center', 'padding': '5px'},
                    style_header={'backgroundColor': 'lightgrey', 'fontWeight': 'bold'}
                )
            ])
        ])
    ])

    dash_app.clientside_callback(
        """
        function(openClicks, closeClicks, sidebarStyle, openStyle, closeStyle) {
            if (openClicks && (!closeClicks || openClicks > closeClicks)) {
                return [{...sidebarStyle, display: 'block'}, {...openStyle, display: 'none'}, {...closeStyle, display: 'inline-block'}];
            } else {
                return [{...sidebarStyle, display: 'none'}, {...openStyle, display: 'inline-block'}, {...closeStyle, display: 'none'}];
            }
        }
        """,
        Output('sidebar', 'style'),
        Output('open-filters', 'style'),
        Output('close-filters', 'style'),
        Input('open-filters', 'n_clicks'),
        Input('close-filters', 'n_clicks'),
        State('sidebar', 'style'),
        State('open-filters', 'style'),
        State('close-filters', 'style'),
        prevent_initial_call=True
    )

    @dash_app.callback(
        Output('candlestick-chart', 'figure'),
        Output('moving-avg-chart', 'figure'),
        Output('returns-histogram', 'figure'),
        Output('forecast-graph', 'figure'),
        Output('stock-table', 'data'),
        Output('stock-table', 'columns'),
        Input('ticker-dropdown', 'value'),
        Input('date-picker', 'start_date'),
        Input('date-picker', 'end_date'),
        Input('granularity-dropdown', 'value'),
        Input('interval-value-input', 'value'),
        Input('data-source-dropdown', 'value'),
        Input('y-axis-dropdown', 'value'),
        Input('volume-norm-slider', 'value'),
        Input('moving-avg-window', 'value'),
        Input('forecast-horizon', 'value')
    )
    def update_chart_and_table(ticker, start_date, end_date, granularity, interval_value, data_source, y_axis_value, volume_norm_range, moving_avg_window, forecast_horizon):
        empty_fig = go.Figure(layout=go.Layout(title="No data"))
        if not ticker or not start_date or not end_date:
            return empty_fig, empty_fig, empty_fig, empty_fig, [], []

        start_date = pd.to_datetime(start_date, errors='coerce')
        end_date = pd.to_datetime(end_date, errors='coerce')
        if pd.isnull(start_date) or pd.isnull(end_date):
            return empty_fig, empty_fig, empty_fig, empty_fig, [], []

        df = get_stock_data_from_bq(ticker, start_date, end_date, granularity)
        if interval_value:
            df = df[df['IntervalValue'] == interval_value]
        if data_source:
            df = df[df['DataSource'] == data_source]
        if df.empty or y_axis_value not in df.columns:
            return empty_fig, empty_fig, empty_fig, empty_fig, [], []

        df['volume_norm'] = (df['Volume'] - df['Volume'].min()) / (df['Volume'].max() - df['Volume'].min() + 1e-9)
        df = df[(df['volume_norm'] >= volume_norm_range[0]) & (df['volume_norm'] <= volume_norm_range[1])]
        if df.empty:
            return empty_fig, empty_fig, empty_fig, empty_fig, [], []

        x_range = [df['date'].min(), df['date'].max()]
        fig_candle = go.Figure(data=[go.Candlestick(
            x=df['date'], open=df['Open'], high=df['High'], low=df['Low'], close=df['Close'],
            increasing_line_color='green', decreasing_line_color='red'
        )])
        fig_candle.update_layout(title="Candlestick Chart", xaxis_title="Date", yaxis_title="Price",
                                 xaxis=dict(range=x_range), height=500, xaxis_rangeslider_visible=False)

        window = moving_avg_window if moving_avg_window else 30
        df['moving_avg'] = df[y_axis_value].rolling(window=window).mean()
        fig_moving_avg = go.Figure()
        fig_moving_avg.add_trace(go.Scatter(x=df['date'], y=df[y_axis_value], mode='lines', name='Actual', line=dict(color='blue')))
        if len(df) >= window:
            fig_moving_avg.add_trace(go.Scatter(x=df['date'], y=df['moving_avg'], mode='lines', name=f'{window}-period MA', line=dict(color='orange', dash='dash')))
        fig_moving_avg.update_layout(title="Moving Average", xaxis_title="Date", yaxis_title=y_axis_value)

        df['returns'] = df[y_axis_value].pct_change() * 100
        fig_returns = go.Figure()
        fig_returns.add_trace(go.Histogram(x=df['returns'].dropna(), nbinsx=50, marker_color='skyblue'))
        fig_returns.update_layout(title="Returns Histogram", xaxis_title="Returns (%)", yaxis_title="Frequency")

        # Linear regression forecast
        X = np.arange(len(df)).reshape(-1, 1)
        y_vals = df[y_axis_value].values
        model = LinearRegression().fit(X, y_vals)
        best_fit_line = model.predict(X)

        forecast_horizon = forecast_horizon if forecast_horizon else 30
        last_date = df['date'].iloc[-1]
        future_dates = pd.date_range(start=last_date + pd.Timedelta(days=1), periods=forecast_horizon)
        future_X = np.arange(len(df), len(df) + forecast_horizon).reshape(-1, 1)
        future_forecast_lr = model.predict(future_X)

        # Holt’s Linear Trend forecast
        holt_model = ExponentialSmoothing(df[y_axis_value], trend='add', seasonal=None).fit()
        holt_fitted = holt_model.fittedvalues
        future_forecast_holt = holt_model.forecast(forecast_horizon)

        forecast_df = pd.DataFrame({'date': future_dates, 'forecast_lr': future_forecast_lr, 'forecast_holt': future_forecast_holt})
        xaxis_range = [df['date'].min(), forecast_df['date'].max()]

        fig_forecast = go.Figure()
        fig_forecast.add_trace(go.Scatter(x=df['date'], y=df[y_axis_value], mode='lines', name='Actual', line=dict(color='blue')))
        fig_forecast.add_trace(go.Scatter(x=df['date'], y=best_fit_line, mode='lines', name='Trend (Linear)', line=dict(color='green', dash='solid')))
        fig_forecast.add_trace(go.Scatter(x=df['date'], y=holt_fitted, mode='lines', name='Holt Fitted', line=dict(color='purple', dash='solid')))
        fig_forecast.add_trace(go.Scatter(x=future_dates, y=future_forecast_lr, mode='lines', name='Forecast (Linear)', line=dict(color='green', dash='dash')))
        fig_forecast.add_trace(go.Scatter(x=future_dates, y=future_forecast_holt, mode='lines', name='Forecast (Holt)', line=dict(color='purple', dash='dot')))
        fig_forecast.update_layout(
            title="Forecasts (Linear Regression & Holt)",
            xaxis_title="Date",
            yaxis_title=y_axis_value,
            xaxis=dict(range=xaxis_range)
        )

        table_data = df.to_dict('records')
        table_columns = [{"name": i, "id": i} for i in df.columns]

        return fig_candle, fig_moving_avg, fig_returns, fig_forecast, table_data, table_columns

    return dash_app

if __name__ == '__main__':
    server = Flask(__name__)
    app = create_dash_app(server)
    server.run(debug=True, host='0.0.0.0', port=8050)
