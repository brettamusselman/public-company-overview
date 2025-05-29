from flask import Flask
import dash
from dash import html, dcc, Input, Output
import logging
from api.api import API  # assumes the API client class is in api.py

# Setup logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Instantiate the API client
api_client = API()

# Mock list of tickers; ideally, fetch this from BigQuery or your backend
AVAILABLE_TICKERS = ["AAPL", "GOOG", "MSFT", "TSLA", "AMZN"]

# Create Dash app
def create_request_app(server: Flask) -> dash.Dash:
    dash_app = dash.Dash(__name__, server=server, url_base_pathname='/request/')

    dash_app.layout = html.Div([
        html.H1("🚀 Trigger Standard Workflow", style={'textAlign': 'center'}),

        html.Div([
            html.Label("Select a Ticker:"),
            dcc.Dropdown(
                id='ticker-select',
                options=[{'label': t, 'value': t} for t in AVAILABLE_TICKERS],
                placeholder='Choose a ticker',
                style={'width': '250px'}
            ),
        ], style={'textAlign': 'center', 'marginTop': '30px'}),

        html.Div(id='request-status', style={'textAlign': 'center', 'marginTop': '20px', 'color': '#0074D9'})
    ])

    @dash_app.callback(
        Output('request-status', 'children'),
        Input('ticker-select', 'value'),
    )
    def trigger_workflow(ticker):
        if not ticker:
            return "Please select a ticker."

        try:
            logger.info(f"Triggering standard workflow for {ticker}")
            result = api_client.invoke_standard_workflow(ticker=ticker)
            return f"Successfully triggered standard workflow for {ticker}: {result}"
        except Exception as e:
            logger.error(f"Failed to trigger workflow for {ticker}: {e}")
            return f"Error: {str(e)}"

    return dash_app

# Entrypoint
if __name__ == '__main__':
    server = Flask(__name__)
    app = create_request_app(server)
    server.run(debug=True, host='0.0.0.0', port=8051)