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

    # Common style dict for consistent font family
    common_style = {
        'fontFamily': "'Segoe UI', Tahoma, Geneva, Verdana, sans-serif",
        'color': '#212529'
    }

    dash_app.layout = html.Div([

        html.Div([
            html.Label(
                "Select a Ticker:",
                className="fw-bold",
                style={**common_style, 'fontSize': '1.1rem', 'fontWeight': '600'}
            ),
            dcc.Dropdown(
                id='ticker-select',
                options=[{'label': t, 'value': t} for t in AVAILABLE_TICKERS],
                placeholder='Choose a ticker',
                style={
                    **common_style,
                    'fontSize': '1.1rem',
                    'minHeight': '45px',
                    'padding': '10px',
                    'fontWeight': '500'
                }
            ),
        ], className='text-center my-3'),

        html.Div(
            id='request-status',
            className='text-center mt-3 text-primary error-message',
            style={**common_style, 'fontSize': '1.1rem', 'color': '#0074D9', 'fontWeight': '500'}
        )
    ], className='container-fluid', style={'fontFamily': "'Segoe UI', Tahoma, Geneva, Verdana, sans-serif"})

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
            return f"✅ Successfully triggered standard workflow for {ticker}: {result}"
        except Exception as e:
            logger.error(f"Failed to trigger workflow for {ticker}: {e}")
            return f"❌ Error: {str(e)}"

    return dash_app

# Entrypoint
if __name__ == '__main__':
    server = Flask(__name__)
    app = create_request_app(server)
    server.run(debug=True, host='0.0.0.0', port=8051)
