from flask import Flask
import dash
from dash import html, dcc, Input, Output, State, dash_table
import pandas as pd
import io
from bq.bq import BQ_Client

# Initialize BQ client and global cache
dq = BQ_Client()
filtered_df_cache = {}

def get_available_tickers():
    return dq.get_available_tickers()

def get_available_data_sources():
    return dq.get_available_sources()

def get_stock_data_from_bq(ticker, start_date, end_date, granularity):
    return dq.get_single_stock(ticker, start_date, end_date, granularity)

def create_download_app(server: Flask) -> dash.Dash:
    external_stylesheets = ['https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css']
    dash_app = dash.Dash(__name__, server=server, url_base_pathname='/download_dash/', external_stylesheets=external_stylesheets)

    tickers = get_available_tickers()
    data_sources = get_available_data_sources()

    dash_app.layout = html.Div(className='container-fluid', children=[

        # Open/Close Filters buttons
        html.Div(className='d-flex', children=[
            html.Button("Open Filters", id="open-filters", className="btn btn-secondary my-2 me-2"),
            html.Button("Close Filters", id="close-filters", className="btn btn-secondary my-2", style={'display': 'none'})
        ]),

        # Main layout with sidebar and content
        html.Div(className='d-flex flex-row', style={'height': '100%', 'width': '100%'}, children=[

            # Sidebar with filters
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
                    min_date_allowed=pd.to_datetime('2010-01-01'),
                    max_date_allowed=pd.to_datetime('2025-12-31'),
                    start_date=pd.to_datetime('2010-01-01'),
                    end_date=pd.to_datetime('2025-12-31'),
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

                html.Label("Volume Norm Range:", className='fw-bold mt-2'),
                dcc.RangeSlider(id='volume-norm-slider', min=0, max=1, step=0.01, value=[0,1],
                    marks={0: '0', 0.5: '0.5', 1: '1'},
                    tooltip={"placement": "bottom", "always_visible": True}, className='mb-2'
                ),
            ]),

            # Main content area
            html.Div(className='flex-fill p-2', style={'minWidth': 0}, children=[

                # Top area with download button
                html.Div(className='d-flex flex-wrap align-items-center gap-2 my-3', children=[
                    dcc.Dropdown(
                        id="file-type-dropdown",
                        options=[
                            {"label": "CSV", "value": "csv"},
                            {"label": "Excel", "value": "excel"},
                        ],
                        value="csv",
                        style={'width': '200px'}
                    ),
                    html.Button("Download Filtered File", id="download-btn", className="btn btn-success"),
                    dcc.Download(id="download-component")
                ]),




                html.Hr(),

                html.H4("Table Preview"),
                dash_table.DataTable(id='stock-table', page_size=10,
                    style_table={'overflowX': 'auto'},
                    style_cell={'textAlign': 'center', 'padding': '5px'},
                    style_header={'backgroundColor': 'lightgrey', 'fontWeight': 'bold'}
                )
            ])
        ])
    ])

    # Sidebar open/close clientside callback
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

    # Update Table callback
    @dash_app.callback(
        Output('stock-table', 'data'),
        Output('stock-table', 'columns'),
        Input('ticker-dropdown', 'value'),
        Input('date-picker', 'start_date'),
        Input('date-picker', 'end_date'),
        Input('granularity-dropdown', 'value'),
        Input('interval-value-input', 'value'),
        Input('data-source-dropdown', 'value'),
        Input('volume-norm-slider', 'value')
    )
    def update_data(ticker, start_date, end_date, granularity, interval_value, data_source, volume_norm_range):
        # Always provide fallback defaults:
        ticker = ticker or tickers[0]  # If user did not select → first available ticker
        start_date = pd.to_datetime(start_date or '2010-01-01')
        end_date = pd.to_datetime(end_date or '2025-12-31')
        granularity = granularity or 'D'

        # Now always get stock data first
        df = get_stock_data_from_bq(ticker, start_date, end_date, granularity)

        # Now apply all filters, always:
        if interval_value:
            df = df[df['IntervalValue'] == interval_value]
        if data_source:
            df = df[df['DataSource'] == data_source]

        df['volume_norm'] = (df['Volume'] - df['Volume'].min()) / (df['Volume'].max() - df['Volume'].min() + 1e-9)
        df = df[(df['volume_norm'] >= volume_norm_range[0]) & (df['volume_norm'] <= volume_norm_range[1])]

        # Cache filtered dataframe for download
        global filtered_df_cache
        filtered_df_cache['df'] = df.copy()

        return df.to_dict('records'), [{'name': i, 'id': i} for i in df.columns]


    # Trigger download callback
    @dash_app.callback(
        Output("download-component", "data"),
        Input("download-btn", "n_clicks"),
        State("file-type-dropdown", "value"),
        prevent_initial_call=True
    )
    def trigger_download(n_clicks, file_type):
        df = filtered_df_cache.get("df")
        if df is None or df.empty:
            return dcc.send_string("No data to download.", "empty.txt")

        if 'polars' in str(type(df)):
            df = df.to_pandas()
        if file_type == "excel":

            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = df[col].dt.tz_localize(None)


            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                df.to_excel(writer, sheet_name='Data', index=False)
            output.seek(0)
            return dcc.send_bytes(output.read(), "filtered_stock_data.xlsx", mime_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        else:
            return dcc.send_data_frame(df.to_csv, "filtered_stock_data.csv", index=False)
        
        print("DF type:", type(df))
        print("DF shape:", df.shape if hasattr(df, 'shape') else 'N/A')


    return dash_app


# Standard entrypoint
server = Flask(__name__)

if __name__ == '__main__':
    app = create_download_app(server)
    server.run(debug=True, host='0.0.0.0', port=8050)
