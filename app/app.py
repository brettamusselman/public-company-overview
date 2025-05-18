from flask import Flask
import dash
from dash import html, dcc, Input, Output
import plotly.graph_objs as go
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from werkzeug.serving import run_simple

server = Flask(__name__)

dash_app = dash.Dash(
    __name__,
    server=server,
    url_base_pathname='/dash/'
)

dash_app.layout = html.Div([
    html.H1("Public Company Overview", style={'textAlign': 'center', 'backgroundColor': '#f0f0f0', 'padding': '10px'}),

    html.Div([
        dcc.Input(
            id='company-input',
            type='text',
            placeholder='Enter company name (e.g. Apple)',
            style={'width': '50%', 'padding': '10px'}
        )
    ], style={'textAlign': 'center', 'marginTop': '20px'}),

    dcc.Graph(id='company-chart')
])

@dash_app.callback(
    Output('company-chart', 'figure'),
    Input('company-input', 'value')
)
def update_chart(company_name):
    if not company_name:
        company_name = "Company Name"
    return {
        'data': [go.Bar(x=[company_name], y=[1])],
        'layout': go.Layout(
            title=f"Overview for {company_name}",
            yaxis={'visible': False},
            xaxis={'title': 'Company'},
            height=400
        )
    }

@server.route('/dash/')
def render_dash():
    return dash_app.index()

@server.route('/')
def index():
    return '''
    <div style="text-align: center; background-color: #f0f0f0; padding: 10px; margin-bottom: 20px;">
        <h1>Public Company Overview</h1>
    </div>
    <h2>Welcome! Visit <a href="/dash/">/dash/</a> to view the dashboard.</h2>
    '''

if __name__ == "__main__":
    run_simple('localhost', 8050, server, use_reloader=True, use_debugger=True)
