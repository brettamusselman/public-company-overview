from flask import Flask
import dash
from dash import html, dcc, Input, Output
import plotly.graph_objs as go

def create_dash_app(server: Flask) -> dash.Dash:
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

    return dash_app
