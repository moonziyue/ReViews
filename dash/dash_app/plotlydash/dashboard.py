"""Instantiate a Dash app."""
import numpy as np
import pandas as pd
import dash
import dash_table
import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import plotly.express as px

from .data import create_dataframe
from .data import get_customer_info
from .data import get_product_info
from .layout import html_layout

def init_dashboard(server):
    """Create a Plotly Dash dashboard."""
    dash_app = dash.Dash(
        server=server,
        routes_pathname_prefix='/dashapp/',
        external_stylesheets=[
            '/static/dist/css/styles.css',
            'https://fonts.googleapis.com/css?family=Lato',
            'https://codepen.io/chriddyp/pen/bWLwgP.css'
#             dbc.themes.BOOTSTRAP
        ]
    )

    # Load DataFrame
    df = create_dataframe(None)

    # Custom HTML layout
    dash_app.index_string = html_layout

    # Create Layout
    dash_app.layout = html.Div(children=[
        html.Div([
            dcc.Input(
                id="input",
                placeholder='Search "product+keyword"',
                type='text',
                value='',
                style={'width': '60%', 'margin':'0 20%'}
            ),
            html.Br(),
            html.Br()
        ], 
            className="row", 
            #style={'minWidth': '90%'}
        ),
        html.Div(
            id='datatable-container',
            children=create_data_table(df),
            className="row"
        ),
        html.Div(id='customer-graph',className="row"),
        html.Br(),
        html.Div(id='product-graph',className="row")
    ],style={'border-radius': 30, 'margin': '30px','justify-content': 'center'})
    init_callbacks(dash_app)
    
    return dash_app.server



def init_callbacks(dash_app):
    # 
    @dash_app.callback(
        Output("datatable-container", "children"), 
        [Input("input", "value")]
    )
    def return_search_results(search_input):
        df = create_dataframe(search_input)
        return create_data_table(df)
    
    @dash_app.callback(
        Output("customer-graph", "children"), 
        [Input("es_datatable", "derived_virtual_data"),
         Input("es_datatable", "derived_virtual_selected_rows")
        ]
    )
    def create_customer_graphs(data, selected_rows):
        if not selected_rows:
            return None
        df=pd.DataFrame(data)
        df=df[['customer_id','product_id']]
        row_id=selected_rows[0]
        customer_id=df.iloc[row_id,0]
        customer_info=get_customer_info(customer_id)
        
        graph_div=html.Div([
            dcc.Graph(figure={
                    'data': [go.Pie(labels=['☆', '☆☆', '☆☆☆', '☆☆☆☆', '☆☆☆☆☆'],
                                    values=list(customer_info[0])[1:6],
                                    pull=[0.2, 0, 0, 0, 0.2],
                                    hoverinfo='label+value+percent', textinfo='label+percent'
                                   )],
                    'layout':{'title':'Ratings'}
                },className="one-third column",style={'width': '33%', 'margin':'0'}),
            dcc.Graph(figure={
                    'data': [go.Pie(labels=['Verified', 'Not Verified'],
                                    values=[customer_info[0][7],customer_info[0][6]-customer_info[0][7]],
                                    pull=[0.2, 0],
                                    hoverinfo='label+value+percent', textinfo='label+percent'
                                   )],
                    'layout':{'title':'Verified Purchase'}
                },className="one-third column",style={'width': '33%', 'margin':'0'}),
            #className="one-third column"
            dcc.Graph(figure={
                    'data': [go.Bar(x=["Total Purchase","Helpful Votes"],
                                    y=[customer_info[0][6],customer_info[0][8]],
                                    text=[customer_info[0][6],customer_info[0][8]],
                                    textposition='auto',
                                    width=[0.5, 0.5],
                                    marker_color=['rgb(238,119,38)','rgb(38,129,178)']
                                   )],
                    'layout':{'title':'Helpful Votes'}
            },className="one-third column",style={'width': '33%', 'margin':'0'})
        ])
        
        graph_container=[
            html.H1('Customer 👤 '+customer_id),
            graph_div
        ]
        
        return graph_container
    
    @dash_app.callback(
        Output("product-graph", "children"), 
        [Input("es_datatable", "derived_virtual_data"),
         Input("es_datatable", "derived_virtual_selected_rows")
        ]
    )
    def create_product_graphs(data, selected_rows):
        if not selected_rows:
            return None
        df=pd.DataFrame(data)
        df=df[['customer_id','product_id']]
        row_id=selected_rows[0]
        product_id=df.iloc[row_id,1]
        product_info=get_product_info(product_id)
        
        fig=px.line(product_info, x='review_date', y=product_info.columns, title='Product Average Rating',
                    labels=dict(review_date='Date'))
        fig.update_xaxes(
            dtick="M1",
            tickformat="%b\n%Y",
            rangeslider_visible=True
        )
        
        graph_container=[
            html.H1('Product 🛍 '+product_id),
            dcc.Graph(
                figure=fig
            )
        ]
        
        return graph_container
    

def create_data_table(df):
    """Create Dash datatable from Pandas DataFrame."""
    #df=df[['product_title','star_rating','verified_purchase','review_body','review_date']]
    table=dash_table.DataTable(
        id='es_datatable',
        style_cell_conditional=[
            {'overflow': 'hidden'},
            {'textOverflow': 'ellipsis'},
            # hide ids
            {'if': {'column_id': 'customer_id'},'display': 'None'},
            {'if': {'column_id': 'product_id'},'display': 'None'},
            # format
            {'if': {'column_id': 'star_rating'},'width': '5%'},
            {'if': {'column_id': 'verified_purchase'},'width': '5%'},
            {'if': {'column_id': 'product_title'},'Width': '200px','minWidth': '200px','maxWidth': '200px'}
        ],
        style_cell={
            'whiteSpace': 'normal',
            'height': 'auto',
        },
        style_table={
            'minWidth': '95%',
            'overflowY': 'auto',
            'height': '800px'
        }, # 
        # show on hover
#         tooltip_data=[
#             {
#                 column: {'value': str(value), 'type': 'markdown'}
#                 for column, value in row.items()
#             } for row in df.to_dict('rows')
#         ],
#         tooltip_duration=None,
        # scroll
        #fixed_columns={'headers': True, 'data': 1},
        #fixed_rows={'headers': True},
        columns=[
            {"name": i, "id": i, "selectable": True} for i in ['product_title','star_rating','verified_purchase','review_body','review_date','customer_id','product_id']
        ],
        data=df.to_dict('records'),
        #filter_action="native",
        sort_action="native",
        sort_mode="multi",
        row_selectable="single",
        selected_rows=[],
        page_action="native",
        page_current= 0,
        page_size= 10,
    )
    return table

