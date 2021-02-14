from google.cloud import storage
# from google.cloud import datastore
from datetime import datetime
import plotly.express as px
import pandas as pd
import numpy as np
import dash
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State, ClientsideFunction
import dash_core_components as dcc
import dash_html_components as html
import dash_table
app = dash.Dash( __name__, external_stylesheets=[dbc.themes.COSMO],)

server = app.server

def read_gcs()->pd.DataFrame:
    """
    read dataframe from parquet file in gcs
    :return:
    """
    bucket_name='crime-stat-app-us'
    file_path=f'gs://{bucket_name}/front/dashboard.parquet'
    return pd.read_parquet(file_path).sort_values(by='datetime', ascending=False)

def get_token():
    client = storage.Client()
    bucket_name = 'crime-stat-app-us'
    bucket = client.get_bucket(bucket_name)
    blob = bucket.get_blob('mapbox.txt')
    return blob.download_as_string().decode('utf-8').strip()

def get_idx_by_value(ddict, val):
    return [k for k in ddict.keys() if ddict[k]==val ][0]

def get_options(arr):
    if type(arr[0])==str and 'All' not in arr and ('EN' not in arr or 'SV' not in arr):
        arr=['All']+arr
    return [{"label": x, "value": x} for x in arr]

def get_map_agg(df, city:str=None):
    if city!='All':
        dfc=df[df['city']==city].copy().iloc[0]
        centering = {'lat': dfc.get('gps_lat'), 'lon': dfc.get('gps_lon')}
    else:
        centering={'lat': 60, 'lon': 18}
    df_plot = df.groupby(['osm_lat', 'osm_lon', 'city'], as_index=False)[['id']].count().rename(
        columns={'id': 'incident counts','osm_lat':'lat', 'osm_lon':'lon'})
    df_plot['marker_size'] = np.round(np.log2(df_plot['incident counts'] + 1), decimals=2)
    return df_plot, centering


@app.callback(
    [Output("main_graph", "figure"),
     Output("main_graph", "clickData"),
     Output("text_total_rows", "children")
     ],
    [
        Input('datetime_RangeSlider', "value"),
        Input("languages", "value"),
        Input("cities", "value"),
        Input("types_inc", "value"),
        Input("guns", "value"),
    ],
    #[State("lock_selector", "value"), State("main_graph", "relayoutData")],
)
def plot_mapbox(dateidx, language, city, type_inc, gun):
    min_idx, max_idx = dateidx
    min_date, max_date = date_range[min_idx], date_range[max_idx]
    cities_loc = [city] if city != 'All' else cities
    type_inc = [type_inc] if type_inc != 'All' else types_inc
    gun = [gun] if gun != 'All' else gun_filters

    dff = df[(df['datetime'] <= max_date)
             & (df['datetime'] >= min_date)
             & (df['city'].isin(cities_loc))
             & (df['language'] == language)
             & (df['incident_type'].isin(type_inc))
             & (df['gun_filter'].isin(gun))
             ].copy()
    map_agg, centering =get_map_agg(df=dff, city=city)
    zoom=4.5 if city=='All' else 10
    px.set_mapbox_access_token(token=mapbox_access_token)
    fig = px.scatter_mapbox(map_agg, lat='lat', lon='lon', center=centering,
                            # color= 'type', #size="Confirmed", size_max=70,
                            color_continuous_scale=px.colors.cyclical.IceFire,
                            size='marker_size'  # size=np.ones(len(df_plot))
                            , size_max=20, zoom=zoom, hover_name='city',
                            hover_data={'city':False, 'incident counts':True, 'marker_size':False},
                            mapbox_style='light', opacity=0.65,
                            title='Accumulative incidents reported to police')
    fig.update_traces(marker_color='skyblue')
    fig.update_layout(height=500, width=500, margin={"r": 0, "t": 0, "l": 0, "b": 0})
    text_incidents = f"""
                    Total {len(dff)} incidents reported for current filter selections. 
                    """
    return fig, None, text_incidents



@app.callback(
    [Output("main_table", "data"),
     Output("main_table", "columns")
     ],
    [
        Input('datetime_RangeSlider', "value"),
        Input("languages", "value"),
        Input("cities", "value"),
        Input("types_inc", "value"),
        Input("guns", "value"),
        Input("main_graph", "clickData"),
        #Input("filter_top_rows", "value")

    ],
    # [State("main_graph", "clickData"),
    #  State("hour-chart", "clickData"),
    #  State("type-chart", "clickData")     ],
)
def filter_tables_mapclick(dateidx, language, city, type_inc, gun, mapclick):
    def return_response_table(dff):
        data = dff[table_columns]
        data = data.to_dict('records')
        columns = [{'id': c, 'name': c} for c in table_columns]
        return data, columns

    min_idx, max_idx=dateidx
    min_date, max_date = date_range[min_idx], date_range[max_idx]
    cities_loc = [city] if city != 'All' else cities
    type_inc = [type_inc] if type_inc != 'All' else types_inc
    gun = [gun] if gun != 'All' else gun_filters
    dff=df[(df['datetime']<=max_date)
            &(df['datetime']>=min_date)
            &(df['city'].isin(cities_loc))
            &(df['language']==language)
            &(df['incident_type'].isin(type_inc))
            &(df['gun_filter'].isin(gun))
           ].copy()
    if mapclick is not None:
        dfff=dff[ (dff['osm_lon'] == mapclick['points'][0]['lon'])&(dff['osm_lat'] == mapclick['points'][0]['lat'])]
    else:
        dfff=dff
    return return_response_table(dfff)

@app.callback(
    Output("type-chart", "figure"),
    [ Input('datetime_RangeSlider', "value"),
        Input("languages", "value"),
        Input("cities", "value"),
        Input("types_inc", "value"),
        Input("guns", "value"),
      ])
def update_type_chart(dateidx, language, city, type_inc, gun):
    min_idx, max_idx=dateidx
    min_date, max_date = date_range[min_idx], date_range[max_idx]
    cities_loc = [city] if city != 'All' else cities
    type_inc = [type_inc] if type_inc != 'All' else types_inc
    gun = [gun] if gun != 'All' else gun_filters

    dff = df[
            (df['datetime'] <= max_date)
            & (df['datetime'] >= min_date)
            & (df['city'].isin(cities_loc))
            & (df['language'] == language)
            & (df['incident_type'].isin(type_inc))
            & (df['gun_filter'].isin(gun))
             ].copy().groupby('type')[['id']].nunique().rename(columns={'id':'incident counts'}
                        ).reset_index().sort_values(ascending=False,by='incident counts').set_index('type')

    fig = px.bar(dff.head(20), y='incident counts', opacity=0.8)
    fig.update_xaxes(tickangle= 45, tickfont_size=10)
    fig.update_traces(marker_color='skyblue')
    fig.update_layout(height=300, width=500, title='',
                      xaxis={'tickangle':45, 'tickfont_size':10, 'title':''},
                      yaxis={ 'title': '', 'tickfont_size':11},
                      plot_bgcolor='white',
                      paper_bgcolor='white',
                      margin={"r": 0, "t": 0, "l": 0, "b": 0})
    return fig


@app.callback(
    Output("hour-chart", "figure"),
    [   Input('datetime_RangeSlider', "value"),
        Input("languages", "value"),
        Input("cities", "value"),
        Input("types_inc", "value"),
        Input("guns", "value"),
      ])
def update_hour_chart(dateidx, language, city, type_inc, gun):
    min_idx, max_idx=dateidx
    min_date, max_date = date_range[min_idx], date_range[max_idx]
    cities_loc = [city] if city != 'All' else cities
    type_inc = [type_inc] if type_inc != 'All' else types_inc
    gun = [gun] if gun != 'All' else gun_filters
    dff = df[
            (df['datetime'] <= max_date)
            & (df['datetime'] >= min_date)
            & (df['city'].isin(cities_loc))
            & (df['language'] == language)
            & (df['incident_type'].isin(type_inc))
            & (df['gun_filter'].isin(gun))
             ].copy()
    dff=dff.groupby('hour')[['id']].nunique().rename(columns={'id':'incident counts'})

    fig = px.bar(dff, y='incident counts',opacity=0.8)
    fig.update_traces(marker_color='skyblue')
    fig.update_layout(height=280, width=500, title='',
                      xaxis={'tickangle':45, 'tickfont_size':10, 'title':'hour of day'},
                      yaxis={ 'title': '', 'tickfont_size':11},
                      plot_bgcolor='white',
                      paper_bgcolor='white',
                      margin={"r": 0, "t": 0, "l": 0, "b": 0})
    return fig

# client = datastore.Client()
# key = client.key('key-value', 'mapbox')
# mapbox_access_token = client.get(key)['value']
mapbox_access_token=get_token()
df=read_gcs()
last_updated=df['date_requested'].max().strftime('%Y-%m-%d %H:%M')
df=df[~df['city'].str.lower().str.strip().str.endswith(' län')]
table_columns=['datetime', 'type', 'details']
df['hour'] = pd.to_datetime(df['datetime']).dt.hour
df['incident_type']=np.where((df['type']=='Trafikrelaterad')|(df['type']=='Traffic-related'), 'Traffic', 'Significant incidents')
df['incident_type']=np.where((~df['incident_type'].isin(['Traffic']))&(
        (df['type'].str.strip().str.lower().str.startswith('summary'))
        |(df['type'].str.strip().str.lower().str.startswith('sammanfattning'))
        ), 'Summary', df['incident_type'])
df['incident_type']=np.where((~df['incident_type'].isin(['Traffic', 'Summary']))&(
        (df['type'].str.strip().str.lower().str.startswith('other'))
        |(df['type'].str.strip().str.lower().str.startswith('övrigt'))
        ), 'Other', df['incident_type'])
df['gun_filter']=np.where(df['gun_filter']=='gun-related', 'Gun-related', 'Not gun-related')

date_range_ms = pd.date_range(start=datetime(2020,10,1), end=datetime.today(),freq='MS')
date_range = pd.date_range(start=datetime(2020,10,1), end=datetime.today()+pd.Timedelta(days=1))
date_range = dict(enumerate(date_range))

date_marks={get_idx_by_value(date_range, x): {'label':x.strftime('%Y-%m-%d')} for x in date_range_ms}

# control types
cities = ['All']+sorted(df['city'].unique())
types_inc = ['All']+['Significant incidents', 'Traffic','Summary', 'Other']#sorted(df['incident_type'].unique())
types = ['All']+sorted(df['type'].unique())
languages = sorted(df['language'].unique())
gun_filters = ['All']+sorted(df['gun_filter'].unique())

options_rows = get_options(list(np.arange(100, int(len(df)/2), 100))+[int(len(df)/2)])
city_options = get_options(cities)
language_options=get_options(languages)
type_options=get_options(types)
type_inc_options=get_options(types_inc)
gun_options=get_options(gun_filters)

#
# row_charts=html.Div([dbc.Row([
#
#     dbc.Col(html.Div([
#         # chart 2 start
#         dcc.Graph(id="hour-chart"),
#         # chart 2 end
#     ]), lg=6, style={"margin-bottom": "40px"}),
#
#     dbc.Col(html.Div([
#         #chart 1 start
#         dcc.Graph(id="type-chart"),
#         #chart 1 end
#     ]), lg=6, style={"margin-top": "40px"}),
#     ])
# ])


row_charts=html.Div([

    dbc.Row([

       dbc.Col(html.Div([
            html.H6('Top 25 incident types and hourly counts'),
        ]),   lg=12, style={"margin-top": "20px"}),
    ]),
    dbc.Row([

    dcc.Loading(dbc.Col(html.Div([

        #chart 1 start
        dcc.Graph(id="type-chart"),
        #chart 1 end
    ]),   lg=6, style={"margin-top": "40px"})),

    dcc.Loading(dbc.Col(html.Div([
        # chart 2 start
        dcc.Graph(id="hour-chart"),
        # chart 2 end
    ]),   lg=6, style={"margin-bottom": "40px"})),
    ]),
])


row_map=html.Div([
        dbc.Row(
            [
                dbc.Col(html.Div(
                    [   #html.Div(id='main_graph_dummy', children=None, n_click=None),
                        html.H6(
                            id='text_total_rows',
                            #children=f"""
                            #Total {int(len(df)/2)} incidents reported in Sweden.
                            #""",
                            style={"margin-bottom": "10px"},
                        ),
                        dcc.Graph(id="main_graph")
                    ]
                ), lg=6),
                dbc.Col(html.Div(
                    [
                        ##
                        html.H6(
                            id='top_rows',
                            children=f"""
                            
                            """,
                            style={"margin-bottom": "28px"},
                        ),
                        #
                        # dcc.Dropdown(
                        #     id="filter_top_rows",
                        #     options=options_rows,
                        #     multi=False,
                        #     value=100,
                        #     className="dcc_control",
                        #     style={"margin-bottom": "10px"}),




                        ###
                        dash_table.DataTable(
                        id='main_table',
                        style_data={
                            'whiteSpace': 'normal',
                            'height': 'auto',
                        },
                        style_table={
                            'height': 500,
                            'textAlign': 'left',
                            'overflowY': 'scroll'
                        },
                        style_cell={
                        'textAlign': 'left',
                        'whiteSpace': 'normal',

                        },
                        style_header=
                        {
                        'fontWeight': 'bold',
                        'border': 'thin lightgrey solid',
                        'backgroundColor': 'primary',
                        'color': 'black'
                        },
                        style_data_conditional=[
                        {
                        'if': {'row_index': 'odd'},
                        'backgroundColor': 'rgb(248, 248, 248)'}],
                        data=df[table_columns].to_dict('records'),
                        columns=[{'id': c, 'name': c} for c in table_columns],
                        # fixed_columns={'headers': True, 'data': 1},
                        # fixed_rows={'headers': True, 'data': 0}
                        ),
                    ]), lg=6),
            ]
        ),])

row_top = html.Div(
    [
        dbc.Row(
            [
                dbc.Col(html.Div(
                    [
                        html.H3(
                            "Sweden Incidents Map Daily",
                            style={"margin-bottom": "20px"},
                        ),
                        html.H6(
                            id='subtitle-timestamp',
                            children=f"""
                            Crime and incidents reports from the swedish police API, last updated {last_updated}.
                            """,
                            style={"margin-bottom": "0px"},
                        ),
                        html.H6(
                            id='subtitle',
                            children=f"""
                            Note that not all the reported incidents correspond to actual crimes. 
                            """,
                            style={"margin-bottom": "20px"},
                        ),

                    ]
                ), lg=12),
                dbc.Col(html.Div(
                    [
                        dcc.Dropdown(
                            id="languages",
                            options=language_options,
                            multi=False,
                            value="EN",
                            className="dcc_control",
                            style={"margin-bottom": "20px"}),
                    ]
                ) ,lg=2),
                dbc.Col(html.Div(
                    [html.H6(
                            id='dummy_text',
                            children="Select Date Range",
                            className="control_label",
                        ),

                        dcc.RangeSlider(
                            id='datetime_RangeSlider',
                            updatemode='mouseup',  # don't let it update till mouse released
                            min=min(date_range.keys()),
                            max=max(date_range.keys()),
                            value=[min(date_range.keys()),
                                   max(date_range.keys())],
                            # TODO add markers for key dates
                            marks=date_marks,
                            # tooltip = { 'always_visible': True },

                        ),

                    ]
                ), lg=12, style={"margin-bottom": "20px"})

            ]
        ),
        dbc.Row([
            dbc.Col(html.Div(
                [
                    html.H6(
                        id='choose_city_text',
                        children="Select City",
                        className="control_label",
                    ),
                    dcc.Dropdown(
                        id="cities",
                        options=city_options,
                        multi=False,
                        value="All",
                        className="dcc_control",
                        style={"margin-bottom": "5px"}
                    ), ]), lg=4),
            dbc.Col(html.Div([
                html.H6(
                    id='choose_type_text',
                    children="Incident Type",
                    className="control_label",
                ),
                dcc.Dropdown(
                    id="types_inc",
                    options=type_inc_options,
                    multi=False,
                    value="Significant incidents",
                    className="dcc_control",
                    style={"margin-bottom": "5px"}
                ), ]), lg=4),
            dbc.Col(html.Div([
                html.H6(
                    id='choose_gun_text',
                    children="Gun Involvement",
                    className="control_label",
                ),
                dcc.Dropdown(
                    id="guns",
                    options=gun_options,
                    multi=False,
                    value="All",
                    className="dcc_control",
                    style={"margin-bottom": "5px"}
                ), ]), lg=4)
        ]),
        dbc.Row([
            dbc.Col(html.Div(
                [
                    html.H6(
                        id='foot-note',
                        children=f"* Significant insidents exclude 'Traffic', 'Summary' and 'Other' incident types. ",
                        style={"margin-bottom": "20px"},
                    ),

                ]
            ), lg=12),
        ])

    ]
)

app.layout = html.Div(dbc.Container(
    [row_top,
     dcc.Loading(row_map),
     dcc.Loading(row_charts),
     ],
    className="p-5",
),
)


# Main
if __name__ == "__main__":
    app.run_server(host='0.0.0.0', port=8080, debug=True) #, use_reloader=False