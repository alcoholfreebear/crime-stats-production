from datetime import datetime
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State, ClientsideFunction
import dash_core_components as dcc
import dash_html_components as html
import dash_table
from app_funcs import *
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


row_charts = html.Div(
    [
        dbc.Row(
            [
                dbc.Col(
                    html.Div(
                        [
                            html.H6("Top 25 incident types and hourly counts"),
                        ]
                    ),
                    lg=12,
                    style={"margin-top": "20px"},
                ),
            ]
        ),
        dbc.Row(
            [
                dcc.Loading(
                    dbc.Col(
                        html.Div(
                            [
                                # chart 1 start
                                dcc.Graph(id="type-chart"),
                                # chart 1 end
                            ]
                        ),
                        lg=6,
                        style={"margin-top": "40px"},
                    )
                ),
                dcc.Loading(
                    dbc.Col(
                        html.Div(
                            [
                                # chart 2 start
                                dcc.Graph(id="hour-chart"),
                                # chart 2 end
                            ]
                        ),
                        lg=6,
                        style={"margin-bottom": "40px"},
                    )
                ),
            ]
        ),
    ]
)


row_map = html.Div(
    [
        dbc.Row(
            [
                dbc.Col(
                    html.Div(
                        [  # html.Div(id='main_graph_dummy', children=None, n_click=None),
                            html.H6(
                                id="text_total_rows",
                                # children=f"""
                                # Total {int(len(df)/2)} incidents reported in Sweden.
                                # """,
                                style={"margin-bottom": "10px"},
                            ),
                            dcc.Graph(id="main_graph"),
                        ]
                    ),
                    lg=6,
                ),
                dbc.Col(
                    html.Div(
                        [
                            ##
                            html.H6(
                                id="top_rows",
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
                                id="main_table",
                                style_data={
                                    "whiteSpace": "normal",
                                    "height": "auto",
                                },
                                style_table={
                                    "height": 500,
                                    "textAlign": "left",
                                    "overflowY": "scroll",
                                },
                                style_cell={
                                    "textAlign": "left",
                                    "whiteSpace": "normal",
                                },
                                style_header={
                                    "fontWeight": "bold",
                                    "border": "thin lightgrey solid",
                                    "backgroundColor": "primary",
                                    "color": "black",
                                },
                                style_data_conditional=[
                                    {
                                        "if": {"row_index": "odd"},
                                        "backgroundColor": "rgb(248, 248, 248)",
                                    }
                                ],
                                data=df[table_columns].to_dict("records"),
                                columns=[{"id": c, "name": c} for c in table_columns],
                                # fixed_columns={'headers': True, 'data': 1},
                                # fixed_rows={'headers': True, 'data': 0}
                            ),
                        ]
                    ),
                    lg=6,
                ),
            ]
        ),
    ]
)

row_top = html.Div(
    [
        dbc.Row(
            [
                dbc.Col(
                    html.Div(
                        [
                            html.H3(
                                "Sweden Incidents Map Daily",
                                style={"margin-bottom": "20px"},
                            ),
                            html.H6(
                                id="subtitle-timestamp",
                                children=f"""
                            Incidents reports from the swedish police API, last updated {last_updated}.
                            """,
                                style={"margin-bottom": "0px"},
                            ),
                            html.H6(
                                id="subtitle",
                                children=f"""
                            Note that not all the reported incidents correspond to actual crimes. 
                            """,
                                style={"margin-bottom": "20px"},
                            ),
                        ]
                    ),
                    lg=12,
                ),
                dbc.Col(
                    html.Div(
                        [
                            html.H6(
                                id="choose-language-text",
                                children=f"""
                                Select Language
                            """,
                                style={"margin-bottom": "20px"},
                            ),
                            dcc.Dropdown(
                                id="languages",
                                options=language_options,
                                multi=False,
                                value="SV",
                                className="dcc_control",
                                style={"margin-bottom": "20px"},
                            ),
                        ]
                    ),
                    lg=2,
                ),
                dbc.Col(
                    html.Div(
                        [
                            html.H6(
                                id="dummy_text",
                                children="Select Date Range",
                                className="control_label",
                            ),
                            dcc.RangeSlider(
                                id="datetime_RangeSlider",
                                updatemode="mouseup",  # don't let it update till mouse released
                                min=min(date_range.keys()),
                                max=max(date_range.keys()),
                                value=[min(date_range.keys()), max(date_range.keys())],
                                # TODO add markers for key dates
                                marks=date_marks,
                                # tooltip = { 'always_visible': True },
                            ),
                        ]
                    ),
                    lg=12,
                    style={"margin-bottom": "20px"},
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    html.Div(
                        [
                            html.H6(
                                id="choose_city_text",
                                children="Select City",
                                className="control_label",
                            ),
                            dcc.Dropdown(
                                id="cities",
                                options=city_options,
                                multi=False,
                                value="All",
                                className="dcc_control",
                                style={"margin-bottom": "5px"},
                            ),
                        ]
                    ),
                    lg=4,
                ),
                dbc.Col(
                    html.Div(
                        [
                            html.H6(
                                id="choose_type_text",
                                children="Incident Type",
                                className="control_label",
                            ),
                            dcc.Dropdown(
                                id="types_inc",
                                options=type_inc_options,
                                multi=False,
                                value="Significant incidents",
                                className="dcc_control",
                                style={"margin-bottom": "5px"},
                            ),
                        ]
                    ),
                    lg=4,
                ),
                dbc.Col(
                    html.Div(
                        [
                            html.H6(
                                id="choose_gun_text",
                                children="Gun Involvement",
                                className="control_label",
                            ),
                            dcc.Dropdown(
                                id="guns",
                                options=gun_options,
                                multi=False,
                                value="All",
                                className="dcc_control",
                                style={"margin-bottom": "5px"},
                            ),
                        ]
                    ),
                    lg=4,
                ),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    html.Div(
                        [
                            html.H6(
                                id="foot-note",
                                children=f"* Significant insidents exclude 'Traffic', 'Summary' and 'Other' incident types. ",
                                style={"margin-bottom": "20px"},
                            ),
                        ]
                    ),
                    lg=12,
                ),
            ]
        ),
    ]
)

app.layout = html.Div(
    dbc.Container(
        [
            row_top,
            dcc.Loading(row_map),
            dcc.Loading(row_charts),
        ],
        className="p-5",
    ),
)

# Main
if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=8080, debug=True)  # , use_reloader=False
    # app.run_server(host='127.0.0.1', port=8080, debug=True) #, use_reloader=False
