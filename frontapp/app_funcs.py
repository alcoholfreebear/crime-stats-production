from datetime import datetime
import plotly.express as px
import pandas as pd
import numpy as np
import dash
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State, ClientsideFunction
from app_data import df, mapbox_access_token

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.COSMO],
)

server = app.server


def get_idx_by_value(ddict, val):
    return [k for k in ddict.keys() if ddict[k] == val][0]


def get_options(arr):
    if (
        type(arr[0]) == str
        and "All" not in arr
        and ("EN" not in arr or "SV" not in arr)
    ):
        arr = ["All"] + arr
    return [{"label": x, "value": x} for x in arr]


def get_map_agg(df: pd.DataFrame, city: str = None, city_0: pd.DataFrame = None):
    add_marker = True
    if city != "All":
        if df.empty:
            df = city_0
            add_marker = False
        dfc = df[df["city"] == city].copy().iloc[0]
        centering = {"lat": dfc.get("gps_lat"), "lon": dfc.get("gps_lon")}
    else:
        centering = {"lat": 60, "lon": 18}
    df_plot = (
        df.groupby(["osm_lat", "osm_lon", "city"], as_index=False)[["id"]]
        .count()
        .rename(columns={"id": "incident counts", "osm_lat": "lat", "osm_lon": "lon"})
    )
    df_plot["marker_size"] = (
        np.round(np.log2(df_plot["incident counts"] + 1), decimals=2)
        if add_marker
        else 0
    )
    return df_plot, centering


@app.callback(
    [
        Output("main_graph", "figure"),
        Output("main_graph", "clickData"),
        Output("text_total_rows", "children"),
    ],
    [
        Input("datetime_RangeSlider", "value"),
        Input("languages", "value"),
        Input("cities", "value"),
        Input("types_inc", "value"),
        Input("guns", "value"),
    ],
    # [ State("lock_selector", "value"), State("main_graph", "relayoutData")],
)
def plot_mapbox(dateidx, language, city, type_inc, gun):
    min_idx, max_idx = dateidx
    min_date, max_date = date_range[min_idx], date_range[max_idx]
    cities_loc = [city] if city != "All" else cities
    type_inc = [type_inc] if type_inc != "All" else types_inc
    gun = [gun] if gun != "All" else gun_filters

    dff = df[
        (df["datetime"] <= max_date)
        & (df["datetime"] >= min_date)
        & (df["city"].isin(cities_loc))
        & (df["language"] == language)
        & (df["incident_type"].isin(type_inc))
        & (df["gun_filter"].isin(gun))
    ].copy()
    city_0 = df[(df["city"].isin(cities_loc))].iloc[[0]]
    map_agg, centering = get_map_agg(df=dff, city=city, city_0=city_0)
    zoom = 4.5 if city == "All" else 10
    px.set_mapbox_access_token(token=mapbox_access_token)
    fig = px.scatter_mapbox(
        map_agg,
        lat="lat",
        lon="lon",
        center=centering,
        # color= 'type', #size="Confirmed", size_max=70,
        color_continuous_scale=px.colors.cyclical.IceFire,
        size="marker_size",  # size=np.ones(len(df_plot))
        size_max=20,
        zoom=zoom,
        hover_name="city",
        hover_data={"city": False, "incident counts": True, "marker_size": False},
        mapbox_style="light",
        opacity=0.65,
        title="Accumulative incidents reported to police",
    )
    fig.update_traces(marker_color="skyblue")
    fig.update_layout(height=500, width=500, margin={"r": 0, "t": 0, "l": 0, "b": 0})
    text_incidents = f"""
                    Total {len(dff)} incidents reported for current filter selections. 
                    """
    return fig, None, text_incidents


@app.callback(
    [Output("main_table", "data"), Output("main_table", "columns")],
    [
        Input("datetime_RangeSlider", "value"),
        Input("languages", "value"),
        Input("cities", "value"),
        Input("types_inc", "value"),
        Input("guns", "value"),
        Input("main_graph", "clickData"),
        # Input("filter_top_rows", "value")
    ],
    # [State("main_graph", "clickData"),
    #  State("hour-chart", "clickData"),
    #  State("type-chart", "clickData")     ],
)
def filter_tables_mapclick(dateidx, language, city, type_inc, gun, mapclick):
    def return_response_table(dff):
        data = dff[table_columns]
        data = data.to_dict("records")
        columns = [{"id": c, "name": c} for c in table_columns]
        return data, columns

    min_idx, max_idx = dateidx
    min_date, max_date = date_range[min_idx], date_range[max_idx]
    cities_loc = [city] if city != "All" else cities
    type_inc = [type_inc] if type_inc != "All" else types_inc
    gun = [gun] if gun != "All" else gun_filters
    dff = df[
        (df["datetime"] <= max_date)
        & (df["datetime"] >= min_date)
        & (df["city"].isin(cities_loc))
        & (df["language"] == language)
        & (df["incident_type"].isin(type_inc))
        & (df["gun_filter"].isin(gun))
    ].copy()
    if mapclick is not None:
        dfff = dff[
            (dff["osm_lon"] == mapclick["points"][0]["lon"])
            & (dff["osm_lat"] == mapclick["points"][0]["lat"])
        ]
    else:
        dfff = dff
    return return_response_table(dfff)


@app.callback(
    Output("type-chart", "figure"),
    [
        Input("datetime_RangeSlider", "value"),
        Input("languages", "value"),
        Input("cities", "value"),
        Input("types_inc", "value"),
        Input("guns", "value"),
    ],
)
def update_type_chart(dateidx, language, city, type_inc, gun):
    min_idx, max_idx = dateidx
    min_date, max_date = date_range[min_idx], date_range[max_idx]
    cities_loc = [city] if city != "All" else cities
    type_inc = [type_inc] if type_inc != "All" else types_inc
    gun = [gun] if gun != "All" else gun_filters

    dff = (
        df[
            (df["datetime"] <= max_date)
            & (df["datetime"] >= min_date)
            & (df["city"].isin(cities_loc))
            & (df["language"] == language)
            & (df["incident_type"].isin(type_inc))
            & (df["gun_filter"].isin(gun))
        ]
        .copy()
        .groupby("type")[["id"]]
        .nunique()
        .rename(columns={"id": "incident counts"})
        .reset_index()
        .sort_values(ascending=False, by="incident counts")
        .set_index("type")
    )

    fig = px.bar(dff.head(20), y="incident counts", opacity=0.8)
    fig.update_xaxes(tickangle=45, tickfont_size=10)
    fig.update_traces(marker_color="skyblue")
    fig.update_layout(
        height=300,
        width=500,
        title="",
        xaxis={"tickangle": 45, "tickfont_size": 10, "title": ""},
        yaxis={"title": "", "tickfont_size": 11},
        plot_bgcolor="white",
        paper_bgcolor="white",
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
    )
    return fig


@app.callback(
    Output("hour-chart", "figure"),
    [
        Input("datetime_RangeSlider", "value"),
        Input("languages", "value"),
        Input("cities", "value"),
        Input("types_inc", "value"),
        Input("guns", "value"),
    ],
)
def update_hour_chart(dateidx, language, city, type_inc, gun):
    min_idx, max_idx = dateidx
    min_date, max_date = date_range[min_idx], date_range[max_idx]
    cities_loc = [city] if city != "All" else cities
    type_inc = [type_inc] if type_inc != "All" else types_inc
    gun = [gun] if gun != "All" else gun_filters
    dff = df[
        (df["datetime"] <= max_date)
        & (df["datetime"] >= min_date)
        & (df["city"].isin(cities_loc))
        & (df["language"] == language)
        & (df["incident_type"].isin(type_inc))
        & (df["gun_filter"].isin(gun))
    ].copy()
    dff = (
        dff.groupby("hour")[["id"]].nunique().rename(columns={"id": "incident counts"})
    )

    fig = px.bar(dff, y="incident counts", opacity=0.8)
    fig.update_traces(marker_color="skyblue")
    fig.update_layout(
        height=280,
        width=500,
        title="",
        xaxis={"tickangle": 45, "tickfont_size": 10, "title": "hour of day"},
        yaxis={"title": "", "tickfont_size": 11},
        plot_bgcolor="white",
        paper_bgcolor="white",
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
    )
    return fig


last_updated = df["date_requested"].max().strftime("%Y-%m-%d %H:%M")
df = df[~df["city"].str.lower().str.strip().str.endswith(" län")]
table_columns = ["datetime", "type", "details"]
df["hour"] = pd.to_datetime(df["datetime"]).dt.hour
df["incident_type"] = np.where(
    (df["type"] == "Trafikrelaterad") | (df["type"] == "Traffic-related"),
    "Traffic",
    "Significant incidents",
)
df["incident_type"] = np.where(
    (~df["incident_type"].isin(["Traffic"]))
    & (
        (df["type"].str.strip().str.lower().str.startswith("summary"))
        | (df["type"].str.strip().str.lower().str.startswith("sammanfattning"))
    ),
    "Summary",
    df["incident_type"],
)
df["incident_type"] = np.where(
    (~df["incident_type"].isin(["Traffic", "Summary"]))
    & (
        (df["type"].str.strip().str.lower().str.startswith("other"))
        | (df["type"].str.strip().str.lower().str.startswith("övrigt"))
    ),
    "Other",
    df["incident_type"],
)
df["gun_filter"] = np.where(
    df["gun_filter"] == "gun-related", "Gun-related", "Not gun-related"
)

date_range_ms = pd.date_range(
    start=datetime(2020, 10, 1), end=datetime.today(), freq="MS"
)
date_range = pd.date_range(
    start=datetime(2020, 10, 1), end=datetime.today() + pd.Timedelta(days=1)
)
date_range = dict(enumerate(date_range))

date_marks = {
    get_idx_by_value(date_range, x): {"label": x.strftime("%Y-%m-%d")}
    for x in date_range_ms
}

# control types
cities = ["All"] + sorted(df["city"].unique())
types_inc = ["All"] + [
    "Significant incidents",
    "Traffic",
    "Summary",
    "Other",
]  # sorted(df['incident_type'].unique())
types = ["All"] + sorted(df["type"].unique())
languages = sorted(df["language"].unique())
gun_filters = ["All"] + sorted(df["gun_filter"].unique())

options_rows = get_options(
    list(np.arange(100, int(len(df) / 2), 100)) + [int(len(df) / 2)]
)
city_options = get_options(cities)
language_options = get_options(languages)
type_options = get_options(types)
type_inc_options = get_options(types_inc)
gun_options = get_options(gun_filters)
