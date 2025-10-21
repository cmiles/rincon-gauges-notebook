import marimo

__generated_with = "0.17.0"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Great Rincon Valley Area USGS Stream Guages

    There are three USGS Stream guages relevant for the Rincon Valley. This notebook shows two different stats:
     - Days of Flow: For a frequently dry stream and often low flow streams like Rincon Creek the Days of Flow is the closest metric to answering the main question 'is there water'. This is shown as small multiples - it is interesting to see similiar years or groups of years.
     - Mean Flow: For stream with steady flow the mean flow is more interesting that 'Days of Flow'. Two of the same chart are shown to make it easier to look at a variety of comparisons. The Y axis is logarythmic since the streams in the Rincon Valley area are generally low flow. 

    Changing the dropdown below will show data for the selected guage.

    [This link](https://maps.waterdata.usgs.gov/mapper/index.html) will take you to the National Water Information System 'Mapper' where you can zoom in and easily find guages:
      - [USGS 09485000 RINCON CREEK NEAR TUCSON, AZ.](https://waterdata.usgs.gov/nwis/inventory?agency_code=USGS&site_no=09485000) - located west of the X9 on Rincon Creek this guage has data back into the 1950s and considering the area of Rincon Creek that is easily accessible today (ie the Arizona Trail crossing east of the Comino Loma Alta trailhead) is the best indication of 'is the creek flowing'
      - [USGS 09484600 PANTANO WASH NEAR VAIL, AZ.](https://waterdata.usgs.gov/nwis/inventory?agency_code=USGS&site_no=09484600) - West of the areas that I believe most hikers use in Cienega Creek - this guage may be misleading since as you hike east into Cienega Creek there is often surface water even if it is only dry sand at the bridge
      - [USGS 09484550 CIENEGA CREEK NEAR SONOITA, AZ.](https://waterdata.usgs.gov/nwis/inventory?agency_code=USGS&site_no=09484550) - far south of the Rincon Valley portion of Cienega Creek but still interesting

    The [Water Services Web](https://waterservices.usgs.gov/) page provides an overview of the data services available to retrieve data. Data in this report is from the Daily Values Service - [Daily Values Service Documentation](https://waterservices.usgs.gov/docs/dv-service/daily-values-service-details/), [Water Services URL Generation Tool](https://waterservices.usgs.gov/test-tools/?service=stat&siteType=&statTypeCd=all&major-filters=sites&format=rdb&date-type=type-period&statReportType=daily&statYearType=calendar&missingData=off&siteStatus=all&siteNameMatchOperator=start).
    """
    )
    return


@app.cell
def _():
    import marimo as mo
    import requests
    import pandas as pd
    from datetime import date

    # --- dropdown with human-readable labels ---
    site_dropdown = mo.ui.dropdown(
        options=[
            "Rincon Creek -- 09485000",
            "Pantano Wash Near Vail -- 09484600",
            "Cienega Creek Near Sonoita -- 09484550",
        ],
        value="Rincon Creek -- 09485000",
        label="Select USGS Site"
    )

    site_dropdown
    return date, mo, pd, requests, site_dropdown


@app.cell
def _(mo, requests, site_dropdown):
    selected_label = site_dropdown.value
    site_id = selected_label[-8:]  # parse last 8 characters

    guage_url = "https://waterservices.usgs.gov/nwis/dv/"
    params = {
        "format": "json",
        "sites": site_id,
        "period": "P3900W",
        "siteStatus": "all"
    }

    guage_response = requests.get(guage_url, params=params)
    guage_data = guage_response.json()

    mo.md(f"Fetched data for **{selected_label}** (site ID: `{site_id}`)")
    return (guage_data,)


@app.cell
def _(guage_data, pd):
    def flatten_usgs_daily(data):
        rows = []
        for ts in data["value"]["timeSeries"]:
            site = ts["sourceInfo"]["siteCode"][0]["value"]
            site_name = ts["sourceInfo"]["siteName"]
            var_code = ts["variable"]["variableCode"][0]["value"]
            stat_code = ts["variable"]["options"]["option"][0]["optionCode"]

            for point in ts["values"][0]["value"]:
                if not point or "dateTime" not in point or "value" not in point:
                    continue
                quals = point.get("qualifiers") or []
                quals_str = ",".join(map(str, quals)) if isinstance(quals, list) else str(quals or "")
                rows.append({
                    "siteCode": site,
                    "siteName": site_name,
                    "variableCode": var_code,
                    "statisticCode": stat_code,
                    "dateTime": point["dateTime"],
                    "value": point["value"],
                    "qualifiers": quals_str
                })

        df = pd.DataFrame(rows)
        if not df.empty:
            df["dateTime"] = pd.to_datetime(df["dateTime"], errors="coerce")
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
            df = df.sort_values("dateTime").reset_index(drop=True)
        return df

    guage_values = flatten_usgs_daily(guage_data)

    assert not (guage_values['dateTime'].dt.floor('D') != guage_values['dateTime']).any(), \
        "Error: Some datetimes have non-zero time components"

    guage_values
    return (guage_values,)


@app.cell
def _(date, guage_values, pd):
    guage_start_date = date(guage_values["dateTime"].min().year + 1, 1, 1)
    guage_end_date = date(date.today().year, 12, 31)

    day_frame = pd.date_range(start=guage_start_date, end=guage_end_date, freq='D')

    day_data = pd.DataFrame(index=day_frame)
    day_data = day_data.reset_index().rename(columns={"index": "dateTime"})

    grouped_guage_days = guage_values.groupby('dateTime').agg(has_flow=('value', lambda x: (x > 0).any()), mean_flow=('value', 'mean'))
    grouped_guage_days['has_data'] = True
    grouped_guage_days.reset_index(inplace=True)

    day_data = day_data.merge(grouped_guage_days, how='left', on='dateTime')

    day_data['year'] = day_data['dateTime'].dt.year
    day_data['month'] = day_data['dateTime'].dt.month

    day_data['has_data'] = day_data['has_data'].astype('boolean')
    day_data['has_flow'] = day_data['has_flow'].astype('boolean')

    day_data['has_data'] = day_data['has_data'].fillna(False)
    day_data['has_flow'] = day_data['has_flow'].fillna(False)

    day_data['month_date_time'] = day_data['dateTime'].values.astype('datetime64[M]')

    day_data
    return day_data, guage_end_date, guage_start_date


@app.cell
def _(day_data, guage_end_date, guage_start_date, pd):
    month_frame = pd.date_range(start=guage_start_date, end=guage_end_date, freq='MS')

    month_data = pd.DataFrame(index=month_frame)
    month_data = month_data.reset_index().rename(columns={"index": "month_date_time"})

    month_day_grouped = day_data.groupby('month_date_time').agg(
        days_with_data=('has_data', 'sum'),
        days_with_flow=('has_flow', 'sum'))

    month_data = month_data.merge(month_day_grouped, how='left', on='month_date_time')

    month_data
    return (month_data,)


@app.cell
def _(mo, month_data, pd):
    import plotly.graph_objects as go
    import plotly.io as pio

    pio.renderers[pio.renderers.default].config['displayModeBar'] = False

    # --- data prep ---
    df = month_data.copy()
    df["month_date_time"] = pd.to_datetime(df["month_date_time"])
    df["year"] = df["month_date_time"].dt.year
    df["month"] = df["month_date_time"].dt.month

    years = sorted(df["year"].unique())
    month_labels = ["Jan","Feb","Mar","Apr","May","Jun",
                    "Jul","Aug","Sep","Oct","Nov","Dec"]

    FACET_W = 90
    FACET_H = 70
    LINE_COLOR = "#3366cc"

    def tiny_fig(year: int) -> go.Figure:
        g = df[df["year"] == year].sort_values("month")

        # mask flow when no data (optional; keep if you used it earlier)
        g["flow_masked"] = g["days_with_flow"].where(g["days_with_data"] > 0, None)

        # --- 1) add headroom so the top of the line isn't clipped ---
        ymax = max(31, float(g["days_with_data"].max() or 0), float(g["days_with_flow"].max() or 0))
        y_top = ymax + 0.6  # small padding; you can use 31.5 if you always cap at 31

        fig = go.Figure()

        # bars
        fig.add_bar(
            x=[month_labels[m-1] for m in g["month"]],
            y=g["days_with_data"],
            marker_color="rgba(200,200,200,0.4)",
            showlegend=False,
            hoverinfo="skip",         
            hovertemplate=None
        )

        # line (no markers)
        fig.add_scatter(
            x=[month_labels[m-1] for m in g["month"]],
            y=g["flow_masked"], 
            mode="lines",
            line=dict(color=LINE_COLOR, width=1.5),  # 3) slightly thinner helps on tiny charts
            showlegend=False,
            connectgaps=False,
            hoverinfo="skip",         
            hovertemplate=None
        )

        fig.update_xaxes(showticklabels=False, ticks="")
        fig.update_yaxes(showticklabels=False, range=[0, y_top])

        # 2) give the title a bit more breathing room from the plot
        fig.update_layout(
            width=FACET_W,
            height=FACET_H,
            margin=dict(t=10, r=0, b=2, l=0),  # was 22
            template="plotly_white",
            title=dict(
                text=str(year),
                x=0.00,
                y=1.00,                  # was 0.98
                xanchor="left",
                yanchor="top",
                font=dict(size=10, color="rgba(0,0,0,0.3)"),
            ),
        )
        return fig

    # build figures for all years
    figs = [tiny_fig(y) for y in years]

    mo.md(
        f"""
        <h2 style="text-align:center;">Days of Flow</h2>
        <hr style="width:100%; border:none; border-top:1px solid #ddd; margin:30px 0;">
        <div style="display:flex; flex-wrap:wrap; column-gap:2px; row-gap:18px; align-items:flex-start;">
          {"".join(f"<div>{mo.as_html(f)}</div>" for f in figs)}
        </div>
        """)

    return


@app.cell
def _(day_data, mo, pd):
    import plotly.express as px

    # --- Ensure datetime column ---
    day_data['dateTime'] = pd.to_datetime(day_data['dateTime'])
    day_data['year'] = day_data['dateTime'].dt.year

    # --- Base figure config as a helper function ---
    def make_flow_chart(start_date, end_date, title_suffix=""):
        fig = px.line(
            day_data,
            x='dateTime',
            y='mean_flow',
            color='year',
            title=None,
            template="plotly_white"
        )
        fig.update_layout(
            autosize=True,
            height=500,
            xaxis_title="Date",
            yaxis_title="Mean Flow",
            showlegend=False,
            margin=dict(l=50, r=50, t=8, b=50),
        )
        fig.update_xaxes(
            rangeslider_visible=True,
            range=[start_date, end_date]
        )
        fig.update_yaxes(type='log')
        return fig

    # --- Create two versions ---
    fig_recent = make_flow_chart(
        day_data['dateTime'].max() - pd.Timedelta(days=5*364),
        day_data['dateTime'].max()
    )
    fig_full = make_flow_chart(
        day_data['dateTime'].max() - pd.Timedelta(days=5*364*2),
        day_data['dateTime'].max() - pd.Timedelta(days=5*364)
    )

    # --- Display both in Marimo Markdown ---
    mo.md(f"""
    <h2 style="text-align:center;">Mean Flow</h2>
        <hr style="width:100%; border:none; border-top:1px solid #ddd; margin:30px 0;">
          <div>
            {mo.as_html(fig_recent)}
          </div>
        <hr style="width:100%; border:none; border-top:1px solid #ddd; margin:30px 0;">
          <div>
            {mo.as_html(fig_full)}
          </div>
    """)

    return


if __name__ == "__main__":
    app.run()
