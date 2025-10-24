import marimo

__generated_with = "0.17.0"
app = marimo.App(width="full", app_title="Rincon Valley USGS Gauges")


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Great Rincon Valley Area USGS Stream Gauges

    There are three USGS Stream gauges relevant for the Rincon Valley. This notebook shows two different stats:
     - Days of Flow: For a frequently dry stream and often low flow streams like Rincon Creek the Days of Flow is the closest metric to answering the main question 'is there water'. This is shown as small multiples - it is interesting to see similiar years or groups of years.
     - Mean Flow: For stream with steady flow the mean flow is more interesting that 'Days of Flow'. Two of the same chart are shown to make it easier to look at a variety of comparisons. The Y axis is logarythmic since the streams in the Rincon Valley area are generally low flow. 

    Changing the dropdown below will show data for the selected guage.

    [This link](https://maps.waterdata.usgs.gov/mapper/index.html) will take you to the National Water Information System 'Mapper' where you can zoom in and easily find gauges:
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

        month_data = pd.DataFrame(rows)
        if not month_data.empty:
            month_data["dateTime"] = pd.to_datetime(month_data["dateTime"], errors="coerce")
            month_data["value"] = pd.to_numeric(month_data["value"], errors="coerce")
            month_data = month_data.sort_values("dateTime").reset_index(drop=True)
        return month_data

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
        days_with_flow=('has_flow', 'sum'),
        max_mean_flow=('mean_flow', 'max'))

    month_data = month_data.merge(month_day_grouped, how='left', on='month_date_time')

    month_data['max_mean_flow'] = month_data['max_mean_flow'].fillna(0)
    month_data['days_with_data'] = month_data['days_with_data'].fillna(0).astype(int)

    month_data["year"]  = month_data["month_date_time"].dt.year
    month_data["month"] = month_data["month_date_time"].dt.month

    month_numbers = range(1, 13)

    month_data
    return month_data, month_numbers


@app.cell
def _(mo, month_data):
    import numpy as np
    import plotly.graph_objects as go

    years = sorted(month_data["year"].unique())

    FACET_W   = 90
    FACET_H   = 70
    LINE_COLOR = "#3366cc"

    def _nanmax0(a):
        """nan-safe max -> 0 when all NaN/empty"""
        try:
            m = np.nanmax(a)
            return 0 if np.isnan(m) else float(m)
        except ValueError:  # empty
            return 0

    def tiny_fig(year: int) -> go.Figure:
        g = month_data[month_data["year"] == year].sort_values("month").copy()

        # bars should show 0 for missing months
        g["days_with_data"] = g["days_with_data"].fillna(0)

        # line hidden where there is no data that month
        # keep actual flow values where data exists; otherwise None breaks the line
        flow_vals = g["days_with_flow"].to_numpy(dtype=float)
        flow_vals = np.where(g["days_with_data"].to_numpy() > 0, flow_vals, np.nan)
        flow_vals = np.where(np.isnan(flow_vals), None, flow_vals)  # Plotly wants None to break lines

        # safe headroom; base at 0
        ymax_data = _nanmax0(g["days_with_data"].to_numpy(dtype=float))
        ymax_flow = _nanmax0(g["days_with_flow"].to_numpy(dtype=float))
        y_top = max(31.0, ymax_data, ymax_flow) + 0.6

        fig = go.Figure()

        # bars
        fig.add_bar(
            x=g["month"],  # 1..12, ticks hidden anyway
            y=g["days_with_data"],
            marker_color="rgba(200,200,200,0.4)",
            showlegend=False,
            hoverinfo="skip",
            hovertemplate=None
        )

        # line (slightly thinner)
        fig.add_scatter(
            x=g["month"],
            y=flow_vals,
            mode="lines",
            line=dict(color=LINE_COLOR, width=2),
            showlegend=False,
            connectgaps=False,
            hoverinfo="skip",
            hovertemplate=None
        )

        fig.update_xaxes(showticklabels=False, ticks="")
        fig.update_yaxes(showticklabels=False, range=[0, y_top])

        # layout; use annotation for the year label to avoid title quirks
        fig.update_layout(
            width=FACET_W,
            height=FACET_H,
            margin=dict(t=10, r=0, b=2, l=0),
            template="plotly_white",
            bargap=0.2,
            annotations=[
                dict(
                    x=0.0, y=1.2,                  # ← move higher above bars
                    xref="paper", yref="paper",
                    text=str(year),
                    showarrow=False,
                    xanchor="left", yanchor="top",
                    font=dict(size=9,              # ← slightly larger
                              color="rgba(0,0,0,0.3)")
                )
            ],
        )

        return fig

    # build figures for all years
    figs = [tiny_fig(y) for y in years]

    # static, no interactivity
    static_config = {
        "staticPlot": True,
        "displayModeBar": False,
        "displaylogo": False,
    }

    tiles = [mo.ui.plotly(f, config=static_config) for f in figs]

    mo.md(
        f"""
        <h2 style="text-align:center;">Days of Flow</h2>
        <hr style="width:100%; border:none; border-top:1px solid #ddd; margin:30px 0;">
        <div style="display:flex; flex-wrap:wrap; column-gap:2px; row-gap:12px; align-items:flex-start;">
          {"".join(f"<div>{p}</div>" for p in tiles)}
        </div>
        <hr style="width:100%; border:none; border-top:1px solid #ddd; margin:30px 0;">
        """
    )
    return go, np, years


@app.cell
def _(go, mo, month_data, month_numbers, np, years):
    pivot = (
        month_data.pivot_table(
            index="year",
            columns="month",
            values=["max_mean_flow", "days_with_data"],
            aggfunc={"max_mean_flow": "mean", "days_with_data": "sum"},
        )
        .reindex(index=years)  # keep years in desired order
        .reindex(columns=month_numbers, level="month")  # ensure months 1..12
    )

    mmf = pivot["max_mean_flow"]
    dwd = pivot["days_with_data"]

    mmf_vals = mmf.to_numpy(dtype=float)
    dwd_vals = dwd.to_numpy(dtype=float)

    # --- Mask only for truly "no data" cells ---
    no_data_mask = (dwd_vals == 0) | np.isnan(dwd_vals)

    mmf_for_log = mmf + 1
    z_logged = np.log10(mmf_for_log)

    # Then mask only no-data cells in z_logged so Plotly leaves them transparent
    z_logged_masked = np.where(no_data_mask, np.nan, z_logged)

    # --- Hover text ---
    heat_hover_text = np.empty_like(mmf_vals, dtype=object)
    heat_hover_text[no_data_mask] = "No Data"
    heat_hover_text[~no_data_mask] = np.char.mod("%.2f", mmf_vals[~no_data_mask])

    month_labels = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

    fig = go.Figure(
        data=go.Heatmap(
            z=z_logged_masked,
            x=month_labels,
            y=mmf.index.astype(str),
            colorscale="PuBu",
            text=heat_hover_text,
            hovertemplate=(
                "Year: %{y}<br>"
                "Month: %{x}<br>"
                "max_mean_flow: %{text}<extra></extra>"
            ),
            colorbar=dict(title="log₁₀"),
            showscale=True,
            hoverongaps=False
        )
    )

    # Make NaN (no data) cells render as white by using a white plot background
    num_years = len(mmf.index)
    row_height = 20
    total_height = max(400, num_years * row_height)

    fig.update_layout(
        title="",
        xaxis=dict(title="", fixedrange=True, showgrid=False),
        yaxis=dict(
            title="",
            autorange="reversed",
            fixedrange=True,
            showgrid=False,
            tickmode="array",
            tickvals=mmf.index,
            ticktext=mmf.index.astype(str),
        ),
        margin=dict(l=4, r=4, t=10, b=10),
        height=total_height,
        paper_bgcolor="white",  # background behind NaN tiles
        plot_bgcolor="white",
    )

    heat_map_tile = mo.ui.plotly(
        fig,
        config={
            "displayModeBar": False,
            "scrollZoom": False,
            "doubleClick": False,
            "staticPlot": True# tooltips only
        },
    )

    mo.md(
        f"""
        <h2 style="text-align:center;">Max Daily Mean Flow by Year and Month</h2>
        <hr style="width:100%; border:none; border-top:1px solid #ddd; margin:30px 0;">
        {heat_map_tile}
        <hr style="width:100%; border:none; border-top:1px solid #ddd; margin:30px 0;">
        </div>
        """
    )

    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
