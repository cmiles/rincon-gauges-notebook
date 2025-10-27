import marimo

__generated_with = "0.17.2"
app = marimo.App(width="full", app_title="Rincon Valley USGS Gauges")


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Greater Rincon Valley Area USGS Stream Gauges

    There are three USGS Stream gauges relevant for the Rincon Valley. This notebook shows two different stats:
     - Days of Flow: For a frequently dry stream and often low flow streams like Rincon Creek the Days of Flow is the closest metric to answering the main question 'is there water'. This is shown as small multiples - it is interesting to see similiar years or groups of years.
     - Mean Flow: For stream with steady flow the mean flow is more interesting that 'Days of Flow'. Two of the same chart are shown to make it easier to look at a variety of comparisons. The Y axis is logarythmic since the streams in the Rincon Valley area are generally low flow. 

    Changing the dropdown below will show data for the selected gauge.

    [This link](https://maps.waterdata.usgs.gov/mapper/index.html) will take you to the National Water Information System 'Mapper' where you can zoom in and easily find gauges:
      - [USGS 09485000 RINCON CREEK NEAR TUCSON, AZ.](https://waterdata.usgs.gov/nwis/inventory?agency_code=USGS&site_no=09485000) - located west of the X9 on Rincon Creek this gauge has data back into the 1950s and considering the area of Rincon Creek that is easily accessible today (ie the Arizona Trail crossing east of the Comino Loma Alta trailhead) is the best indication of 'is the creek flowing'
      - [USGS 09484600 PANTANO WASH NEAR VAIL, AZ.](https://waterdata.usgs.gov/nwis/inventory?agency_code=USGS&site_no=09484600) - West of the areas that I believe most hikers use in Cienega Creek - this gauge may be misleading since as you hike east into Cienega Creek there is often surface water even if it is only dry sand at the bridge
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

    gauge_url = "https://waterservices.usgs.gov/nwis/dv/"
    params = {
        "format": "json",
        "sites": site_id,
        "period": "P3900W",
        "siteStatus": "all"
    }

    gauge_response = requests.get(gauge_url, params=params)
    gauge_data = gauge_response.json()

    mo.md(f"Fetched data for **{selected_label}** (site ID: `{site_id}`)")
    return (gauge_data,)


@app.cell
def _(gauge_data, pd):
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

    gauge_values = flatten_usgs_daily(gauge_data)

    assert not (gauge_values['dateTime'].dt.floor('D') != gauge_values['dateTime']).any(), \
        "Error: Some datetimes have non-zero time components"

    gauge_values
    return (gauge_values,)


@app.cell
def _(date, gauge_values, pd):
    gauge_start_date = date(gauge_values["dateTime"].min().year + 1, 1, 1)
    gauge_end_date = date(date.today().year, 12, 31)

    day_frame = pd.date_range(start=gauge_start_date, end=gauge_end_date, freq='D')

    day_data = pd.DataFrame(index=day_frame)
    day_data = day_data.reset_index().rename(columns={"index": "dateTime"})

    grouped_gauge_days = gauge_values.groupby('dateTime').agg(has_flow=('value', lambda x: (x > 0).any()), mean_flow=('value', 'mean'))
    grouped_gauge_days['has_data'] = True
    grouped_gauge_days.reset_index(inplace=True)

    day_data = day_data.merge(grouped_gauge_days, how='left', on='dateTime')

    day_data['year'] = day_data['dateTime'].dt.year
    day_data['month'] = day_data['dateTime'].dt.month

    day_data['has_data'] = day_data['has_data'].astype('boolean')
    day_data['has_flow'] = day_data['has_flow'].astype('boolean')

    day_data['has_data'] = day_data['has_data'].fillna(False)
    day_data['has_flow'] = day_data['has_flow'].fillna(False)

    day_data['month_date_time'] = day_data['dateTime'].values.astype('datetime64[M]')
    day_data['year_date_time'] = day_data['dateTime'].values.astype('datetime64[Y]')

    day_data.sort_values('dateTime', inplace=True)

    day_data
    return day_data, gauge_end_date, gauge_start_date


@app.cell
def _(day_data, gauge_end_date, gauge_start_date, pd):
    import numpy as np

    month_frame = pd.date_range(start=gauge_start_date, end=gauge_end_date, freq='MS')

    month_data = pd.DataFrame(index=month_frame)
    month_data = month_data.reset_index().rename(columns={"index": "month_date_time"})

    month_day_grouped = day_data.groupby('month_date_time').agg(
        days_with_data=('has_data', 'sum'),
        days_with_flow=('has_flow', 'sum'),
        max_mean_flow=('mean_flow', lambda x: np.max(x) if len(x) else np.nan),
        mean_flow=('mean_flow', lambda x: np.mean(x) if len(x) else np.nan),
        twenty_five_quantile_flow=('mean_flow', lambda x: np.quantile(x, 0.25) if len(x) else np.nan),
        seventy_five_quantile_flow=('mean_flow', lambda x: np.quantile(x, 0.75) if len(x) else np.nan),
    ).reset_index()

    month_data = month_data.merge(month_day_grouped, how='left', on='month_date_time')

    month_data['max_mean_flow'] = month_data['max_mean_flow'].fillna(0)
    month_data['days_with_data'] = month_data['days_with_data'].fillna(0).astype(int)

    month_data["year"]  = month_data["month_date_time"].dt.year
    month_data["month"] = month_data["month_date_time"].dt.month

    month_numbers = range(1, 13)

    month_data
    return month_data, month_numbers, np


@app.cell
def _(day_data, np):
    month_summary_data = (day_data.query('has_data').groupby('month').agg(
        max_mean_flow=('mean_flow', 'max'),
        median_flow=('mean_flow', 'median'),
        twenty_five_quantile_flow=('mean_flow', lambda x: np.quantile(x, 0.25)),
        seventy_five_quantile_flow=('mean_flow', lambda x: np.quantile(x, 0.75)),
        ).reindex(np.arange(1, 13), fill_value=np.nan)  # ensure 1..12 present
                .rename_axis('month')
                .reset_index())
    return (month_summary_data,)


@app.cell
def _(mo, month_data, np):

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
    return go, years


@app.cell
def _(go, mo, month_data, month_numbers, np, years):
    # --- Pivot just what you need ---
    mmf = (
        month_data.pivot_table(
            index="year",
            columns="month",
            values="max_mean_flow",
            aggfunc="mean",
        )
        .reindex(index=years)                       # keep years in desired order
        .reindex(columns=month_numbers)             # ensure months 1..12
    )

    # --- Log transform; NaNs stay NaN and will render as gaps/transparent ---
    z_logged = np.log10(mmf + 1)

    # --- Hover text: "No Data" for NaN, else value with 2 decimals ---
    heat_hover_text = (
        mmf.round(2)
           .astype(object)                          # allow mixing floats and strings
           .where(~mmf.isna(), "No Data")
    )

    month_labels = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

    fig = go.Figure(
        data=go.Heatmap(
            z=z_logged.to_numpy(),
            x=month_labels,
            y=mmf.index.astype(str),
            colorscale="PuBu",
            text=heat_hover_text.to_numpy(),
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

    # Size per year for mobile
    row_height = 20
    total_height = max(400, len(mmf.index) * row_height)

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
        paper_bgcolor="white",
        plot_bgcolor="white",
    )

    heat_map_tile = mo.ui.plotly(
        fig,
        config={
            "displayModeBar": False,
            "scrollZoom": False,
            "doubleClick": False,
            "staticPlot": True  # tooltips only
        },
    )

    mo.md(
        f"""
        <h2 style="text-align:center;">Max Daily Mean Flow by Year and Month</h2>
        <hr style="width:100%; border:none; border-top:1px solid #ddd; margin:30px 0;">
        {heat_map_tile}
        <hr style="width:100%; border:none; border-top:1px solid #ddd; margin:30px 0;">
        """
    )
    return


@app.cell
def _(mo, month_summary_data, pd):
    def monthly_flow_band_linear(
        month_summary_data: pd.DataFrame,
        y_title="Flow (cfs)"
    ):
        import numpy as np
        import pandas as pd
        import plotly.graph_objects as go

        # --- Keep only needed columns and ensure months 1–12 ---
        cols = ["month", "median_flow",
                "twenty_five_quantile_flow", "seventy_five_quantile_flow"]
        df = month_summary_data[cols].copy()

        df["month"] = df["month"].astype(int)
        month_index = pd.Index(np.arange(1, 13), name="month")
        df = df.set_index("month").reindex(month_index).reset_index()

        # ensure numeric (preserve NaN)
        for c in ["median_flow", "twenty_five_quantile_flow", "seventy_five_quantile_flow"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        # --- Handle swapped quantiles (rare but safe check) ---
        q25 = df["twenty_five_quantile_flow"].to_numpy(dtype=float)
        q75 = df["seventy_five_quantile_flow"].to_numpy(dtype=float)
        swap_mask = (q25 > q75) & np.isfinite(q25) & np.isfinite(q75)
        if np.any(swap_mask):
            tmp = q25.copy()
            q25[swap_mask] = q75[swap_mask]
            q75[swap_mask] = tmp[swap_mask]

        x = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]

        # --- Plotly figure ---
        fig = go.Figure()

        # Lower bound (q25) invisible anchor
        fig.add_trace(go.Scatter(
            x=x, y=q25,
            mode="lines",
            line=dict(width=0),
            showlegend=False,
            hoverinfo="skip"
        ))

        # Upper bound (q75) filled to q25
        fig.add_trace(go.Scatter(
            x=x, y=q75,
            mode="lines",
            line=dict(width=0),
            fill="tonexty",
            name="25–75% band",
            hoverinfo="skip",
            # optional: set a subtle fill if you want
            # fillcolor="rgba(31,119,180,0.2)"
        ))

        # Median line (+ show band in tooltip via customdata)
        custom = np.column_stack([q25, q75])
        fig.add_trace(go.Scatter(
            x=x, y=df["median_flow"],
            mode="lines+markers",
            name="Median",
            customdata=custom,
            hovertemplate=(
                "Month: %{x}<br>"
                "Median: %{y:.2f} cfs<br>"
                "25–75%: %{customdata[0]:.2f}–%{customdata[1]:.2f} cfs"
                "<extra></extra>"
            )
        ))

        # --- Layout (mobile/static friendly) ---
        fig.update_layout(
            title=None,
            xaxis_title="",
            yaxis_title=y_title,
            margin=dict(l=8, r=8, t=40, b=30),
            height=280,
            legend=dict(orientation="h", y=1.1, x=0),
            hovermode="x unified",
            plot_bgcolor="white",
            paper_bgcolor="white"
        )

        fig.update_xaxes(fixedrange=True)
        fig.update_yaxes(rangemode="tozero", fixedrange=True, tickformat="~s")  # 1.2k style

        return fig

    # --- Marimo tile ---
    monthly_flow_fig = monthly_flow_band_linear(month_summary_data)

    monthly_flow_tile = mo.ui.plotly(
        monthly_flow_fig,
        config={
            "displayModeBar": False,
            "scrollZoom": False,
            "doubleClick": False,   # ok to disable double-click reset
            "staticPlot": False     # keep interactivity for tooltips
        },
    )

    mo.md(
        f"""
        <h2 style="text-align:center;">Monthly Flow - Median with 25-75% Band</h2>
        <hr style="width:100%; border:none; border-top:1px solid #ddd; margin:30px 0;">
        {monthly_flow_tile}
        <hr style="width:100%; border:none; border-top:1px solid #ddd; margin:30px 0;">
        """
    )

    return


@app.cell
def _(day_data, mo, np, pd):
    def fmt_cfs(x):
        if pd.isna(x): return "—"
        if x >= 1000:  return f"{x:,.0f}"
        if x >= 100:   return f"{x:,.0f}"
        if x >= 10:    return f"{x:,.1f}"
        return f"{x:,.2f}"

    def fmt_date(d):
        return d.strftime("%Y-%m-%d") if pd.notna(d) else "—"

    # --- Prepare data ---
    df = day_data.copy()
    df["dateTime"] = pd.to_datetime(df["dateTime"])
    df = df.sort_values("dateTime").reset_index(drop=True)

    if "has_data" not in df.columns:
        df["has_data"] = df["mean_flow"].notna()
    if "has_flow" not in df.columns:
        df["has_flow"] = df["mean_flow"] > 0

    df["year"]  = df["dateTime"].dt.year
    df["month"] = df["dateTime"].dt.month

    # =============================================================================
    # 1) Top 10 Dry Streaks (missing data ends streak)
    # =============================================================================
    # state: -1 = missing data, 1 = dry (flow == 0), 0 = wet (flow > 0)
    state = np.where(
        df["has_data"] == False, -1,
        np.where(df["has_flow"] == False, 1, 0)
    )
    state = pd.Series(state, index=df.index)
    run_id = (state != state.shift()).cumsum()

    dry_mask = (state == 1)
    dry_rows = df.loc[dry_mask].assign(run=run_id[dry_mask].values)

    if not dry_rows.empty:
        dry_summary = (dry_rows.groupby("run")
                                .agg(length=("dateTime", "size"),
                                     start=("dateTime", "min"),
                                     end=("dateTime", "max"))
                                .reset_index(drop=True))
        top10_dry = (dry_summary.sort_values(["length", "start"], ascending=[False, True])
                               .head(10)
                               .reset_index(drop=True))
    else:
        top10_dry = pd.DataFrame(columns=["length","start","end"])

    # =============================================================================
    # 1b) Top 10 Wet Streaks (flow > 0; missing data ends streak)
    # =============================================================================
    wet_mask = (state == 0)
    wet_rows = df.loc[wet_mask].assign(run=run_id[wet_mask].values)

    if not wet_rows.empty:
        wet_summary = (wet_rows.groupby("run")
                                .agg(length=("dateTime", "size"),
                                     start=("dateTime", "min"),
                                     end=("dateTime", "max"))
                                .reset_index(drop=True))
        top10_wet = (wet_summary.sort_values(["length", "start"], ascending=[False, True])
                               .head(10)
                               .reset_index(drop=True))
    else:
        top10_wet = pd.DataFrame(columns=["length","start","end"])

    # =============================================================================
    # 2) Wettest & Driest Years (eligible = ≥1 data day each month)
    # =============================================================================
    data_days = df[df["has_data"]].copy()

    ym_counts = (data_days.groupby(["year","month"])
                            .size()
                            .rename("days_with_data")
                            .reset_index())

    months_per_year = (ym_counts.assign(has_month=ym_counts["days_with_data"]>0)
                                  .groupby("year")["has_month"].sum())

    eligible_years = months_per_year[months_per_year>=12].index

    annual_mean = (data_days[data_days["year"].isin(eligible_years)]
                   .groupby("year")["mean_flow"]
                   .mean()
                   .sort_index())

    if annual_mean.empty:
        wettest = driest = pd.Series(dtype=float)
    else:
        wettest = annual_mean.sort_values(ascending=False).head(10)
        driest  = annual_mean.sort_values(ascending=True).head(10)

    # =============================================================================
    # 3) Top 10 Daily Mean Flow Days
    # =============================================================================
    top10_flow = (
        df.nlargest(10, "mean_flow")[["dateTime", "mean_flow"]]
          .assign(date_str=lambda s: s["dateTime"].dt.strftime("%Y-%m-%d"))
          .reset_index(drop=True)
    )

    # =============================================================================
    # --- HTML Rendering
    # =============================================================================
    def make_dry_list_html():
        if top10_dry.empty:
            return "<li>No dry streaks found</li>"
        return "".join(
            f"<li><strong>{int(r.length)}</strong> days &nbsp; "
            f"({fmt_date(r.start)} → {fmt_date(r.end)})</li>"
            for r in top10_dry.itertuples(index=False)
        )

    def make_wet_list_html():
        if top10_wet.empty:
            return "<li>No wet streaks found</li>"
        return "".join(
            f"<li><strong>{int(r.length)}</strong> days &nbsp; "
            f"({fmt_date(r.start)} → {fmt_date(r.end)})</li>"
            for r in top10_wet.itertuples(index=False)
        )

    def make_year_list_html(series):
        if series.empty:
            return "<li>No eligible years</li>"
        return "".join(
            f"<li><strong>{int(y)}</strong> — {fmt_cfs(v)} cfs</li>"
            for y, v in series.items()
        )

    def make_flow_list_html():
        return "".join(
            f"<li>{r.date_str}: {fmt_cfs(r.mean_flow)} cfs</li>"
            for r in top10_flow.itertuples(index=False)
        )

    html = f"""
    <style>
    .section {{ margin-bottom: 30px; }}
    .top-list {{ margin: 12px 0 0 20px; padding: 0; }}
    .top-list li {{ margin: 4px 0; }}
    h3 {{ margin: 16px 0 6px 0; }}
    </style>

    <div class="section">
      <h3>Top 10 Wet Streaks</h3>
      <ol class="top-list">{make_wet_list_html()}</ol>
    </div>

    <div class="section">
      <h3>Top 10 Dry Streaks</h3>
      <ol class="top-list">{make_dry_list_html()}</ol>
    </div>

    <div class="section">
      <h3>Top 10 Wettest Years (annual mean, full coverage)</h3>
      <ol class="top-list">{make_year_list_html(wettest)}</ol>
    </div>

    <div class="section">
      <h3>Top 10 Driest Years (annual mean, full coverage)</h3>
      <ol class="top-list">{make_year_list_html(driest)}</ol>
    </div>

    <div class="section">
      <h3>Top 10 Daily Mean Flow Days</h3>
      <ol class="top-list">{make_flow_list_html()}</ol>
    </div>
    """

    # In Marimo:
    mo.md(html)

    return


if __name__ == "__main__":
    app.run()
