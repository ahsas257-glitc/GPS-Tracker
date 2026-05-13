from __future__ import annotations

from datetime import datetime

import pandas as pd
import streamlit as st

import app as tracker


def active_dataset() -> tuple[pd.DataFrame, pd.DataFrame, dict, int]:
    settings = tracker.load_project_settings()
    profiles = tracker.dataset_profiles(settings)
    active_settings = settings
    rejected_count = int(st.session_state.get("gps_rejected_count", 0))

    with st.sidebar:
        st.markdown("### Data")
        selected_dataset_name = ""
        if profiles:
            names = tracker.dataset_names(settings)
            default_name = str(settings.get("active_dataset") or names[0])
            selected_dataset_name = st.selectbox("Project dataset", names, index=names.index(default_name) if default_name in names else 0, key="report_dataset_profile")
            active_settings = tracker.settings_for_dataset(settings, selected_dataset_name)
            st.caption("Source: configured dataset in Settings (Google Drive).")
        else:
            st.warning("No saved datasets yet. Open Settings to add one.")
        use_drive = True
        use_samples = False

    raw = tracker.load_selected_files([], use_samples, use_drive, active_settings)
    if raw.empty:
        st.info("No active dataset found. Configure Google Drive files in Settings and try again.")
        st.stop()

    points, mapping, rejected_count = tracker.prepare_points(raw, active_settings)
    if points.empty:
        st.error("No usable latitude/longitude columns were found.")
        st.write(mapping)
        st.stop()

    st.session_state["gps_points"] = points
    st.session_state["gps_mapped"] = tracker.mapped_points(points)
    st.session_state["gps_rejected_count"] = rejected_count
    st.session_state["gps_column_mapping"] = mapping
    st.session_state["gps_dataset_name"] = selected_dataset_name
    return points.copy(), raw.copy(), active_settings, rejected_count


def detect_date_column(df: pd.DataFrame) -> str | None:
    candidates = ["Date_And_Time", "SubmissionDate", "submission_time", "starttime", "start", "date", "Date", "timestamp"]
    for col in candidates:
        if col in df.columns:
            parsed = pd.to_datetime(df[col], errors="coerce")
            if parsed.notna().sum() > 0:
                return col
    return None


def data_health_summary(points: pd.DataFrame, mapped: pd.DataFrame, filtered: pd.DataFrame, raw: pd.DataFrame, rejected_count: int) -> pd.DataFrame:
    total_raw = len(raw)
    accepted = len(points)
    mapped_count = len(mapped)
    filtered_count = len(filtered)
    missing_gps = int((~points["has_coordinates"]).sum()) if "has_coordinates" in points.columns else 0
    outside = int((points["has_coordinates"] & ~points["inside_afghanistan"]).sum()) if "has_coordinates" in points.columns and "inside_afghanistan" in points.columns else 0
    rows = [
        {"metric": "Raw rows", "value": total_raw, "share_%": 100.0 if total_raw else 0.0},
        {"metric": "Rejected rows", "value": rejected_count, "share_%": round(rejected_count / max(total_raw, 1) * 100, 1)},
        {"metric": "Accepted rows", "value": accepted, "share_%": round(accepted / max(total_raw, 1) * 100, 1)},
        {"metric": "Missing GPS", "value": missing_gps, "share_%": round(missing_gps / max(accepted, 1) * 100, 1)},
        {"metric": "Outside Afghanistan", "value": outside, "share_%": round(outside / max(accepted, 1) * 100, 1)},
        {"metric": "Mapped points", "value": mapped_count, "share_%": round(mapped_count / max(accepted, 1) * 100, 1)},
        {"metric": "Filtered points", "value": filtered_count, "share_%": round(filtered_count / max(mapped_count, 1) * 100, 1)},
    ]
    return pd.DataFrame(rows)


def configured_columns_status(points: pd.DataFrame, active_settings: dict) -> pd.DataFrame:
    configured = active_settings.get("columns", {})
    keys = ["latitude", "longitude", "altitude", "accuracy", "review_status"]
    rows: list[dict] = []
    for key in keys:
        col = str(configured.get(key, "") or "").strip()
        exists = bool(col and col in points.columns)
        non_null = int(points[col].notna().sum()) if exists else 0
        completeness = round(non_null / max(len(points), 1) * 100, 1) if exists else 0.0
        rows.append({
            "configured_field": key,
            "column_name": col or "(not set)",
            "exists_in_data": "Yes" if exists else "No",
            "non_null_rows": non_null,
            "completeness_%": completeness,
        })
    return pd.DataFrame(rows)


def build_temporal_trend(df: pd.DataFrame, freq: str = "D") -> tuple[pd.DataFrame, str]:
    date_col = detect_date_column(df)
    if not date_col:
        return pd.DataFrame(), ""
    temp = df.copy()
    temp["_date"] = pd.to_datetime(temp[date_col], errors="coerce")
    temp = temp[temp["_date"].notna()].copy()
    if temp.empty:
        return pd.DataFrame(), date_col
    temp["_bucket"] = temp["_date"].dt.to_period(freq).dt.to_timestamp()
    trend = temp.groupby("_bucket", as_index=False).agg(points=("point_id", "count"))
    if "accuracy" in temp.columns:
        trend_acc = temp.groupby("_bucket", as_index=False).agg(avg_accuracy=("accuracy", "mean"))
        trend = trend.merge(trend_acc, on="_bucket", how="left")
        trend["avg_accuracy"] = trend["avg_accuracy"].round(2)
    trend = trend.rename(columns={"_bucket": "period_start"}).sort_values("period_start")
    return trend, date_col


def highest_risk_rows(df: pd.DataFrame, active_settings: dict, top_n: int = 100) -> pd.DataFrame:
    if df.empty:
        return df
    score = pd.Series(0, index=df.index, dtype="float64")
    if "accuracy" in df.columns:
        fair_max = float(active_settings.get("quality", {}).get("fair_max", 100) or 100)
        score += df["accuracy"].fillna(fair_max * 2) / max(fair_max, 1)
    if "gps_quality" in df.columns:
        penalty = {"Excellent": 0.0, "Good": 0.5, "Fair": 1.0, "Needs review": 2.0, "Missing": 3.0}
        score += df["gps_quality"].map(lambda x: penalty.get(str(x), 1.0)).astype(float)
    out = df.copy()
    out["_risk_score"] = score.round(2)
    out = out.sort_values("_risk_score", ascending=False).head(top_n)
    return out


def kpi_strip(points: pd.DataFrame, mapped: pd.DataFrame, filtered: pd.DataFrame, rejected_count: int) -> None:
    total_rows = len(points)
    avg_accuracy = filtered["accuracy"].mean() if "accuracy" in filtered.columns else None
    excellent_points = int((filtered["gps_quality"] == "Excellent").sum()) if "gps_quality" in filtered.columns else 0
    excellent_share = excellent_points / max(len(filtered), 1) * 100
    mapped_share = len(mapped) / max(len(points), 1) * 100
    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("Report points", f"{len(filtered):,}")
    k2.metric("Mapped share", f"{mapped_share:.1f}%")
    k3.metric("Zones", f"{filtered['map_region'].nunique():,}" if "map_region" in filtered.columns else "0")
    k4.metric("Districts", f"{filtered['map_district'].nunique():,}" if "map_district" in filtered.columns else "0")
    k5.metric("Avg accuracy", "-" if avg_accuracy is None or pd.isna(avg_accuracy) else f"{avg_accuracy:,.1f} m")
    k6.metric("Excellent GPS", f"{excellent_share:.1f}%", delta=f"{excellent_points:,} points", delta_color="off")
    st.caption(f"{total_rows:,} accepted rows | {rejected_count:,} rejected rows ignored")


def main() -> None:
    st.set_page_config(
        page_title="GPS Tracker Reports",
        page_icon=tracker.APP_PAGE_ICON,
        layout="wide",
        initial_sidebar_state="expanded",
    )
    tracker.page_style()
    tracker.render_sidebar_nav()

    st.markdown(
        """
        <div class="page-header">
            <h1>Reports</h1>
            <p>Advanced analytics, quality intelligence, and executive exports powered by your Settings profile.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    points, raw, active_settings, rejected_count = active_dataset()
    mapped = tracker.mapped_points(points)
    if mapped.empty:
        st.warning("There are no points inside Afghanistan to report.")
        st.stop()

    filtered, _filters = tracker.location_filter_panel(mapped, key_prefix="report")
    if filtered.empty:
        st.warning("No records match the selected report filters.")
        st.stop()

    kpi_strip(points, mapped, filtered, rejected_count)
    health = data_health_summary(points, mapped, filtered, raw, rejected_count)
    quality_df = tracker.quality_summary(filtered)
    sources_df = tracker.source_summary(filtered)
    zones_df = tracker.location_summary(filtered, ["map_region"])
    provinces_df = tracker.location_summary(filtered, ["map_region", "map_province"])
    districts_df = tracker.location_summary(filtered, ["map_region", "map_province", "map_district"])

    st.markdown('<div class="section-title">Executive Summary</div>', unsafe_allow_html=True)
    summary_tabs = st.tabs(["Health", "Zones", "Quality", "Provinces", "Districts", "Sources", "Schema"])
    with summary_tabs[0]:
        tracker.safe_dataframe(health, use_container_width=True, hide_index=True, height=320)
    with summary_tabs[1]:
        tracker.safe_dataframe(zones_df, use_container_width=True, height=320, hide_index=True)
    with summary_tabs[2]:
        tracker.safe_dataframe(quality_df, use_container_width=True, height=320, hide_index=True)
    with summary_tabs[3]:
        st.bar_chart(provinces_df.set_index("map_province")["points"] if not provinces_df.empty else pd.Series(dtype=int))
        tracker.safe_dataframe(provinces_df, use_container_width=True, height=360, hide_index=True)
    with summary_tabs[4]:
        tracker.safe_dataframe(districts_df, use_container_width=True, height=460, hide_index=True)
    with summary_tabs[5]:
        tracker.safe_dataframe(sources_df, use_container_width=True, height=360, hide_index=True)
    with summary_tabs[6]:
        schema = configured_columns_status(points, active_settings)
        tracker.safe_dataframe(schema, use_container_width=True, height=320, hide_index=True)

    st.markdown('<div class="section-title">Temporal Intelligence</div>', unsafe_allow_html=True)
    freq = tracker.safe_segmented_control("Trend granularity", options=["Daily", "Weekly", "Monthly"], default="Daily")
    freq_map = {"Daily": "D", "Weekly": "W", "Monthly": "M"}
    trend, date_col = build_temporal_trend(filtered, freq=freq_map.get(str(freq), "D"))
    if trend.empty:
        st.info("No valid date/time column found for temporal analysis.")
    else:
        st.caption(f"Time source column: `{date_col}`")
        st.line_chart(trend.set_index("period_start")["points"])
        if "avg_accuracy" in trend.columns:
            st.line_chart(trend.set_index("period_start")["avg_accuracy"])
        tracker.safe_dataframe(trend, use_container_width=True, height=280, hide_index=True)

    st.markdown('<div class="section-title">Risk & Outlier Review</div>', unsafe_allow_html=True)
    top_n = st.slider("Rows to inspect", min_value=20, max_value=300, step=20, value=100)
    risky = highest_risk_rows(filtered, active_settings, top_n=top_n)
    if risky.empty:
        st.info("No rows available for risk review.")
    else:
        risk_cols = tracker.unique_preserve_order(tracker.display_columns_for(risky, active_settings) + ["_risk_score"])
        tracker.safe_dataframe(risky[risk_cols], use_container_width=True, height=360)

    st.markdown('<div class="section-title">Export Center</div>', unsafe_allow_html=True)
    export_cols = st.columns([1, 1, 2], gap="medium")
    with export_cols[0]:
        st.download_button(
            "Download report Excel",
            data=tracker.excel_report_bytes(filtered, points),
            file_name="gps_tracker_afghanistan_report.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
    with export_cols[1]:
        st.download_button(
            "Download filtered CSV",
            data=tracker.csv_bytes(filtered),
            file_name="gps_tracker_report_points.csv",
            mime="text/csv",
            use_container_width=True,
        )
    with export_cols[2]:
        export_payload = {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "dataset_name": st.session_state.get("gps_dataset_name", ""),
            "kpis": health.to_dict(orient="records"),
            "quality_breakdown": quality_df.to_dict(orient="records"),
            "sources": sources_df.to_dict(orient="records"),
        }
        st.download_button(
            "Download report summary JSON",
            data=pd.Series(export_payload).to_json(force_ascii=False, indent=2).encode("utf-8"),
            file_name="gps_tracker_report_summary.json",
            mime="application/json",
            use_container_width=True,
        )

    st.markdown('<div class="section-title">Filtered Records</div>', unsafe_allow_html=True)
    visible_cols = tracker.unique_preserve_order(tracker.display_columns_for(filtered, active_settings))
    tracker.safe_dataframe(filtered[visible_cols], use_container_width=True, height=460)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        st.error(f"Runtime error: {type(exc).__name__}")
        tracker.render_professional_error(
            "Reports page temporarily unavailable",
            "A runtime issue occurred while opening Reports. Please refresh once. If it continues, contact the administrator.",
        )
