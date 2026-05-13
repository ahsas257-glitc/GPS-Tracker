from __future__ import annotations

import json

import pandas as pd
import streamlit as st

import app as tracker


SOURCE_TO_KEY = {"Google Drive": "google_drive", "Upload files": "upload", "Sample files": "samples"}
KEY_TO_SOURCE = {value: key for key, value in SOURCE_TO_KEY.items()}


def option_index(options: list[str], value: str | None, fallback: str = "Auto detect") -> int:
    if value and value in options:
        return options.index(value)
    return options.index(fallback) if fallback in options else 0


def best_candidate(columns: list[str], kind: str, settings: dict) -> str | None:
    configured = tracker.configured_column(settings, kind, columns)
    if configured:
        return configured
    candidates = tracker.smart_column_candidates(columns, kind, settings)
    return str(candidates[0]["column"]) if candidates else tracker.find_column(columns, kind)


def mapping_select(label: str, columns: list[str], current: str, suggested: str | None, key: str, required: bool = False) -> str:
    options = ["Auto detect"] + ([] if required else ["Not used"]) + columns
    initial = current if current in columns else suggested or "Auto detect"
    selected = st.selectbox(label, options, index=option_index(options, initial), key=key)
    if selected == "Auto detect":
        return suggested or ""
    if selected == "Not used":
        return ""
    return selected


def load_preview_data(settings: dict, preview_source: str) -> pd.DataFrame:
    if preview_source == "Google Drive":
        return tracker.load_selected_files([], False, True, settings)
    if preview_source == "Sample files":
        return tracker.load_selected_files([], True, False, settings)
    uploaded = st.session_state.get("settings_preview_upload")
    if uploaded:
        return tracker.load_selected_files(uploaded, False, False, settings)
    return pd.DataFrame()


def upsert_dataset(settings: dict, old_name: str, dataset: dict) -> dict:
    migrated = tracker.migrate_project_settings(settings)
    datasets = tracker.dataset_profiles(migrated)
    new_name = str(dataset.get("name", "")).strip()
    replaced = False
    updated = []
    for item in datasets:
        if item.get("name") == old_name or item.get("name") == new_name:
            if not replaced:
                updated.append(dataset)
                replaced = True
            continue
        updated.append(item)
    if not replaced:
        updated.append(dataset)
    migrated["datasets"] = updated
    migrated["active_dataset"] = new_name
    return migrated


def main() -> None:
    st.set_page_config(page_title="GPS Tracker Settings", page_icon=tracker.APP_PAGE_ICON, layout="wide", initial_sidebar_state="expanded")
    tracker.page_style()
    tracker.render_sidebar_nav()
    tracker.require_settings_admin_access()

    settings = tracker.load_project_settings()
    profiles = tracker.dataset_profiles(settings)
    names = tracker.dataset_names(settings)
    secret_status = tracker.google_drive_secret_status()

    st.markdown(
        """
        <div class="page-header">
            <div>
                <div class="page-title">Project Settings</div>
                <div class="page-subtitle">Create named datasets, map their columns once, then select them in Dashboard or Reports.</div>
            </div>
            <div class="page-badge">Multi-dataset setup</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    selector_options = names + ["+ Add new dataset"] if names else ["+ Add new dataset"]
    default_active = str(settings.get("active_dataset") or (names[0] if names else "+ Add new dataset"))
    selected_option = st.selectbox(
        "Dataset profile",
        selector_options,
        index=selector_options.index(default_active) if default_active in selector_options else 0,
        help="This name will appear in Dashboard and Reports.",
    )

    adding_new = selected_option == "+ Add new dataset"
    old_name = selected_option if not adding_new else ""
    current_dataset = tracker.default_dataset_config(f"Dataset {len(names) + 1}") if adding_new else tracker.settings_for_dataset(settings, selected_option)
    drive = current_dataset.get("google_drive", {})
    columns_config = current_dataset.get("columns", {})
    quality = current_dataset.get("quality", {})

    tab_dataset, tab_columns, tab_rules, tab_test, tab_config = st.tabs(["Dataset", "Smart Columns", "Rules", "Test", "Config"])

    with tab_dataset:
        left, right = st.columns([1.15, .85], gap="large")
        with left:
            st.subheader("Dataset Identity")
            dataset_name = st.text_input("Dataset name", value=str(current_dataset.get("name", "")), placeholder="Example: Kabul Survey 2026")
            folder_input = st.text_input(
                "Google Drive folder URL or folder ID",
                value=str(drive.get("folder_url") or drive.get("folder_id") or ""),
            )
            folder_id = tracker.extract_drive_folder_id(folder_input)
            recursive = st.toggle("Read subfolders too", value=bool(drive.get("recursive", False)))
            max_files = st.number_input("Maximum Excel/Google Sheet files to read", min_value=1, max_value=500, value=int(drive.get("max_files", 50) or 50), step=5)
            default_source_label = KEY_TO_SOURCE.get(str(current_dataset.get("data_source", "google_drive")), "Google Drive")
            data_source_label = st.radio(
                "Default source for this dataset",
                ["Google Drive", "Upload files", "Sample files"],
                index=["Google Drive", "Upload files", "Sample files"].index(default_source_label),
                horizontal=True,
            )
        with right:
            st.subheader("Available Profiles")
            if profiles:
                tracker.safe_dataframe(pd.DataFrame([{
                    "name": item.get("name", ""),
                    "source": KEY_TO_SOURCE.get(str(item.get("data_source", "")), item.get("data_source", "")),
                    "folder": tracker.extract_drive_folder_id(str(item.get("google_drive", {}).get("folder_id") or item.get("google_drive", {}).get("folder_url") or "")),
                } for item in profiles]), use_container_width=True, hide_index=True, height=245)
            else:
                st.info("No datasets saved yet.")
            if secret_status["available"]:
                st.success(f"Google Drive secret is ready: {secret_status['client_email']}")
            else:
                st.warning("Google Drive secret was not detected.")

    draft_settings = tracker.deep_merge(settings, {
        "active_dataset": dataset_name,
        "data_source": SOURCE_TO_KEY[data_source_label],
        "google_drive": {
            "folder_id": folder_id,
            "folder_url": folder_input,
            "credentials_source": "streamlit_secrets",
            "credentials_path": str(tracker.CREDENTIALS_PATH),
            "recursive": recursive,
            "max_files": int(max_files),
            "selected_file_ids": drive.get("selected_file_ids", []),
        },
        "columns": columns_config,
        "quality": quality,
    })

    with tab_dataset:
        st.subheader("Drive Files")
        drive_files = []
        if folder_id and secret_status["available"]:
            with st.spinner("Reading Google Drive folder..."):
                drive_files = tracker.list_google_drive_dataset_files(draft_settings)
        elif folder_id:
            st.info("Google Drive folder is set, but the secret is not available.")

        if drive_files:
            file_lookup = {item["path"]: item["id"] for item in drive_files}
            saved_ids = {str(file_id) for file_id in drive.get("selected_file_ids", [])}
            default_labels = [label for label, file_id in file_lookup.items() if file_id in saved_ids]
            if not default_labels:
                default_labels = list(file_lookup.keys())
            selected_file_labels = st.multiselect(
                "Datasets/files to use",
                list(file_lookup.keys()),
                default=default_labels,
                help="Only selected files will be loaded in Dashboard and Reports.",
            )
            selected_file_ids = [file_lookup[label] for label in selected_file_labels]
            tracker.safe_dataframe(pd.DataFrame(drive_files), use_container_width=True, hide_index=True, height=260)
        else:
            selected_file_ids = list(drive.get("selected_file_ids", []))
            st.info("No Excel or Google Sheet files found yet.")

    draft_settings = tracker.deep_merge(draft_settings, {"google_drive": {"selected_file_ids": selected_file_ids}})

    with tab_columns:
        st.subheader("Smart Column Mapping")
        preview_source = st.radio("Read preview columns from", ["Google Drive", "Upload a preview file", "Sample files"], horizontal=True)
        if preview_source == "Upload a preview file":
            st.file_uploader("Preview Excel file", type=["xlsx", "xls"], accept_multiple_files=True, key="settings_preview_upload")

        with st.spinner("Reading preview columns..."):
            raw = load_preview_data(draft_settings, preview_source)

        columns = list(raw.columns) if not raw.empty else []
        suggested = {kind: best_candidate(columns, kind, draft_settings) for kind in ["latitude", "longitude", "altitude", "accuracy"]}

        if raw.empty:
            st.info("No preview data yet. Choose Google Drive, upload a preview file, or use sample data.")
        else:
            st.caption(f"{len(raw):,} rows and {len(columns):,} columns detected for `{dataset_name}`.")

        c1, c2 = st.columns([1, 1], gap="large")
        with c1:
            lat_col = mapping_select("Latitude column", columns, str(columns_config.get("latitude", "")), suggested.get("latitude"), "settings_latitude", required=True)
            lon_col = mapping_select("Longitude column", columns, str(columns_config.get("longitude", "")), suggested.get("longitude"), "settings_longitude", required=True)
            alt_col = mapping_select("Altitude column", columns, str(columns_config.get("altitude", "")), suggested.get("altitude"), "settings_altitude")
            acc_col = mapping_select("Accuracy column", columns, str(columns_config.get("accuracy", "")), suggested.get("accuracy"), "settings_accuracy")

            review_options = ["Not used"] + columns
            review_current = str(columns_config.get("review_status", "review_status") or "")
            review_col = st.selectbox("Rejected/status column", review_options, index=option_index(review_options, review_current if review_current in columns else "Not used", "Not used"))
            review_value = "" if review_col == "Not used" else review_col

            default_display = tracker.selected_display_columns(current_dataset, columns)
            display_cols = st.multiselect(
                "Columns to show/read in tables and exports",
                columns,
                default=default_display,
            )
            popup_base = ["point_id", "source_file", "source_sheet", "map_region", "map_province", "map_district", "latitude", "longitude", "altitude", "accuracy", "gps_quality"]
            popup_options = tracker.unique_preserve_order(popup_base + columns)
            popup_default = tracker.selected_popup_columns(current_dataset, popup_options) or popup_base
            popup_cols = st.multiselect(
                "Columns to show in map popup (point details)",
                popup_options,
                default=popup_default,
                help="These columns will appear in each point popup on the Dashboard map.",
            )
        with c2:
            candidate_rows = []
            for kind in ["latitude", "longitude", "altitude", "accuracy"]:
                candidate_rows.extend(tracker.smart_column_candidates(columns, kind, draft_settings)[:5])
            if candidate_rows:
                tracker.safe_dataframe(pd.DataFrame(candidate_rows), use_container_width=True, hide_index=True, height=260)
            else:
                st.info("No GPS-like columns detected yet.")
            if not raw.empty:
                preview_settings = tracker.deep_merge(draft_settings, {"columns": {"display": display_cols}})
                preview = tracker.ensure_unique_columns(raw.head(40))
                preview_cols = tracker.unique_preserve_order(tracker.display_columns_for(preview, preview_settings))
                tracker.safe_dataframe(preview[preview_cols] if preview_cols else preview, use_container_width=True, height=300)

    with tab_rules:
        st.subheader("Quality Rules")
        q1, q2, q3 = st.columns(3)
        with q1:
            excellent_max = st.number_input("Excellent GPS <= meters", min_value=0.0, value=float(quality.get("excellent_max", 10) or 10), step=1.0)
        with q2:
            good_max = st.number_input("Good GPS <= meters", min_value=0.0, value=float(quality.get("good_max", 30) or 30), step=1.0)
        with q3:
            fair_max = st.number_input("Fair GPS <= meters", min_value=0.0, value=float(quality.get("fair_max", 100) or 100), step=5.0)
        st.caption("The selected rejected/status column excludes rows where value is `REJECTED`.")

    dataset_config = tracker.normalize_dataset_config({
        "name": dataset_name,
        "data_source": SOURCE_TO_KEY[data_source_label],
        "google_drive": {
            "folder_id": folder_id,
            "folder_url": folder_input,
            "credentials_source": "streamlit_secrets",
            "credentials_path": str(tracker.CREDENTIALS_PATH),
            "recursive": recursive,
            "max_files": int(max_files),
            "selected_file_ids": selected_file_ids,
        },
        "columns": {
            "latitude": locals().get("lat_col", str(columns_config.get("latitude", ""))),
            "longitude": locals().get("lon_col", str(columns_config.get("longitude", ""))),
            "altitude": locals().get("alt_col", str(columns_config.get("altitude", ""))),
            "accuracy": locals().get("acc_col", str(columns_config.get("accuracy", ""))),
            "review_status": locals().get("review_value", str(columns_config.get("review_status", "review_status") or "")),
            "display": locals().get("display_cols", columns_config.get("display", [])),
            "popup_display": locals().get("popup_cols", columns_config.get("popup_display", [])),
        },
        "quality": {
            "excellent_max": locals().get("excellent_max", float(quality.get("excellent_max", 10) or 10)),
            "good_max": locals().get("good_max", float(quality.get("good_max", 30) or 30)),
            "fair_max": locals().get("fair_max", float(quality.get("fair_max", 100) or 100)),
        },
    }, dataset_name or "Dataset")

    full_dataset_settings = tracker.deep_merge(settings, dataset_config)
    full_dataset_settings["active_dataset"] = dataset_config["name"]

    with tab_test:
        st.subheader("Dataset Test")
        checks = [
            {"check": "Dataset name", "status": "OK" if dataset_config["name"] else "Missing", "detail": dataset_config["name"] or "Add a name"},
            {"check": "Drive folder", "status": "OK" if folder_id else "Missing", "detail": folder_id or "Add folder URL/ID"},
            {"check": "Service account secret", "status": "OK" if secret_status["available"] else "Missing", "detail": str(secret_status["client_email"] or secret_status["source"])},
            {"check": "Latitude column", "status": "OK" if dataset_config["columns"]["latitude"] else "Missing", "detail": str(dataset_config["columns"]["latitude"] or "Select latitude")},
            {"check": "Longitude column", "status": "OK" if dataset_config["columns"]["longitude"] else "Missing", "detail": str(dataset_config["columns"]["longitude"] or "Select longitude")},
        ]
        tracker.safe_dataframe(pd.DataFrame(checks), use_container_width=True, hide_index=True)
        if st.button("Run full test", type="primary", use_container_width=True):
            with st.spinner("Testing selected dataset..."):
                test_raw = load_preview_data(full_dataset_settings, preview_source if "preview_source" in locals() else "Google Drive")
                if test_raw.empty:
                    st.warning("No rows were loaded from the selected source.")
                else:
                    points, mapping, rejected_count = tracker.prepare_points(test_raw, full_dataset_settings)
                    mapped = tracker.mapped_points(points)
                    st.write(mapping)
                    st.success(f"{len(test_raw):,} raw rows, {len(points):,} accepted rows, {len(mapped):,} mapped Afghanistan points, {rejected_count:,} rejected rows.")
                    if not points.empty:
                        tracker.safe_dataframe(points[tracker.display_columns_for(points, full_dataset_settings)].head(50), use_container_width=True, height=320)

    with tab_config:
        st.subheader("Save and Manage")
        save_col, delete_col, cache_col = st.columns(3)
        with save_col:
            if st.button("Save dataset", type="primary", use_container_width=True):
                if not dataset_config["name"]:
                    st.error("Dataset name is required.")
                else:
                    tracker.save_project_settings(upsert_dataset(settings, old_name, dataset_config))
                    st.cache_data.clear()
                    st.success(f"Dataset `{dataset_config['name']}` saved. It will now appear in Dashboard and Reports.")
        with delete_col:
            if st.button("Delete selected dataset", use_container_width=True, disabled=adding_new or not old_name):
                updated = tracker.migrate_project_settings(settings)
                updated["datasets"] = [item for item in tracker.dataset_profiles(updated) if item.get("name") != old_name]
                updated["active_dataset"] = updated["datasets"][0]["name"] if updated["datasets"] else ""
                tracker.save_project_settings(updated)
                st.cache_data.clear()
                st.success(f"Dataset `{old_name}` deleted.")
        with cache_col:
            if st.button("Clear cached data", use_container_width=True):
                st.cache_data.clear()
                st.success("Cache cleared.")

        exported = upsert_dataset(settings, old_name, dataset_config)
        st.download_button(
            "Download all dataset settings JSON",
            data=json.dumps(exported, indent=2, ensure_ascii=False).encode("utf-8"),
            file_name="gps_tracker_dataset_settings.json",
            mime="application/json",
            use_container_width=True,
        )

        imported = st.file_uploader("Import dataset settings JSON", type=["json"], key="settings_import_json")
        if imported is not None:
            try:
                imported_settings = json.loads(imported.getvalue().decode("utf-8"))
                tracker.save_project_settings(tracker.migrate_project_settings(imported_settings))
                st.cache_data.clear()
                st.success("Imported settings saved. Reload the page to see them.")
            except Exception as exc:
                st.error(f"Could not import settings: {exc}")

        st.code(json.dumps(exported, indent=2, ensure_ascii=False), language="json")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        st.error(f"Runtime error: {type(exc).__name__}")
        tracker.render_professional_error(
            "Settings page temporarily unavailable",
            "A runtime issue occurred while opening Settings. Please refresh once. If it continues, contact the administrator.",
        )
