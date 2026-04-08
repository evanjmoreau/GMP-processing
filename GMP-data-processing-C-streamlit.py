import pandas as pd
import os
import io
import zipfile
import streamlit as st
from datetime import datetime
import calendar

# === CONFIG ===
run_date = datetime.today().strftime("%m%d%y")

meter_to_project = {
    "6214641"   : "ERE-Olde-Farmhouse-Rd_Solar",
    "E36202156" : "ERE-I-Love-Cows_Solar",
    "5253372"   : "ERE-Boardman-Hill_Solar",
    "6046437"   : "ERE-Danyow-Rd_Solar",
    "6214663"   : "ERE-Nava_Storage-PROD",
    "6214676"   : "ERE-Nava_Storage-AUX",
    "6214648"   : "ERE-South-St_Storage-PROD",
    "6068450"   : "ERE-South-St_Storage-AUX",
    "6096680"   : "ERE-Nava-Solar",
    "E18070996" : "ERE-SMC_Solar"
}

column_map = {
    "KWH_Consumed"   : "KW_Consumed",
    "KWH_Generated"  : "KW_Generated",
    "KVARH_Consumed" : "KVAR_Consumed",
    "KVARH_Generated": "KVAR_Generated"
}

# === UI ===
st.title("GMP Meter Data Processor")
st.markdown("Upload one or more `.xlsx` or `.csv` meter data files to process.")

uploaded_files = st.file_uploader(
    "Choose input files",
    type=["xlsx", "csv"],
    accept_multiple_files=True
)

if uploaded_files and st.button("Process Files", type="primary"):

    project_data = {}
    files_read = 0
    files_skipped = 0
    log = []

    # === STEP 1: READ FILES ===
    st.subheader("Reading files...")
    for uploaded_file in uploaded_files:
        file_name = uploaded_file.name
        log.append(f"Reading: {file_name}")

        try:
            if file_name.endswith(".xlsx"):
                df = pd.read_excel(uploaded_file)
            else:
                df = pd.read_csv(uploaded_file)

            df.columns = df.columns.str.strip()

            for source_col, new_col in column_map.items():
                if source_col in df.columns:
                    df[new_col] = df[source_col] * 4
                else:
                    log.append(f"  ⚠️ Column '{source_col}' not found — '{new_col}' will not be added.")

            if "Meter_Badge" not in df.columns:
                log.append(f"  ⚠️ 'Meter_Badge' column not found — skipping file.")
                files_skipped += 1
                continue

            unique_meters = df["Meter_Badge"].astype(str).str.strip().unique()
            meter_value = unique_meters[0]

            if meter_value not in meter_to_project:
                log.append(f"  ⚠️ Meter ID '{meter_value}' not in mapping — skipping.")
                files_skipped += 1
                continue

            project_name = meter_to_project[meter_value]
            log.append(f"  ✅ Mapped to: {project_name} ({len(df)} rows)")

            if project_name not in project_data:
                project_data[project_name] = []
            project_data[project_name].append(df)
            files_read += 1

        except Exception as e:
            log.append(f"  ❌ Error reading {file_name}: {e}")
            files_skipped += 1

    # === STEP 2: PROCESS AND BUILD ZIP ===
    summary = []
    zip_buffer = io.BytesIO()

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for project_name, df_list in project_data.items():
            project_name_clean = "".join(
                c for c in project_name if c.isalnum() or c in ("_", "-")
            )

            combined = pd.concat(df_list, ignore_index=True)
            rows_before = len(combined)

            if "Msrmt_Local_Dttm" not in combined.columns:
                log.append(f"  ⚠️ 'Msrmt_Local_Dttm' not found in {project_name} — skipping.")
                continue

            combined["Msrmt_Local_Dttm"] = pd.to_datetime(combined["Msrmt_Local_Dttm"], format="mixed")
            combined = combined.drop_duplicates()
            rows_removed = rows_before - len(combined)

            combined = combined.sort_values("Msrmt_Local_Dttm").reset_index(drop=True)
            combined["_year_month"] = combined["Msrmt_Local_Dttm"].dt.to_period("M")

            for period, month_df in combined.groupby("_year_month"):
                month_df = month_df.drop(columns=["_year_month"])
                ts = period.to_timestamp()
                month_label = ts.strftime("%B-%Y")
                output_name = f"{project_name_clean}_{month_label}_MeterData_{run_date}.csv"

                days_in_month = calendar.monthrange(ts.year, ts.month)[1]
                expected_rows = days_in_month * 24 * 4
                actual_rows = len(month_df)
                completeness = (actual_rows / expected_rows) * 100

                if actual_rows == expected_rows:
                    status = "Complete"
                elif actual_rows > expected_rows:
                    status = f"Over ({actual_rows - expected_rows} extra rows)"
                else:
                    status = f"Incomplete ({expected_rows - actual_rows} rows missing)"

                csv_bytes = month_df.to_csv(index=False).encode("utf-8")
                zf.writestr(f"Project Data/{output_name}", csv_bytes)

                summary.append({
                    "Project"            : project_name,
                    "Month"              : month_label,
                    "Actual Rows"        : actual_rows,
                    "Expected Rows"      : expected_rows,
                    "Completeness"       : f"{completeness:.1f}%",
                    "Duplicates Removed" : rows_removed,
                    "Status"             : status
                })

    zip_buffer.seek(0)

    # === STEP 3: DISPLAY RESULTS ===
    with st.expander("Processing log", expanded=False):
        st.text("\n".join(log))

    st.subheader("Summary")
    col1, col2, col3 = st.columns(3)
    col1.metric("Files Read", files_read)
    col2.metric("Files Skipped", files_skipped)
    col3.metric("Output Files", len(summary))

    if summary:
        st.dataframe(pd.DataFrame(summary), use_container_width=True)

    st.download_button(
        label="Download all output files (.zip)",
        data=zip_buffer,
        file_name=f"GMP_ProjectData_{run_date}.zip",
        mime="application/zip"
    )
