import pandas as pd
import os
import io
import zipfile
import streamlit as st
from datetime import datetime

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

    files_read = 0
    files_skipped = 0
    log = []
    zip_buffer = io.BytesIO()

    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for uploaded_file in uploaded_files:
            file_name = uploaded_file.name
            log.append(f"Processing: {file_name}")

            try:
                if file_name.endswith(".xlsx"):
                    df = pd.read_excel(uploaded_file)
                else:
                    df = pd.read_csv(uploaded_file)

                df.columns = df.columns.str.strip()

                # === ADD CALCULATED COLUMNS ===
                added_columns = []
                for source_col, new_col in column_map.items():
                    if source_col in df.columns:
                        df[new_col] = df[source_col] * 4
                        added_columns.append(new_col)
                    else:
                        log.append(f"  ⚠️ Column '{source_col}' not found — '{new_col}' will not be added.")

                if added_columns:
                    log.append(f"  ✅ Added columns: {', '.join(added_columns)}")

                # === GET METER BADGE ===
                if "Meter_Badge" not in df.columns:
                    log.append(f"  ⚠️ 'Meter_Badge' column not found — meter ID set to UNKNOWN.")
                    meter_value = "UNKNOWN"
                else:
                    unique_meters = df["Meter_Badge"].astype(str).str.strip().unique()
                    if len(unique_meters) > 1:
                        log.append(f"  ⚠️ Multiple meter IDs found: {list(unique_meters)} — using first value.")
                    meter_value = unique_meters[0]

                # === MAP TO PROJECT NAME ===
                if meter_value not in meter_to_project:
                    log.append(f"  ⚠️ Meter ID '{meter_value}' not in mapping — labeled UNKNOWN.")
                project_name = meter_to_project.get(meter_value, f"ERE-UNKNOWN_{meter_value}")

                project_name_clean = "".join(
                    c for c in project_name if c.isalnum() or c in ("_", "-")
                )

                output_name = f"{project_name_clean}_MeterData_{run_date}.csv"
                csv_bytes = df.to_csv(index=False).encode("utf-8")
                zf.writestr(f"Project Data/{output_name}", csv_bytes)

                log.append(f"  💾 Saved: {output_name}")
                files_read += 1

            except Exception as e:
                log.append(f"  ❌ Error processing {file_name}: {e}")
                files_skipped += 1

    zip_buffer.seek(0)

    # === DISPLAY RESULTS ===
    with st.expander("Processing log", expanded=True):
        st.text("\n".join(log))

    col1, col2 = st.columns(2)
    col1.metric("Files Processed", files_read)
    col2.metric("Files Skipped", files_skipped)

    st.download_button(
        label="Download all output files (.zip)",
        data=zip_buffer,
        file_name=f"GMP_ProjectData_{run_date}.zip",
        mime="application/zip"
    )
