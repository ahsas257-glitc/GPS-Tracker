# Afghanistan GPS Point Tracker

A Streamlit app for reading project Excel datasets from upload, local samples, or Google Drive, extracting configurable GPS columns, validating coordinates, and showing points on an Afghanistan map with province and district boundaries.

The dashboard enriches each valid point with Afghanistan zone, province, and district metadata from the bundled shapefiles. It includes dependent filters, an Afghanistan-locked Folium map, distance measurement, a reusable Settings page, and a separate Reports page with Excel/CSV exports.

## Run

```powershell
pip install -r requirements.txt
streamlit run app.py
```

Open **Settings** first when using a new project. Add the Google Drive folder URL/ID, configure the service-account credentials in Streamlit Secrets, then map the Latitude, Longitude, optional Altitude/Accuracy, rejected-status, and project display columns. Share the Drive folder with the service-account email shown inside your JSON file.

## Google Drive Secrets

For local development, create `.streamlit/secrets.toml`. For Streamlit Cloud, paste the same TOML into **App settings > Secrets**:

```toml
[gdrive_service_account]
type = "service_account"
project_id = "your-project-id"
private_key_id = "your-private-key-id"
private_key = "REPLACE_WITH_PRIVATE_KEY_FROM_SERVICE_ACCOUNT_JSON"
client_email = "your-service-account@your-project.iam.gserviceaccount.com"
client_id = "your-client-id"
auth_uri = "https://accounts.google.com/o/oauth2/auth"
token_uri = "https://oauth2.googleapis.com/token"
auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
client_x509_cert_url = "https://www.googleapis.com/robot/v1/metadata/x509/your-service-account"
universe_domain = "googleapis.com"
```

Never commit `.streamlit/secrets.toml`; it is ignored by `.gitignore`.

Admin access for Settings can be configured in the same secrets file:

```toml
admin_users = [
  { email = "admin@example.com", password = "change-this-password", role = "admin", active = true }
]
```

Open **Reports** to generate report tables and downloads from the active dashboard dataset.

## Expected GPS Columns

- `GPS-Latitude`
- `GPS-Longitude`
- `GPS-Altitude`
- `GPS-Accuracy`

The app also handles close column-name variants automatically, but explicit Settings mappings always take priority.
