# Temporary public dashboard demo

This demo entrypoint keeps the dashboard visible without the AWS PostgreSQL/Redpanda
deployment. It reads vehicle positions from the public MBTA V3 API and labels the
engineering KPIs as a historical production snapshot. It does not run the project's
streaming pipeline or claim that those historical metrics are live.

## Deploy to Streamlit Community Cloud

1. Open https://share.streamlit.io/ and sign in with GitHub.
2. Choose **Create app**.
3. Set repository to `rahulrb99/transit-data-platform`.
4. Set branch to `demo/streamlit-snapshot`.
5. Set the main file path to `dashboard/demo_app.py`.
6. Under advanced settings, choose Python 3.11 if available, then deploy.

The app dependencies are in `dashboard/requirements.txt`. The production app
`dashboard/app.py` and `main` branch are intentionally unchanged.

## Limitations

- Live vehicle markers depend on the public MBTA API and network access from the host.
- Historical engineering metrics come from the README's point-in-time production
  snapshot captured on 2026-09-07.
- The stream-processing stack, PostgreSQL queries, S3 backups, and AWS operational
  services are not hosted by this demo app.
- When the AWS-backed production service is restored, the production dashboard can
  be redeployed separately.
