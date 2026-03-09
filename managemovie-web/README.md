# ManageMovie Web (0.2.25)

Dieser Unterbau enthält die Web-App und den Runner für den Mac-Betrieb.

## Relevante Pfade
- `app/managemovie.py`
- `app/run_managemovie.sh`
- `web/app.py`
- `start_web.sh`

## Betrieb auf macOS
- Start über das Top-Level-`start.sh`
- Stop über das Top-Level-`stop.sh`
- Benutzer-Autostart über `install_launchagent_service.sh`
- Root-Watchdog für Systemboot über `install_launchdaemon_service.sh`
- HTTPS-Zertifikate über `setup_https.sh`
