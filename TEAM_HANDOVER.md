Version: 0.2.21

# Team Handover ManageMovie MAC

## Betrieb
- Repo ist für fremde Macs bestimmt
- Web-UI läuft auf `https://<Mac-IP>:8126/`
- Port `443` bleibt frei

## Standardablauf
1. Repo klonen
2. `setup.sh`
3. `setup_https.sh`
4. `setup_mariadb.sh`
5. `install_launchdaemon_service.sh`
6. `start.sh`

## Update
- `./update_ManageMovie.sh`

## Autostart
- User-Watchdog per Cron
- Root-Watchdog per LaunchDaemon
