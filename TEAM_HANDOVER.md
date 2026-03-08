Version: 0.2.24

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
6. `install_mamo_mac_app.sh`
7. `nohup ./start.sh &`

## Update
- `./update_ManageMovie.sh`

## Autostart
- User-Watchdog per Cron
- Root-Watchdog per LaunchDaemon
- `Beim Booten starten` ist standardmäßig aktiv und kann in der UI deaktiviert werden
