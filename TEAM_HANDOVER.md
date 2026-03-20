Version: 0.2.39

# Team Handover ManageMovie MAC

## Betrieb
- Repo ist für fremde Macs bestimmt
- Web-UI läuft auf `https://<Mac-IP>:8126/`
- Port `443` bleibt frei

## Standardablauf
1. Repo klonen
2. `setup.sh` kopiert nach `/Applications/ManageMovie`
3. `/Applications/ManageMovie/setup_https.sh`
4. `/Applications/ManageMovie/setup_mariadb.sh`
5. `/Applications/ManageMovie/install_launchdaemon_service.sh`
6. `/Applications/ManageMovie/install_mamo_mac_app.sh`
7. `nohup /Applications/ManageMovie/start.sh &`

## Deinstallation
- `./uninstall_ManageMovie.sh`

## Update
- `./update_ManageMovie.sh`

## Autostart
- User-Watchdog per Cron
- Root-Watchdog per LaunchDaemon
- `Beim Booten starten` ist auf macOS standardmäßig deaktiviert und kann in der UI aktiviert werden
