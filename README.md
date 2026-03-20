# ManageMovie MAC 0.2.39

ManageMovie MAC ist das eigenständige öffentliche Installations-Repository für den Betrieb auf einem fremden Mac. Dieses Repo enthält nur die Dateien, die für Setup, HTTPS, MariaDB, Update, Start/Stop-App und Autostart auf macOS nötig sind.

## Zielplattform
- macOS auf Apple Silicon oder Intel
- HTTPS-Web-UI auf Port `8126`
- Lokale MariaDB auf dem Mac

## Installation auf einem fremden Mac
```bash
tmpdir="$(mktemp -d /tmp/managemovie-mac.XXXXXX)" && git clone https://github.com/paddythefish-brumm/ManageMovie-MAC.git "$tmpdir/ManageMovie-MAC" && "$tmpdir/ManageMovie-MAC/setup.sh" && /Applications/ManageMovie/setup_https.sh && /Applications/ManageMovie/setup_mariadb.sh && /Applications/ManageMovie/install_launchdaemon_service.sh && /Applications/ManageMovie/install_mamo_mac_app.sh && nohup /Applications/ManageMovie/start.sh >/tmp/managemovie-start.log 2>&1 </dev/null &
```

Danach läuft die App unter:
- `https://<Mac-IP>:8126/`

## Wichtige Regeln
- Port `443` bleibt frei
- Die App lauscht nur auf `8126`
- API-Keys werden nicht im Repo gespeichert
- Projekt, `.env.local`, Zertifikate, Daten und Datenbank liegen lokal unter `/Applications/ManageMovie`

## Update
Im geklonten Repo:
```bash
cd ManageMovie-MAC
./update_ManageMovie.sh
```

## Vollstaendige Entfernung
```bash
cd ManageMovie-MAC
./uninstall_ManageMovie.sh
```
Das Skript entfernt App, Daten, Settings, API-Keys, lokale MariaDB-Struktur, Autostart und die von ManageMovie installierten Brew-Komponenten (`mariadb`, `ffmpeg`).

## Wichtige Dateien
- Start: `start.sh`
- Stop: `stop.sh`
- Uninstall: `uninstall_ManageMovie.sh`
- Mac-App: `install_mamo_mac_app.sh`
- HTTPS: `setup_https.sh`
- MariaDB: `setup_mariadb.sh`
- User-Autostart: `install_launchagent_service.sh`
- Boot-Autostart: `install_launchdaemon_service.sh`
- Web-App: `managemovie-web/web/app.py`
