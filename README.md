# ManageMovie MAC 0.2.20

ManageMovie MAC ist das eigenständige öffentliche Installations-Repository für den Betrieb auf einem fremden Mac. Dieses Repo enthält nur die Dateien, die für Setup, HTTPS, MariaDB, Update und Autostart auf macOS nötig sind.

## Zielplattform
- macOS auf Apple Silicon oder Intel
- HTTPS-Web-UI auf Port `8126`
- Lokale MariaDB auf dem Mac

## Installation auf einem fremden Mac
```bash
git clone https://github.com/paddythefish-brumm/ManageMovie-MAC.git && cd ManageMovie-MAC && ./setup.sh && ./setup_https.sh && ./setup_mariadb.sh && ./install_launchdaemon_service.sh && ./start.sh
```

Danach läuft die App unter:
- `https://<Mac-IP>:8126/`

## Wichtige Regeln
- Port `443` bleibt frei
- Die App lauscht nur auf `8126`
- API-Keys werden nicht im Repo gespeichert
- `.env.local`, Zertifikate, Daten und Datenbank bleiben lokal auf dem Ziel-Mac

## Update
Im geklonten Repo:
```bash
cd ManageMovie-MAC
./update_ManageMovie.sh
```

## Wichtige Dateien
- Start: `start.sh`
- Stop: `stop.sh`
- HTTPS: `setup_https.sh`
- MariaDB: `setup_mariadb.sh`
- User-Autostart: `install_launchagent_service.sh`
- Boot-Autostart: `install_launchdaemon_service.sh`
- Web-App: `managemovie-web/web/app.py`
