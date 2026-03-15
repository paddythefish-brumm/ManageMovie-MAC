Version: 0.2.33

# Lastenheft ManageMovie MAC

## 1. Zielbild
ManageMovie MAC ist eine eigenständige HTTPS-Web-Anwendung für macOS. Installation, Update und Autostart erfolgen direkt aus einem öffentlichen GitHub-Repo.

## 2. In Scope
- Web-UI für Analyse, Copy und Encode
- HTTPS auf Port `8126`
- Lokale MariaDB
- Benutzer- und Boot-Autostart auf macOS
- Update direkt aus dem Mac-Repo

## 3. Funktionale Anforderungen
- Ein Git-Aufruf auf einem fremden Mac installiert die Anwendung.
- Nach Setup und Boot-Installer startet die App automatisch neu.
- Updates werden über `update_ManageMovie.sh` aus dem Repo eingespielt.
- Eine Checkbox `Beim Booten starten` steuert den Boot-Autostart.
- Auf macOS ist `Beim Booten starten` nach der Installation standardmäßig deaktiviert.
- `install_mamo_mac_app.sh` legt `/Applications/ManageMovie.app` für `Start`, `Stop` und `Open` an.
- `uninstall_ManageMovie.sh` entfernt App, Autostart, Root-Helfer, Daten, lokale DB-Inhalte, API-Keys, Settings und Projekt restlos.

## 4. Nicht-funktionale Anforderungen
- Port `443` bleibt frei
- Port `8126` ist der einzige externe HTTPS-Port
- Keine Secrets im Repo
