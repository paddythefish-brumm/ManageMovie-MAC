Version: 0.2.36

# ManageMovie MAC Flowchart

```mermaid
flowchart TD
    A["git clone ManageMovie-MAC"] --> B["./setup.sh"]
    B --> C["./setup_https.sh"]
    C --> D["./setup_mariadb.sh"]
    D --> E["./install_launchdaemon_service.sh"]
    E --> F["./install_mamo_mac_app.sh"]
    F --> G["nohup ./start.sh &"]
    G --> H["https://Mac-IP:8126"]
```
