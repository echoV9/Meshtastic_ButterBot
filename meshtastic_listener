[Unit]
Description=Meshtastic message listener and scheduled feedback
After=network.target

[Service]
Type=simple
User=fox
WorkingDirectory=/home/fox/git/meshtastic
ExecStart=/home/fox/git/meshtastic/.venv/bin/python -u /home/fox/git/meshtastic/meshtastic_listener.py
Restart=on-failure
RestartSec=15
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
