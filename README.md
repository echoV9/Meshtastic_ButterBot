<img width="1199" height="1312" alt="image" src="https://github.com/user-attachments/assets/ac5f9742-f8e1-461f-a1e1-f6f65018aa63" />

# ButterBot
# Mesthtastic Listener & Scheduled Feedback

A Python service that listens for public Meshtastic text messages and periodically broadcasts a greeting, the local time, and a list of recently heard devices.

## Purpose

New mesh users often send a message without knowing whether another node received it. Range tests and diagnostic tools can answer that question, but they require additional knowledge and configuration.

This project provides a simple feedback mechanism: a listening node records public text messages and includes their senders in scheduled reports. Users can look for their device name and node ID to confirm that their message reached the listening application, either directly or through mesh relays.

The service provides periodic reception feedback. It does not immediately reply to every message, prove a direct radio link, measure range, or confirm delivery throughout the mesh.

## Features

- Continuously listens over a USB serial connection.
- Records nonempty public texts on a configured channel.
- Broadcasts at midnight, noon, and 8 PM Central Time by default.
- Lists each qualifying sender once, with its latest observed text receipt time.
- Uses a rolling reporting window, set to 24 hours by default.
- Keeps observations and broadcast-attempt records in SQLite across restarts.
- Suppresses repeated observations of known sender/packet-ID pairs.
- Excludes private messages, the local node's messages, and packets flagged as arriving through MQTT.
- Splits long reports into byte-limited pages and spaces transmissions apart.
- Rotates application logs and prevents multiple instances of this script from running concurrently.

## Example broadcast

The greeting and report are separate messages, submitted 15 seconds apart by default:

```text
Greetings! Today is November 3, 2026, and the time is now 12:00 PM CST. Good afternoon!
```

```text
Public texts received (past 24h; latest per device):
- Meshtastic ef4c (!db51ef4c) @ 11/03 11:32 AM CST
```

## How it works

1. Connect to a Meshtastic radio over USB and subscribe to text-receive events.
2. Accept nonempty broadcast texts on the configured channel, excluding the local sender and MQTT-flagged packets.
3. Queue the sender's identity, the host's observation time, and the packet ID. Message contents are not persisted by this application.
4. Save observations in SQLite, retaining one latest-observation row per node ID.
5. At a scheduled time, generate a greeting and a snapshot of eligible devices, newest first.
6. Reserve that scheduled broadcast slot, submit the messages, and record completion of submission.

### What “past 24h; latest per device” means

`REPORT_HOURS = 24` filters device records by their latest qualifying text observation. It is a rolling window evaluated when the report is generated, rather than a period starting after the previous broadcast.

Each node ID has one device record. A newer distinct text updates its timestamp and recorded name. Multiple texts from the same sender produce one report entry. Duplicate sender/packet-ID pairs do not refresh that timestamp while they remain in the duplicate-tracking table.

For a report generated at noon on November 3, an observation from November 3 at 11:32 AM qualifies. With no further messages, that observation also appears in the 8 PM and midnight reports, then drops out of the November 4 noon report.

Reporting does not clear records. Records older than the reporting window remain in SQLite but are omitted from the report. Duplicate-packet tracking records are pruned after approximately seven days.

**Time handling:** receipt timestamps come from the host computer when the application observes a packet. Output uses `America/Chicago`, selecting CST or CDT automatically. The current cutoff calculation subtracts 24 local clock hours; across a daylight-saving change, that can represent 23 or 25 elapsed hours.

## Requirements

- Linux with systemd for the service deployment described below.
- Python 3.9 or later, with the `venv` module and IANA timezone data available.
- A USB-connected Meshtastic radio with a working channel configuration.
- Serial-device and data-directory access for the service account.
- Python packages: `meshtastic` and `pypubsub`.

The script uses Linux/Unix `fcntl` locking. These service instructions target Linux, not Windows.

## Installation

The examples use account `fox` and directory `/home/fox/git/meshtastic`. Replace these values consistently for your system, including the script's `BASE_DIR` setting.

### 1. Place the script and install dependencies

Save `meshtastic_listener.py` in the project directory. Run the following as the intended service account:

```bash
mkdir -p /home/fox/git/meshtastic
cd /home/fox/git/meshtastic
python3 -m venv .venv
.venv/bin/python -m pip install meshtastic pypubsub
.venv/bin/python -m py_compile meshtastic_listener.py
```

A successful syntax check prints no output. It does not verify USB access or radio operation.

### 2. Review configuration

Edit the configuration section near the top of the Python script:

| Setting | Default | Meaning |
| --- | --- | --- |
| `TIMEZONE` | `ZoneInfo("America/Chicago")` | Schedule and displayed receipt timezone. |
| `DEVICE_PORT` | `None` | Automatically detect the USB radio; alternatively specify its serial path. |
| `CHANNEL_INDEX` | `0` | Local channel index used for listening and broadcasting. Confirm which index is your general channel. |
| `ALLOWED_HOURS` | `(0, 12, 20)` | Midnight, noon, and 8 PM in the configured timezone. |
| `ALLOWED_MINUTE` | `0` | Scheduled minute within each allowed hour. |
| `BASE_DIR` | `/home/fox/git/meshtastic` | Location of the database, log, and lock file. |
| `REPORT_HOURS` | `24` | Reporting window; `None` includes all retained device records. |
| `MAX_TEXT_BYTES` | `220` | Application byte budget for each report page. |
| `PAGE_DELAY_SECONDS` | `15` | Delay between outgoing messages. |
| `EXCLUDE_MQTT` | `True` | Ignore packets carrying the `viaMqtt` flag. |

For unattended use, consider a persistent serial path returned by:

```bash
ls -l /dev/serial/by-id/
```

For example, assign the actual path to `DEVICE_PORT`:

```python
DEVICE_PORT = "/dev/serial/by-id/YOUR_ACTUAL_DEVICE_ID"
```

If you change `ALLOWED_HOURS`, update the hour-to-greeting mapping in `greeting()` as well. Adding an unmapped scheduled hour causes a `KeyError` when that report is generated.

### 3. Create the systemd service

Create `/etc/systemd/system/meshtastic-listener.service`:

```ini
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
```

Ensure `fox` can write to `BASE_DIR` and access the radio's serial device. Check the device's ownership and group before changing permissions; group names vary by distribution.

### 4. Disable the previous cron sender

If replacing an earlier cron-based sender, remove or comment out its transmission entry:

```bash
crontab -e
```

The listener owns the serial connection continuously. Another sender or serial client can conflict with it. The script's lock prevents another instance of this listener, but does not coordinate with unrelated programs.

### 5. Start the service

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now meshtastic-listener.service
```

Wait at least 20 seconds before checking that startup remains successful:

```bash
sudo systemctl status meshtastic-listener.service --no-pager -l
tail -n 30 /home/fox/git/meshtastic/meshtastic_scheduled.log
```

Look for `active (running)` and a recent `Listener started on channel 0` entry. An immediate status check can precede a startup failure.

## Verify operation

### Reception

Watch the log:

```bash
tail -f /home/fox/git/meshtastic/meshtastic_scheduled.log
```

Send a public text from another node on the configured channel. A qualifying, nonduplicate observation should produce:

```text
Received public text from DEVICE_NAME (!xxxxxxxx)
```

Press Ctrl+C to stop watching the log; this does not stop the service.

### Scheduled transmission

Leave the listener running through the next configured broadcast minute. Look for:

```text
Submitted broadcast: Greetings! ...
Submitted broadcast: Public texts received ...
Scheduled report submitted for YYYY-MM-DD HH:MM
```

Confirm the messages arrive on another node. Successful API submission alone does not confirm transmission or remote reception.

## Operations

```bash
# Start, stop, or restart the listener.
sudo systemctl start meshtastic-listener.service
sudo systemctl stop meshtastic-listener.service
sudo systemctl restart meshtastic-listener.service

# Inspect service diagnostics and application logs.
sudo journalctl -u meshtastic-listener.service -n 100 --no-pager -o cat
tail -n 50 /home/fox/git/meshtastic/meshtastic_scheduled.log
```

After editing the Python file, check syntax and restart the service. Editing the service definition additionally requires `sudo systemctl daemon-reload`.

Replace the script as a complete file when upgrading. Merging code from an older cron sender can leave obsolete functions, undefined variables, and indentation errors.

## Stored data

| File | Purpose |
| --- | --- |
| `meshtastic_received.sqlite3` | Device observations, recent packet IDs, and scheduled broadcast attempts. |
| `meshtastic_scheduled.log` | Connection events, received-sender records, submission events, and exceptions. |
| `meshtastic_scheduled.log.1` through `.3` | Rotated logs; the active log rotates near 2 MB. |
| `meshtastic_listener.lock` | Advisory process lock; the file may remain after shutdown. |

The database contains three tables:

- `devices`: node ID, recorded name, and latest qualifying observation time.
- `seen_packets`: sender/packet-ID pairs and observation times for duplicate suppression.
- `broadcasts`: scheduled slot and submission status (`started` or `submitted`).

Device and broadcast records persist without automatic age-based deletion. The SQLite device table does not retain a complete message history. Logs may retain earlier observation events until rotated out.

Names come from the radio's node database when the text is observed: long name first, short name second, and node ID as a fallback. A later name announcement alone does not update the stored report name. Names are cleaned and may be shortened to fit a report page.

## Scheduling and delivery behavior

- The loop checks for scheduled work approximately once per second.
- A report starts only during the configured minute; missed slots are not caught up later.
- Each slot is reserved in SQLite before sending begins.
- A crash or shutdown during a report leaves the slot reserved. It is deliberately not retried automatically, even if some or all pages were not submitted.
- The report is generated before the greeting is sent. Messages arriving during page transmission are queued for subsequent processing and are not added to that report snapshot.
- Long reports produce multiple pages, each with the same header. More devices mean more transmissions and a longer report sequence.
- Broadcasts request no individual delivery acknowledgment. `submitted` means all pages were handed to the Meshtastic interface, not that remote nodes received them.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Service repeatedly restarts | Read the recent journal and application traceback. Stop the service while correcting the cause. |
| `SyntaxError` or `IndentationError` | Run `py_compile`; replace mixed or damaged code with the complete intended script. |
| `NameError` referencing `state_path` or `scheduled_time` | Obsolete cron code remains. Install the complete listener file rather than merging versions. |
| Missing `meshtastic` or `pubsub` import | Install dependencies using the exact virtual-environment Python in `ExecStart`. |
| Serial permission denied | Check device ownership, group membership, and service-account access. |
| Radio not found or port busy | Check USB detection, `DEVICE_PORT`, and other serial applications or cron jobs. |
| Another listener instance is running | Stop the existing listener process. A leftover lock file alone does not mean the lock is held. |
| No received-device entries | Send a nonempty broadcast text from a different node on the configured channel; confirm it is not MQTT-flagged. |
| No report at startup | Startup does not trigger a report. Wait for a configured broadcast minute. |
| Report lacks a recent message | It may fall outside the snapshot, reporting window, or filters, or have been classified as a duplicate. |
| Old errors appear in the log | Compare error timestamps with the current service start time and newest entries. |

To run interactively, stop the service first, then use the service account and environment:

```bash
sudo systemctl stop meshtastic-listener.service
sudo -u fox /home/fox/git/meshtastic/.venv/bin/python \
  -u /home/fox/git/meshtastic/meshtastic_listener.py
```

Stop the interactive process with Ctrl+C before restarting the service.

## Limitations and interpretation

- The report confirms an observation by this listening application; it does not prove direct RF connectivity, distance, signal quality, or delivery to other nodes.
- Observations can include relayed traffic. MQTT exclusion depends on the packet flag, not independent route verification.
- The displayed window does not establish continuous monitoring. The application cannot reconstruct an entire preceding day merely because it has just started.
- Texts received while the application is offline may be missing. Buffered packets supplied after reconnecting are timestamped when the application observes them.
- Accurate host time is necessary for reliable scheduling and receipt timestamps.
- Reported device names and node IDs are identifiers supplied through the mesh, not verified human identities.
- The application does not persist message text, but it stores sender metadata and broadcasts those identifiers and receipt times on the configured channel. Consider that visibility when operating it.
- This project does not reconfigure radio channels, encryption, modem settings, or regulatory parameters.

## Project status

The current script has passed syntax checks and simulated checks for filtering, database updates, duplicate suppression, report sizing, and scheduled-slot deduplication. A deployed instance has logged public-text reception and successful submission of a scheduled greeting and report. Remote reception and behavior on other hardware or library versions should be verified in each deployment.

## License

MIT License

Copyright (c) 2026 echo

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NON-INFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
