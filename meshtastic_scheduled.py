#!/usr/bin/env python3

"""
Continuously listen for public Meshtastic text messages and broadcast
scheduled greetings with a summary of recently heard devices.

Default broadcast schedule:
    - Midnight
    - Noon
    - 8 PM

All scheduled times and reported receipt times use America/Chicago,
which automatically accounts for CST/CDT daylight-saving changes.

Run this script continuously, preferably through systemd. A cron job
that starts only at broadcast time cannot reliably observe messages
received between broadcasts.

The SQLite database preserves:
    - Each device's latest observed public text message time.
    - Recently seen packet IDs, to suppress duplicate observations.
    - Scheduled broadcast attempts, to prevent repeated reports.

The script records sender names and receipt times, not message contents.
"""

import fcntl
import logging
from logging.handlers import RotatingFileHandler
import queue
import signal
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import meshtastic
import meshtastic.serial_interface
from pubsub import pub


# ==================================================
# Configuration
# ==================================================

# Use an IANA timezone rather than a fixed UTC offset.
# America/Chicago automatically selects CST or CDT for each date.
TIMEZONE = ZoneInfo("America/Chicago")

# USB serial device connected to the Meshtastic radio.
#
# None asks the Meshtastic library to detect the device automatically.
# For unattended operation, a persistent /dev/serial/by-id/... path
# helps avoid selecting a different device after a reboot.
DEVICE_PORT = None

# Local channel index used for general/public messages.
#
# Index 0 is commonly the primary channel, but confirm your radio's
# configuration. This is a channel index, not a LoRa frequency slot
# or modem preset.
CHANNEL_INDEX = 0

# Hours are expressed using a 24-hour clock in TIMEZONE.
# These values mean midnight, noon, and 8 PM.
ALLOWED_HOURS = (0, 12, 20)

# Broadcast only during minute 00 of an allowed hour.
#
# The main loop checks approximately once per second. A stored
# broadcast record prevents repeated sends during that minute.
# Missed scheduled times are not automatically caught up later.
ALLOWED_MINUTE = 0

# Directory holding the log, database, and process lock.
# The account running this script must have write access.
BASE_DIR = Path("/home/fox/git/meshtastic")

# SQLite stores observations and scheduled broadcast attempts.
DATABASE_FILE = BASE_DIR / "meshtastic_received.sqlite3"

# Application log, automatically rotated to limit disk usage.
LOG_FILE = BASE_DIR / "meshtastic_scheduled.log"

# Advisory lock file used to prevent multiple copies of this script
# from attempting to use the same USB radio.
LOCK_FILE = BASE_DIR / "meshtastic_listener.lock"

# Include devices whose latest observed public text falls within
# this many hours of the scheduled report.
#
# Each device appears only once, using its latest receipt time.
# Set to None to report every device stored since logging began.
#
# This setting filters report output; older device records remain
# in the database.
REPORT_HOURS = 24

# Maximum UTF-8 byte length targeted for each report page.
#
# Meshtastic messages have a limited payload size. Measuring encoded
# bytes matters because some Unicode characters use multiple bytes.
# This conservative limit leaves room below the usual text limit.
MAX_TEXT_BYTES = 220

# Pause between the greeting and report pages, and between subsequent
# pages, rather than submitting the entire report in one burst.
#
# This is a fixed pacing interval, not a radio airtime calculation.
PAGE_DELAY_SECONDS = 15

# Ignore packets explicitly marked as having arrived through MQTT.
#
# This relies on the packet's viaMqtt flag; it does not independently
# verify the complete path taken by a message.
EXCLUDE_MQTT = True

# Thread-safe signal requesting a clean application shutdown.
STOP = threading.Event()

# Set when the Meshtastic library reports a lost connection.
# The main loop then exits with an error so systemd can restart it.
DISCONNECTED = threading.Event()

# Transfer observations from the library's receive callback to the
# main thread. Keeping database work in the main thread avoids
# sharing a SQLite connection between threads.
RECEIVED = queue.Queue()


# ==================================================
# Receive callbacks
# ==================================================

def on_receive(packet, interface):
    """
    Filter an incoming text packet and queue its sender information.

    The Meshtastic library invokes this callback asynchronously.
    Keep it short so database operations or radio transmissions do
    not block the library's receive processing.
    """

    # Numeric Meshtastic node number identifying the original sender.
    sender = packet.get("from")

    # Obtain this radio's own node number so its messages are excluded.
    # getattr provides a safe fallback while device information is
    # unavailable or still being initialized.
    local_info = getattr(interface, "myInfo", None)
    local_number = getattr(local_info, "my_node_num", None)

    if sender is None or sender == local_number:
        return

    # Accept broadcasts only. Direct messages to this node or another
    # specific destination must not appear in the public report.
    if packet.get("to") != meshtastic.BROADCAST_NUM:
        return

    # Only observe the configured general/public channel.
    # An omitted channel field represents the default index, 0.
    if packet.get("channel", 0) != CHANNEL_INDEX:
        return

    # Optionally exclude messages identified as MQTT-originated.
    if EXCLUDE_MQTT and packet.get("viaMqtt", False):
        return

    # Although subscribed to text events, verify that a nonempty
    # decoded text value is actually present.
    text = packet.get("decoded", {}).get("text")
    if not isinstance(text, str) or not text.strip():
        return

    # Look up the sender's advertised name in the library's node
    # database. The sender may be known before its name is available.
    node = (getattr(interface, "nodesByNum", None) or {}).get(sender, {})
    user = node.get("user", {})

    # Prefer the descriptive long name, then the short name.
    name = user.get("longName") or user.get("shortName")

    # Format the numeric node number as a conventional Meshtastic ID.
    # This also distinguishes devices advertising identical names.
    node_id = f"!{sender:08x}"

    # Timestamp the observation using the host computer's clock.
    # This avoids depending on the remote sender's clock.
    #
    # Receipt time means when this application observed the packet;
    # buffered packets delivered after reconnecting may therefore
    # have a later observation time than their original radio receipt.
    #
    # Store only identity, time, and packet ID—not message contents.
    RECEIVED.put((
        node_id,
        name or node_id,
        time.time(),
        packet.get("id"),
    ))


def on_disconnect(interface):
    """
    Notify the main loop that the radio connection was lost.

    The interface argument is supplied by the Meshtastic event system.
    No connection recovery is attempted inside this callback.
    """
    DISCONNECTED.set()


# ==================================================
# Persistent records
# ==================================================

def open_database():
    """
    Open the SQLite database and create missing tables.

    Existing records are retained across script or host restarts.
    This connection is used only by the main thread.
    """
    db = sqlite3.connect(DATABASE_FILE)

    db.executescript("""
        -- One row per sender, retaining its latest observation.
        CREATE TABLE IF NOT EXISTS devices (
            node_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            last_received REAL NOT NULL
        );

        -- Recent sender/packet-ID pairs suppress duplicate packets.
        -- The sender is part of the key because different devices
        -- can generate the same packet ID.
        CREATE TABLE IF NOT EXISTS seen_packets (
            node_id TEXT NOT NULL,
            packet_id INTEGER NOT NULL,
            received REAL NOT NULL,
            PRIMARY KEY (node_id, packet_id)
        );

        -- One row per scheduled broadcast time.
        -- "started" reserves the slot before any send is attempted.
        -- "submitted" means every page was handed to the API.
        -- Neither status confirms reception by another mesh node.
        CREATE TABLE IF NOT EXISTS broadcasts (
            slot TEXT PRIMARY KEY,
            status TEXT NOT NULL
        );
    """)

    db.commit()
    return db


def record_received(db):
    """
    Drain queued observations into persistent storage.

    Duplicate packets do not advance the device's latest receipt
    time. A newer, distinct text message updates its device record.
    """
    while True:
        try:
            # Nonblocking read: return to the main loop when no more
            # observations are waiting.
            node_id, name, received, packet_id = RECEIVED.get_nowait()
        except queue.Empty:
            break

        # Each observation is handled in a transaction. The context
        # manager commits on success and rolls back on an exception.
        with db:
            if packet_id:
                # INSERT OR IGNORE leaves an existing sender/packet-ID
                # pair unchanged when that packet is observed again.
                result = db.execute(
                    """
                    INSERT OR IGNORE INTO seen_packets
                    VALUES (?, ?, ?)
                    """,
                    (node_id, packet_id, received),
                )

                # No inserted row means this packet was already seen.
                if result.rowcount == 0:
                    continue

            # Add a newly heard device or update an existing device.
            # The WHERE clause prevents an older queued observation
            # from replacing a newer receipt time.
            #
            # SQL parameters keep names and IDs out of SQL syntax.
            db.execute(
                """
                INSERT INTO devices VALUES (?, ?, ?)
                ON CONFLICT(node_id) DO UPDATE SET
                    name = excluded.name,
                    last_received = excluded.last_received
                WHERE excluded.last_received > devices.last_received
                """,
                (node_id, name, received),
            )

        logging.info(
            "Received public text from %s (%s)",
            name,
            node_id,
        )


# ==================================================
# Message formatting
# ==================================================

def clean_name(name):
    """
    Make an advertised device name suitable for a report line.

    Remove nonprintable characters, collapse whitespace, and limit
    the name's length. This helps prevent line breaks or control
    characters from disrupting the report's formatting.
    """
    cleaned = "".join(
        character for character in name
        if character.isprintable()
    )

    return " ".join(cleaned.split())[:32] or "Unnamed"


def greeting(now):
    """Build the scheduled greeting using Central date and time."""

    # Remove the leading zero from single-digit calendar dates.
    date = now.strftime("%B %d, %Y").replace(" 0", " ")

    # %Z supplies the appropriate CST/CDT abbreviation.
    clock = now.strftime("%I:%M %p %Z").lstrip("0")

    # This function is called only during the configured hours.
    salutation = {
        0: "Good night!",
        12: "Good afternoon!",
        20: "Good evening!",
    }[now.hour]

    return (
        f"Greetings! Today is {date}, "
        f"and the time is now {clock}. {salutation}"
    )


def report_messages(db, now):
    """
    Build one or more report pages from stored observations.

    Each device is listed once. Results are ordered with the most
    recently heard device first. Entries are kept intact when
    splitting the report into byte-limited pages.
    """
    if REPORT_HOURS is None:
        # No time filter: include every stored device.
        rows = db.execute(
            "SELECT node_id, name, last_received FROM devices "
            "ORDER BY last_received DESC"
        ).fetchall()

        period = "since logging began"

    else:
        # Convert the start of the reporting window to a Unix
        # timestamp, matching the database's receipt-time format.
        cutoff = (now - timedelta(hours=REPORT_HOURS)).timestamp()

        rows = db.execute(
            "SELECT node_id, name, last_received FROM devices "
            "WHERE last_received >= ? ORDER BY last_received DESC",
            (cutoff,),
        ).fetchall()

        period = f"past {REPORT_HOURS}h"

    # Explicitly report an empty observation window.
    if not rows:
        return [
            f"No public text messages recorded by this node ({period})."
        ]

    # Repeat the header on every page so pages remain understandable
    # even if a recipient misses an earlier transmission.
    header = (
        f"Public texts received ({period}; latest per device):\n"
    )

    pages = []
    page = header

    for node_id, name, received in rows:
        # Convert the stored timestamp to Central time for display.
        observed = datetime.fromtimestamp(received, TIMEZONE)

        # Include the date because a rolling reporting window can
        # span multiple calendar days.
        timestamp = observed.strftime("%m/%d %I:%M %p %Z")

        entry = (
            f"- {clean_name(name)} ({node_id}) @ {timestamp}\n"
        )

        # Measure encoded bytes, not Python character count.
        # Start another page before adding an entry that would
        # exceed the configured page limit.
        if len((page + entry).encode("utf-8")) > MAX_TEXT_BYTES:
            pages.append(page.rstrip())
            page = header

        page += entry

    # Preserve the final page after processing all devices.
    pages.append(page.rstrip())
    return pages


# ==================================================
# Scheduled transmission
# ==================================================

def send_if_due(db, interface):
    """
    Submit a greeting and report when a scheduled time is reached.

    A scheduled slot is reserved before sending. If the process
    stops halfway through, that slot is not automatically retried.
    This favors avoiding duplicate public broadcasts over retrying
    an incomplete report.
    """
    now = datetime.now(TIMEZONE)

    # Return immediately outside a permitted broadcast minute.
    if (
        now.hour not in ALLOWED_HOURS
        or now.minute != ALLOWED_MINUTE
    ):
        return

    # The local date and scheduled minute identify this broadcast.
    slot = now.strftime("%Y-%m-%d %H:%M")

    # A reserved or completed slot must not be sent again.
    if db.execute(
        "SELECT 1 FROM broadcasts WHERE slot = ?",
        (slot,),
    ).fetchone():
        return

    # Generate a snapshot of the report at this scheduled time.
    # New observations arriving during transmission are queued for
    # storage afterward and are not added to this snapshot.
    messages = [greeting(now), *report_messages(db, now)]

    # Persist the reservation before submitting the first message.
    with db:
        db.execute(
            "INSERT INTO broadcasts VALUES (?, 'started')",
            (slot,),
        )

    for index, message in enumerate(messages):
        # Send the first message immediately. Wait before subsequent
        # pages, while allowing a shutdown request to interrupt.
        if index and STOP.wait(PAGE_DELAY_SECONDS):
            return

        if STOP.is_set():
            return

        # Exit with an error if the library reported a lost link.
        # systemd can then restart the application.
        if DISCONNECTED.is_set():
            raise ConnectionError(
                "Meshtastic device disconnected"
            )

        # Explicitly broadcast on the configured public channel.
        # No individual destination acknowledgment is requested.
        #
        # A successful return means submission to the interface;
        # it does not prove over-the-air delivery or reception.
        interface.sendText(
            message,
            destinationId=meshtastic.BROADCAST_ADDR,
            channelIndex=CHANNEL_INDEX,
            wantAck=False,
        )

        logging.info("Submitted broadcast: %s", message)

    # Mark complete only after every message was submitted.
    with db:
        db.execute(
            """
            UPDATE broadcasts
            SET status = 'submitted'
            WHERE slot = ?
            """,
            (slot,),
        )

    logging.info(
        "Scheduled report submitted for %s",
        slot,
    )


# ==================================================
# Main
# ==================================================

def main():
    """Initialize resources, listen continuously, and shut down cleanly."""

    # Create the data directory if it does not already exist.
    BASE_DIR.mkdir(parents=True, exist_ok=True)

    # Keep the active log near 2 MB and retain three rotated copies.
    # Log timestamps use the host's configured local timezone;
    # broadcast timestamps explicitly use TIMEZONE.
    handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=2_000_000,
        backupCount=3,
    )

    logging.basicConfig(
        handlers=[handler],
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
    )

    # Hold this file open for the entire application lifetime.
    # Linux releases the advisory lock when the descriptor closes
    # or the process terminates, even after an unexpected exit.
    lock_handle = LOCK_FILE.open("a")

    try:
        fcntl.flock(
            lock_handle.fileno(),
            fcntl.LOCK_EX | fcntl.LOCK_NB,
        )
    except BlockingIOError:
        raise RuntimeError(
            "Another listener instance is running"
        )

    # SIGTERM is normally used by systemd to stop the service.
    # SIGINT handles Ctrl+C during an interactive run.
    # Set a flag rather than performing cleanup inside the handler.
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: STOP.set())

    db = open_database()
    interface = None

    # Register callbacks before opening the connection so events
    # emitted during connection setup can also be observed.
    pub.subscribe(
        on_receive,
        "meshtastic.receive.text",
    )
    pub.subscribe(
        on_disconnect,
        "meshtastic.connection.lost",
    )

    try:
        logging.info("Connecting to Meshtastic device")

        interface = meshtastic.serial_interface.SerialInterface(
            devPath=DEVICE_PORT
        )

        logging.info(
            "Listener started on channel %s",
            CHANNEL_INDEX,
        )

        while not STOP.is_set():
            # Persist observations before generating any due report.
            record_received(db)

            if DISCONNECTED.is_set():
                raise ConnectionError(
                    "Meshtastic device disconnected"
                )

            # Usually returns immediately; sends only when due.
            send_if_due(db, interface)

            # Retain duplicate-packet history for seven days.
            # This does not delete the separate device records.
            with db:
                db.execute(
                    """
                    DELETE FROM seen_packets
                    WHERE received < ?
                    """,
                    (time.time() - 7 * 86400,),
                )

            # Wait approximately one second between checks.
            # Event.wait returns early when shutdown is requested.
            STOP.wait(1)

    finally:
        # Cleanup runs after normal shutdown and after exceptions.
        # Stop accepting new callback observations first.
        pub.unsubscribe(
            on_receive,
            "meshtastic.receive.text",
        )
        pub.unsubscribe(
            on_disconnect,
            "meshtastic.connection.lost",
        )

        # Close the radio connection if initialization succeeded.
        # Log a close failure without skipping subsequent cleanup.
        if interface is not None:
            try:
                interface.close()
            except Exception:
                logging.exception(
                    "Error closing interface"
                )

        # Save any observations still waiting in the queue.
        record_received(db)

        db.close()

        # Closing the descriptor also releases the process lock.
        lock_handle.close()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        # Preserve the traceback and exit unsuccessfully.
        # A systemd service configured with Restart=on-failure
        # will restart the listener after this failure.
        logging.exception(
            "Listener stopped unexpectedly"
        )
        raise
