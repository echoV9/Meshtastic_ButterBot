#!/usr/bin/env python3

import logging
import sys
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path

import meshtastic
import meshtastic.serial_interface


# ==================================================
# Configuration
# ==================================================

TIMEZONE = "America/Chicago"

# Set to None for automatic USB device detection.
DEVICE_PORT = None

# Log files.
LOG_FILE = "/home/fox/git/meshtastic/meshtastic_scheduled.log"

# Record the last successful scheduled transmission.
STATE_FILE = "/home/fox/git/meshtastic/meshtastic_last_sent.txt"

# Only these hours are permitted.
ALLOWED_HOURS = (0, 8, 20)

# Only transmit during the scheduled minute.
ALLOWED_MINUTE = 0


# ==================================================
# Logging
# ==================================================

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)


# ==================================================
# Check Sending Schedule
# ==================================================

def check_schedule():
    """
    Verify that execution occurs at midnight, 8 AM, or 8 PM
    Central Time.

    Prevent duplicate transmissions within the same minute.
    """

    now = datetime.now(ZoneInfo(TIMEZONE))

    if now.hour not in ALLOWED_HOURS or now.minute != ALLOWED_MINUTE:
        logging.info(
            "Transmission skipped. Current Central Time: %s",
            now.strftime("%Y-%m-%d %I:%M %p"),
        )
        return None

    scheduled_time = now.strftime("%Y-%m-%d %H:%M")

    state_path = Path(STATE_FILE)

    if state_path.exists():
        last_sent = state_path.read_text().strip()

        if last_sent == scheduled_time:
            logging.info(
                "Duplicate transmission prevented for %s",
                scheduled_time,
            )
            return None

    return scheduled_time


# ==================================================
# Generate the Message
# ==================================================

def generate_message():
    """Generate a friendly message using current Central Time."""

    now = datetime.now(ZoneInfo(TIMEZONE))

    current_date = now.strftime("%B %d, %Y").replace(" 0", " ")
    current_time = now.strftime("%I:%M %p").lstrip("0")

    if now.hour == 8:
        greeting = "Good morning!"
    elif now.hour == 20:
        greeting = "Good evening!"
    else:
        greeting = "Good night!"

    return (
        f"Greetings! Today is {current_date}, "
        f"and the time is now {current_time}. "
        f"{greeting}"
    )


# ==================================================
# Send the Meshtastic Message
# ==================================================

def send_text_message(scheduled_time):
    """Connect, send the message, and close the connection."""

    interface = None

    try:
        logging.info("Connecting to Meshtastic device via USB...")

        if DEVICE_PORT:
            interface = meshtastic.serial_interface.SerialInterface(
                devPath=DEVICE_PORT
            )
        else:
            interface = meshtastic.serial_interface.SerialInterface()

        message = generate_message()

        logging.info("Sending Meshtastic message: %s", message)
        print(f"Sending message: '{message}'")

        interface.sendText(message)

        logging.info("Message submitted to Meshtastic interface.")
        print("Message submitted successfully.")

        # Record the scheduled transmission after submission.
        Path(STATE_FILE).write_text(scheduled_time + "\n")

    except Exception:
        logging.exception("Failed to send Meshtastic message.")
        print("ERROR: Failed to send Meshtastic message. Check the log.")
        raise

    finally:
        if interface is not None:
            try:
                interface.close()
                logging.info("Meshtastic interface closed.")
                print("Meshtastic interface closed.")
            except Exception:
                logging.exception("Error closing Meshtastic interface.")


# ==================================================
# Main
# ==================================================

if __name__ == "__main__":

    try:
        scheduled_time = check_schedule()

        if scheduled_time is None:
            sys.exit(0)

        send_text_message(scheduled_time)

    except Exception:
        sys.exit(1)

