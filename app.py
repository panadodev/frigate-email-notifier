
import asyncio
import json
import logging
import os
import queue
import smtplib
import sqlite3
import ssl
import threading
import time
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path

import paho.mqtt.client as mqtt
import requests
from amqtt.broker import Broker
from argon2 import PasswordHasher

# ==================================================
# CONFIGURATION
# ==================================================

MQTT_PORT = int(os.getenv("MQTT_INTERNAL_PORT", "18883"))
MQTT_USERNAME = os.environ["MQTT_USERNAME"]
MQTT_PASSWORD = os.environ["MQTT_PASSWORD"]

FRIGATE_URL = os.getenv(
    "FRIGATE_URL", "http://10.0.0.5:5000"
).rstrip("/")

FRIGATE_TOKEN = os.getenv("FRIGATE_TOKEN", "")

CAMERAS = {
    x.strip()
    for x in os.getenv("CAMERAS", "").split(",")
    if x.strip()
}

SMTP_HOST = os.environ["SMTP_HOST"]
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_SECURITY = os.getenv("SMTP_SECURITY", "starttls").lower()

SMTP_USERNAME = os.environ["SMTP_USERNAME"]
SMTP_PASSWORD = os.environ["SMTP_PASSWORD"]

EMAIL_FROM = os.getenv("EMAIL_FROM", SMTP_USERNAME)

EMAIL_TO = [
    x.strip()
    for x in os.environ["EMAIL_TO"].split(",")
    if x.strip()
]

COOLDOWN = int(os.getenv("COOLDOWN_SECONDS", "60"))

DB_PATH = "/data/notifications.db"

EVENT_QUEUE = queue.Queue(maxsize=100)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

log = logging.getLogger("frigate-email")


# ==================================================
# DATABASE
# ==================================================

def initialize_database():
    with sqlite3.connect(DB_PATH) as db:
        db.execute("""
            CREATE TABLE IF NOT EXISTS sent_events (
                event_id TEXT PRIMARY KEY,
                camera TEXT NOT NULL,
                sent_at REAL NOT NULL
            )
        """)
        db.execute("""
            CREATE INDEX IF NOT EXISTS idx_camera_sent
            ON sent_events(camera, sent_at)
        """)


def was_sent(event_id):
    with sqlite3.connect(DB_PATH) as db:
        result = db.execute(
            "SELECT 1 FROM sent_events WHERE event_id = ?",
            (event_id,)
        ).fetchone()

    return result is not None


def camera_on_cooldown(camera):
    with sqlite3.connect(DB_PATH) as db:
        result = db.execute(
            """
            SELECT MAX(sent_at)
            FROM sent_events
            WHERE camera = ?
            """,
            (camera,)
        ).fetchone()[0]

    return result is not None and time.time() - result < COOLDOWN


def record_sent(event_id, camera):
    with sqlite3.connect(DB_PATH) as db:
        db.execute(
            """
            INSERT OR IGNORE INTO sent_events
            (event_id, camera, sent_at)
            VALUES (?, ?, ?)
            """,
            (event_id, camera, time.time())
        )


# ==================================================
# FRIGATE SNAPSHOT
# ==================================================

def get_snapshot(event_id):
    url = f"{FRIGATE_URL}/api/events/{event_id}/snapshot.jpg"

    headers = {}

    if FRIGATE_TOKEN:
        headers["Authorization"] = f"Bearer {FRIGATE_TOKEN}"

    for attempt in range(5):
        try:
            response = requests.get(
                url,
                headers=headers,
                timeout=15
            )

            response.raise_for_status()

            if response.headers.get(
                "Content-Type", ""
            ).startswith("image/"):
                return response.content

            log.warning("Snapshot response was not an image")

        except requests.RequestException as error:
            log.warning(
                "Snapshot attempt %s failed: %s",
                attempt + 1,
                error
            )

        time.sleep(2)

    return None


# ==================================================
# EMAIL
# ==================================================

def format_timestamp(value):
    if value is None:
        return "Unknown"

    try:
        return datetime.fromtimestamp(float(value)).astimezone().isoformat()
    except (ValueError, TypeError, OSError):
        return str(value)


def send_email(camera, event, snapshot):
    event_id = event["id"]

    message = EmailMessage()

    message["Subject"] = f"Frigate: Person detected on {camera}"
    message["From"] = EMAIL_FROM
    message["To"] = ", ".join(EMAIL_TO)

    message.set_content(f"""
Person detected by Frigate.

Camera: {camera}
Event ID: {event_id}
Start: {format_timestamp(event.get("start_time"))}
End: {format_timestamp(event.get("end_time"))}
Confidence: {event.get("top_score", "Unknown")}

The detection snapshot is attached.
""".strip())

    message.add_attachment(
        snapshot,
        maintype="image",
        subtype="jpeg",
        filename=f"{camera}-{event_id}.jpg"
    )

    context = ssl.create_default_context()

    if SMTP_SECURITY == "ssl":
        with smtplib.SMTP_SSL(
            SMTP_HOST,
            SMTP_PORT,
            context=context,
            timeout=30
        ) as server:
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.send_message(message)

    elif SMTP_SECURITY == "starttls":
        with smtplib.SMTP(
            SMTP_HOST,
            SMTP_PORT,
            timeout=30
        ) as server:
            server.ehlo()
            server.starttls(context=context)
            server.ehlo()
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.send_message(message)

    else:
        raise ValueError("SMTP_SECURITY must be ssl or starttls")

    log.info("Email sent: %s / %s", camera, event_id)


# ==================================================
# EVENT PROCESSING
# ==================================================

def process_event(event):
    camera = event.get("camera")
    event_id = event.get("id")

    if not camera or not event_id:
        return

    if camera not in CAMERAS:
        return

    if event.get("label") != "person":
        return

    if event.get("false_positive", False):
        return

    if was_sent(event_id):
        log.info("Duplicate event ignored: %s", event_id)
        return

    if camera_on_cooldown(camera):
        log.info("Camera cooldown active: %s", camera)
        return

    log.info("Person detected on %s", camera)

    snapshot = get_snapshot(event_id)

    if not snapshot:
        log.error("Snapshot unavailable: %s", event_id)
        return

    for attempt in range(3):
        try:
            send_email(camera, event, snapshot)
            record_sent(event_id, camera)
            return

        except Exception:
            log.exception(
                "Email attempt %s failed",
                attempt + 1
            )
            time.sleep(3)

    log.error("Email delivery failed: %s", event_id)


def notification_worker():
    while True:
        event = EVENT_QUEUE.get()

        try:
            process_event(event)
        except Exception:
            log.exception("Event processing failed")
        finally:
            EVENT_QUEUE.task_done()


# ==================================================
# MQTT SUBSCRIBER
# ==================================================

def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        log.info("Connected to embedded MQTT broker")
        client.subscribe("frigate/events", qos=1)
    else:
        log.error("MQTT connection failed: %s", reason_code)


def on_message(client, userdata, message):
    try:
        payload = json.loads(message.payload.decode("utf-8"))

        if payload.get("type") != "end":
            return

        event = payload.get("after", {})

        if event.get("label") != "person":
            return

        try:
            EVENT_QUEUE.put_nowait(event)
        except queue.Full:
            log.error("Notification queue full; event dropped")

    except Exception:
        log.exception("Invalid MQTT event")


# ==================================================
# BROKER
# ==================================================

def create_password_file():
    password_file = Path("/tmp/mqtt-passwords")

    hashed_password = PasswordHasher().hash(MQTT_PASSWORD)

    password_file.write_text(
        f"{MQTT_USERNAME}:{hashed_password}\n"
    )

    return str(password_file)


async def main():
    if not CAMERAS:
        raise ValueError("Configure at least one camera")

    if not EMAIL_TO:
        raise ValueError("Configure EMAIL_TO")

    if SMTP_SECURITY not in ("ssl", "starttls"):
        raise ValueError("Invalid SMTP_SECURITY")

    initialize_database()

    password_file = create_password_file()

    broker_config = {
        "listeners": {
            "default": {
                "type": "tcp",
                "bind": f"0.0.0.0:{MQTT_PORT}"
            }
        },
        "plugins": {
            "amqtt.plugins.authentication.FileAuthPlugin": {
                "password_file": password_file
            },
            "amqtt.plugins.logging_amqtt.EventLoggerPlugin": {}
        }
    }

    broker = Broker(config=broker_config)

    await broker.start()

    log.info("MQTT broker listening on port %s", MQTT_PORT)

    threading.Thread(
        target=notification_worker,
        daemon=True,
        name="notification-worker"
    ).start()

    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="frigate-email-handler"
    )

    client.username_pw_set(
        MQTT_USERNAME,
        MQTT_PASSWORD
    )

    client.on_connect = on_connect
    client.on_message = on_message

    client.reconnect_delay_set(
        min_delay=1,
        max_delay=30
    )

    client.connect("127.0.0.1", MQTT_PORT, 60)
    client.loop_start()

    log.info("Notification service ready")

    try:
        await asyncio.Event().wait()
    finally:
        client.loop_stop()
        client.disconnect()
        await broker.shutdown()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Shutting down")
