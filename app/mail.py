"""Outbound email. ``SMTPMailer`` talks to any SMTP server (Mailpit in docker-compose);
``MemoryMailer`` records messages for tests and can be told to fail N times to exercise the
retry path."""

import smtplib
from dataclasses import dataclass, field
from email.message import EmailMessage
from pathlib import Path

from app.config import get_settings


@dataclass
class Mail:
    to: str
    subject: str
    body: str
    attachments: list[Path] = field(default_factory=list)


class SMTPMailer:
    def __init__(self, host: str, port: int, sender: str) -> None:
        self.host, self.port, self.sender = host, port, sender

    def send(self, mail: Mail) -> None:
        msg = EmailMessage()
        msg["From"] = self.sender
        msg["To"] = mail.to
        msg["Subject"] = mail.subject
        msg.set_content(mail.body)
        for path in mail.attachments:
            msg.add_attachment(
                path.read_bytes(),
                maintype="application",
                subtype="pdf" if path.suffix == ".pdf" else "octet-stream",
                filename=path.name,
            )
        # Raises smtplib.SMTPException / OSError on failure — the task layer retries those.
        with smtplib.SMTP(self.host, self.port, timeout=10) as smtp:
            smtp.send_message(msg)


class MemoryMailer:
    def __init__(self) -> None:
        self.sent: list[Mail] = []
        self.fail_times = 0
        self.calls = 0

    def send(self, mail: Mail) -> None:
        self.calls += 1
        if self.fail_times > 0:
            self.fail_times -= 1
            raise smtplib.SMTPServerDisconnected("simulated SMTP outage")
        self.sent.append(mail)


_mailer: SMTPMailer | MemoryMailer | None = None


def get_mailer() -> SMTPMailer | MemoryMailer:
    global _mailer
    if _mailer is None:
        s = get_settings()
        _mailer = (
            MemoryMailer()
            if s.mail_backend == "memory"
            else SMTPMailer(s.smtp_host, s.smtp_port, s.mail_from)
        )
    return _mailer


def set_mailer(mailer: SMTPMailer | MemoryMailer | None) -> None:
    global _mailer
    _mailer = mailer
