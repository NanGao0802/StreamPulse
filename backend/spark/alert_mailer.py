import logging
import os
import smtplib
from email.header import Header
from email.mime.text import MIMEText
from typing import Dict, List

from dotenv import load_dotenv

load_dotenv("/opt/pipeline/conf/alert.env")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name, str(default)).strip().lower()
    return value in {"1", "true", "yes", "on"}


class AlertMailer:
    _instance = None

    def __init__(self) -> None:
        self.enabled = _env_bool("ALERT_EMAIL_ENABLED", False)
        self.host = os.getenv("SMTP_HOST", "")
        self.port = int(os.getenv("SMTP_PORT", "465"))
        self.use_ssl = _env_bool("SMTP_USE_SSL", True)
        self.use_tls = _env_bool("SMTP_USE_TLS", False)
        self.user = os.getenv("SMTP_USER", "")
        self.password = os.getenv("SMTP_PASSWORD", "")
        self.mail_from = os.getenv("SMTP_FROM", "")
        self.mail_to = [x.strip() for x in os.getenv("SMTP_TO", "").split(",") if x.strip()]

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = AlertMailer()
        return cls._instance

    def is_ready(self) -> bool:
        if not self.enabled:
            return False
        if not self.host or not self.port or not self.user or not self.password or not self.mail_from:
            return False
        if len(self.mail_to) == 0:
            return False
        if "YOUR_QQ_SMTP_AUTH_CODE" in self.password:
            return False
        return True

    def send_alert(
        self,
        topic: str,
        stat_hour: str,
        comment_count: int,
        negative_count: int,
        negative_ratio: float,
        top_negative_comments: List[Dict]
    ) -> Dict[str, str]:
        if not self.is_ready():
            logger.warning("Alert mail disabled or SMTP config incomplete")
            return {
                "status": "disabled",
                "message": "email disabled or smtp config incomplete"
            }

        subject = f"[舆情预警] {topic} 在 {stat_hour} 负面占比异常"

        lines = []
        for idx, item in enumerate(top_negative_comments, start=1):
            publish_time = item.get("publish_time", "")
            heat_score = item.get("heat_score", "")
            text = item.get("text", "")
            lines.append(
                f"{idx}. 发布时间：{publish_time}\n"
                f"   热度：{heat_score}\n"
                f"   评论：{text}"
            )

        comments_block = "\n\n".join(lines) if lines else "无高热度负面评论样例"

        body = f"""
检测到舆情预警，请及时关注：

话题：{topic}
统计小时：{stat_hour}
该小时评论总数：{comment_count}
该小时负面评论数：{negative_count}
该小时负面占比：{negative_ratio:.2%}

该话题该小时内热度最高的负面 3 条评论如下：
{comments_block}

该邮件由实时舆情分析与预警系统自动发送。
""".strip()

        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = Header(subject, "utf-8")
        msg["From"] = self.mail_from
        msg["To"] = ", ".join(self.mail_to)

        try:
            if self.use_ssl:
                server = smtplib.SMTP_SSL(self.host, self.port, timeout=30)
            else:
                server = smtplib.SMTP(self.host, self.port, timeout=30)

            if self.use_tls and not self.use_ssl:
                server.starttls()

            server.login(self.user, self.password)
            server.sendmail(self.mail_from, self.mail_to, msg.as_string())
            server.quit()

            logger.info(f"Alert email sent: topic={topic}, stat_hour={stat_hour}")
            return {
                "status": "sent",
                "message": "ok"
            }
        except Exception as e:
            logger.exception(f"Alert email send failed: {e}")
            return {
                "status": "failed",
                "message": str(e)
            }
