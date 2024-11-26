import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from src.utils.logger import logger
# from src.config.email_config import emailconfig
import src.config.config as config

def send_email(subject, message,to_email):
    try:
        # 创建一个MIMEMultipart对象
        msg = MIMEMultipart()
        msg['From'] = config.MAIL_FROM_EMAIL
        msg['To'] = to_email
        msg['Subject'] = subject

        # 将消息正文添加到MIMEMultipart对象中
        msg.attach(MIMEText(message, 'plain'))

        # 创建SMTP对象并连接到SMTP服务器
 
        host = config.MAIL_SMTPHOST
        user = config.MAIL_USER
        from_email = config.MAIL_FROM_EMAIL
        password = config.MAIL_PWD
        server = smtplib.SMTP(host)
        server.connect(host,25)
        server.login(user, password)

        # 发送邮件
        server.sendmail(from_email, to_email, msg.as_string())

        # 关闭SMTP连接
        server.quit()
    except Exception as e:
        logger.error(f'邮件发送失败: {e}')