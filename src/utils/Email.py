import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from src.config.email_config import emailconfig

def send_email(subject, message,to_email):
    try:
        # 创建一个MIMEMultipart对象
        msg = MIMEMultipart()
        msg['From'] = emailconfig["from_email"]
        msg['To'] = to_email
        msg['Subject'] = subject

        # 将消息正文添加到MIMEMultipart对象中
        msg.attach(MIMEText(message, 'plain'))

        # 创建SMTP对象并连接到SMTP服务器
        host = emailconfig["smtpHost"]
        user = emailconfig["user"]
        from_email = emailconfig["from_email"]
        password = emailconfig["password"]
        server = smtplib.SMTP(host)
        server.connect(host,25)
        # server.starttls()
        server.login(user, password)

        # 发送邮件
        server.sendmail(from_email, to_email, msg.as_string())

        # 关闭SMTP连接
        server.quit()
    except Exception as e:
        print(e)
        print("邮件发送失败")

subject = "测试邮件"
message = "这是一封测试邮件"
to_email = "2435184615@qq.com"

send_email(subject, message, to_email)