import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders


EMAIL_ADDRESS = "ghoshpicklu2@gmail.com"
EMAIL_PASSWORD = "cjok miel xfyq lyei"
TO_EMAIL =  [
    "Picklu.Ghosh2@cognizant.com",
    "Arthi.Senthil@cognizant.com"
]

CSV_FILE = "data_comp.xlsx"
REPORT_FILE = "analysis_report.txt"

msg = MIMEMultipart()
msg["From"] = EMAIL_ADDRESS
msg["To"] = ", ".join(TO_EMAIL)
msg["Subject"] = "GitHub Action Data Quality Report"

body = """
Hello,

Please find attached files:

1. Excel Report.
2. AI Analysis Report

Thanks
"""

msg.attach(MIMEText(body, "plain"))

# List of attachments
files_to_attach = [CSV_FILE, REPORT_FILE]

for file_path in files_to_attach:

    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        continue

    with open(file_path, "rb") as attachment:

        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())

    encoders.encode_base64(part)

    part.add_header(
        "Content-Disposition",
        f"attachment; filename={os.path.basename(file_path)}",
    )

    msg.attach(part)


# Send Email
try:

    server = smtplib.SMTP("smtp.gmail.com", 587)
    server.starttls()

    server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)

    server.send_message(msg)

    server.quit()

    print("Email with CSV and analysis report sent successfully!")

except Exception as e:

    print("Error sending email:", e)
    raise
