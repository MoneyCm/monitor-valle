import smtplib
from email.message import EmailMessage
import os
from pathlib import Path
from src.core.logging_config import logger

def send_report_email():
    sender_email = os.environ.get("SMTP_USER")
    sender_password = os.environ.get("SMTP_PASSWORD")
    receiver_email = os.environ.get("RECEIVER_EMAIL")
    
    if not all([sender_email, sender_password, receiver_email]):
        logger.error("Faltan variables de entorno para el correo. Configura SMTP_USER, SMTP_PASSWORD y RECEIVER_EMAIL.")
        return

    msg = EmailMessage()
    msg['Subject'] = 'Nuevo Boletín de Seguridad - Jamundí (Datos Actualizados)'
    msg['From'] = sender_email
    msg['To'] = receiver_email
    
    msg.set_content("""Hola,

Se ha detectado una actualización en la información de seguridad de Jamundí en el observatorio.
Adjunto encontrarás el boletín semanal generado automáticamente con los nuevos datos consolidados.

Este es un mensaje automático del pipeline de Monitor Valle.

Saludos,
Monitor Valle Bot""")

    pdf_path = Path("data/final/boletin_semanal_jamundi.pdf")
    if pdf_path.exists():
        with open(pdf_path, 'rb') as f:
            pdf_data = f.read()
            msg.add_attachment(pdf_data, maintype='application', subtype='pdf', filename=pdf_path.name)
    else:
        logger.warning(f"PDF no encontrado en {pdf_path}. Se enviará el correo sin adjunto.")

    try:
        smtp_server = os.environ.get("SMTP_SERVER", "smtp.gmail.com")
        smtp_port = int(os.environ.get("SMTP_PORT", 587))
        
        logger.info(f"Conectando al servidor SMTP {smtp_server}:{smtp_port}...")
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.send_message(msg)
        logger.success(f"Correo con reporte enviado exitosamente a {receiver_email}")
    except Exception as e:
        logger.error(f"Error crítico al enviar correo: {e}")

if __name__ == "__main__":
    send_report_email()
