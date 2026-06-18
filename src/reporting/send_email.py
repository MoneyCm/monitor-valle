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
    msg['Subject'] = 'Actualización: Boletín Estadístico de Seguridad y Convivencia - Jamundí'
    msg['From'] = sender_email
    msg['To'] = receiver_email
    
    msg.set_content("""Cordial saludo,

Les informo que se ha detectado una nueva actualización de datos en el Observatorio del Delito Valle. Con base en esta información, se ha generado el Boletín Estadístico de Seguridad y Convivencia para el municipio de Jamundí, comparando el acumulado del año actual frente al mismo periodo del año anterior.

Adjunto a este correo comparto el boletín detallado en formato PDF para su revisión.

Atentamente,

César Alfonso Forero Molano
Profesional Universitario II
Secretaría de Seguridad y Convivencia
Alcaldía Municipal de Jamundí""")

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
