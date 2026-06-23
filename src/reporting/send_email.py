"""Envio del boletin PDF por correo electronico via SMTP.

Lee credenciales SMTP de variables de entorno y adjunta el PDF generado.
"""
import smtplib
from email.message import EmailMessage
import os
from pathlib import Path
from src.core.config import settings
from src.core.logging_config import logger


def send_report_email():
    """Envia el boletin PDF por correo electronico."""
    sender_email = os.environ.get("SMTP_USER")
    sender_password = os.environ.get("SMTP_PASSWORD")
    receiver_email = os.environ.get("RECEIVER_EMAIL")
    municipio = settings.obs_municipio
    municipio_slug = settings.municipio_slug
    
    if not all([sender_email, sender_password, receiver_email]):
        logger.error("Faltan variables de entorno para el correo. Configura SMTP_USER, SMTP_PASSWORD y RECEIVER_EMAIL.")
        return

    msg = EmailMessage()
    msg['Subject'] = f'Actualizacion: Boletin Estadistico de Seguridad y Convivencia - {municipio}'
    msg['From'] = sender_email
    msg['To'] = receiver_email
    
    msg.set_content(f"""Cordial saludo,

Les informo que se ha detectado una nueva actualizacion de datos en el Observatorio del Delito Valle. Con base en esta informacion, se ha generado el Boletin Estadistico de Seguridad y Convivencia para el municipio de {municipio}, comparando el acumulado del año actual frente al mismo periodo del año anterior.

Adjunto a este correo comparto el boletin detallado en formato PDF para su revision.

Atentamente,

Cesar Alfonso Forero Molano
Profesional Universitario II
Secretaria de Seguridad y Convivencia
Alcaldia Municipal de {municipio}""")

    pdf_path = settings.final_dir / f"boletin_semanal_{municipio_slug}.pdf"
    if pdf_path.exists():
        with open(pdf_path, 'rb') as f:
            pdf_data = f.read()
            msg.add_attachment(pdf_data, maintype='application', subtype='pdf', filename=pdf_path.name)
    else:
        logger.warning(f"PDF no encontrado en {pdf_path}. Se enviara el correo sin adjunto.")

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
        logger.error(f"Error critico al enviar correo: {e}")


if __name__ == "__main__":
    send_report_email()
