import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import os
import hashlib
from pathlib import Path
from src.core.config import settings
from src.core.logging_config import logger


def send_report_email():
    """Envia el boletin PDF por correo electronico."""
    sender_email = os.environ.get("SMTP_USER") or os.environ.get("GMAIL_USER")
    sender_password = os.environ.get("SMTP_PASSWORD") or os.environ.get("GMAIL_PASS") or os.environ.get("GMAIL_APP_PASSWORD")
    receiver_email = os.environ.get("RECEIVER_EMAIL") or os.environ.get("EMAIL_DEST") or sender_email
    municipio = settings.obs_municipio
    municipio_slug = settings.municipio_slug
    
    if not all([sender_email, sender_password, receiver_email]):
        logger.error("Faltan variables de entorno para el correo (SMTP_USER/GMAIL_USER, SMTP_PASSWORD/GMAIL_PASS y RECEIVER_EMAIL/EMAIL_DEST).")
        return

    pdf_path = settings.final_dir / f"boletin_semanal_{municipio_slug}.pdf"
    
    # Calcular SHA256 del boletín PDF
    sha_pdf = "N/A"
    if pdf_path.exists():
        try:
            sha256_hash = hashlib.sha256()
            with open(pdf_path, "rb") as f:
                for byte_block in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(byte_block)
            sha_pdf = sha256_hash.hexdigest()
        except Exception as hash_err:
            logger.error(f"Error calculando SHA256 para el boletin: {hash_err}")

    asunto = f'🚨 Actualización: Boletín Estadístico de Seguridad y Convivencia - {municipio}'

    cuerpo_html = f"""
    <html>
    <body style="font-family:'Segoe UI',Arial,sans-serif;color:#1A1A2E;max-width:640px;margin:auto;background-color:#f4f4f8;padding:20px;">
      <div style="background:#281FD0;padding:24px 28px;border-bottom:4px solid #FFE000;border-radius:6px 6px 0 0;box-shadow:0 4px 10px rgba(0,0,0,0.1);">
        <div style="font-size:10px;color:#FFE000;letter-spacing:2px;font-weight:bold;text-transform:uppercase;">Alcaldía de Jamundí · Valle del Cauca</div>
        <h2 style="color:white;margin:6px 0 0;font-size:18px;">📊 OBSERVATORIO DEL DELITO VALLE</h2>
        <p style="color:rgba(255,255,255,.75);margin:4px 0 0;font-size:12px;">Monitoreo y Extracción de Datos de Looker Studio</p>
      </div>
      <div style="padding:28px;background:white;border-radius:0 0 6px 6px;box-shadow:0 4px 10px rgba(0,0,0,0.1);">
        <h3 style="color:#281FD0;margin-top:0;font-size:15px;text-transform:uppercase;">{asunto}</h3>
        
        <p>Cordial saludo,</p>
        
        <p>Les informo que se ha detectado una nueva actualización de datos en el portal del <b>Observatorio del Delito Valle</b>.</p>
        <p>Con base en esta información, se ha extraído la estadística y generado el <b>Boletín Estadístico de Seguridad y Convivencia</b> para el municipio de <b>{municipio}</b>, comparando el acumulado del año actual frente al mismo periodo del año anterior.</p>
        <p>Adjunto a este correo comparto el boletín detallado en formato PDF para su revisión y análisis correspondiente.</p>
        
        <!-- Caja de Integridad del Reporte -->
        <div style="margin-top:24px;padding:12px 16px;background:#fffde7;border-left:4px solid #FFE000;font-size:11px;color:#555566;border-radius:4px;">
          <b>Integridad del Reporte (Archivo PDF Adjunto):</b><br>
          SHA256 Checksum: <code style="font-size:10px;font-family:monospace;color:#281FD0;">{sha_pdf}</code>
        </div>
        
        <!-- Firma Profesional del Elaborador -->
        <div style="margin-top:30px;border-top:1px solid #e1e2eb;padding-top:15px;">
          <p style="margin:0;font-size:13px;font-weight:bold;color:#281FD0;">Elaborado por:</p>
          <p style="margin:4px 0 0;font-size:12px;color:#444455;line-height:1.4;">
            <b>César Alfonso Forero Molano</b><br>
            Profesional Universitario II<br>
            Secretaría de Seguridad y Convivencia<br>
            Alcaldía Municipal de {municipio}
          </p>
        </div>
      </div>
      <div style="background:#f8f9fa;padding:14px;text-align:center;font-size:11px;color:#999;border-top:1px solid #eee;border-radius:0 0 6px 6px;margin-top:15px;">
        Fuente: Observatorio del Delito Valle / Looker Studio · Municipio Jamundí (76364) · Generado automáticamente vía GitHub Actions
      </div>
    </body>
    </html>
    """

    msg = MIMEMultipart("mixed")
    msg['Subject'] = asunto
    msg['From'] = sender_email
    msg['To'] = receiver_email
    
    msg.attach(MIMEText(cuerpo_html, "html", "utf-8"))

    if pdf_path.exists():
        try:
            with open(pdf_path, 'rb') as f:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename="{pdf_path.name}"')
            msg.attach(part)
            logger.info(f"Boletin PDF adjuntado correctamente: {pdf_path.name}")
        except Exception as att_err:
            logger.error(f"Error al adjuntar el archivo PDF: {att_err}")
    else:
        logger.warning(f"PDF no encontrado en {pdf_path}. Se enviara el correo sin adjunto.")

    try:
        smtp_server = os.environ.get("SMTP_SERVER", "smtp.gmail.com")
        smtp_port = int(os.environ.get("SMTP_PORT", 587))
        
        logger.info(f"Conectando al servidor SMTP {smtp_server}:{smtp_port}...")
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, receiver_email, msg.as_string())
        logger.success(f"Correo con reporte enviado exitosamente a {receiver_email}")
    except Exception as e:
        logger.error(f"Error critico al enviar correo: {e}")


if __name__ == "__main__":
    send_report_email()
