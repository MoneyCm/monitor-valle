# Observatorio del Delito Valle - Jamundí Data Extractor

Este proyecto es un scraper automatizado y robusto diseñado para extraer datos estadísticos del portal [Observatorio del Delito Valle](https://www.observatoriodeldelitovalle.co/), específicamente para el municipio de **Jamundí**.

El sistema utiliza **Playwright** para la navegación y autenticación dinámica, además de interceptar peticiones de **Looker Studio** para obtener datos estructurados en formato JSON que luego son transformados a CSV y Excel.

## 🚀 Características
- **Login Automático:** Manejo de sesiones y cookies.
- **Descubrimiento de Datos:** Identificación automática de módulos, filtros y reportes.
- **Extracción de Looker Studio:** Captura de datos tabulares históricos.
- **Descarga de Reportes:** Localización y descarga sistemática de archivos PDF/Excel en `/mis-reportes`.
- **Deduplicación:** Control de registros mediante hashes para evitar duplicados.
- **GitHub Actions Ready:** Configurado para ejecutarse de forma programada.
- **Logs Detallados:** Seguimiento paso a paso con Loguru.

## 🛠️ Requisitos
- Python 3.9+
- Playwright (y sus navegadores)

## 📦 Instalación Local
1. Clonar el repositorio.
2. Crear un entorno virtual e instalar dependencias:
   ```bash
   python -m venv venv
   source venv/bin/activate  # En Windows: venv\Scripts\activate
   pip install -r requirements.txt
   playwright install chromium
   ```
3. Configurar variables de entorno:
   ```bash
   cp .env.example .env
   # Edita .env con tus credenciales OBS_USER y OBS_PASSWORD
   ```

## 🏃 Ejecución
Para ejecutar el pipeline completo de extracción:
```bash
python -m src.pipelines.run_full_extract
```

Opciones disponibles (próximamente):
- `--year 2024`: Filtrar por año específico.
- `--discover-only`: Solo mapear el sitio sin extraer.
- `--full`: Extracción histórica completa.

## 📂 Estructura de Salida
Los datos se guardan en la carpeta `data/`:
- `data/raw/`: Datos JSON/CSV tal cual se obtuvieron.
- `data/processed/`: Datos normalizados y validados.
- `data/final/`: Archivos consolidados (Excel Maestro y CSV Global).

## 🛡️ Seguridad
- Las credenciales nunca se guardan en el código, se leen de `.env` o GitHub Secrets.
- Manejo de reintentos con backoff exponencial para respetar el servidor.

## 📝 Mantenimiento
Si la interfaz del portal cambia (selectores de login o rutas de menú), actualiza los selectores en `src/scrapers/auth.py` y `src/scrapers/discovery.py`.
