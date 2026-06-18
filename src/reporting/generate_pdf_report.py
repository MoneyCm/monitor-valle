"""
Generador del Boletín Estadístico de Seguridad y Convivencia - Jamundí.
Diseño institucional (azul/amarillo) replicando el boletín de plataforma-seguridad.
Datos extraídos automáticamente del Observatorio del Delito Valle.
"""
import os
import datetime
from pathlib import Path

import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from fpdf import FPDF


class JamundiBoletinReporter:
    """Genera un PDF de 2 páginas con diseño institucional a partir del CSV maestro."""

    # --- Paleta institucional (RGB 0-255) ---
    COLOR_AZUL = (0, 51, 160)          # #0033A0 - primario
    COLOR_AMARILLO = (255, 192, 0)     # #FFC000 - acento
    COLOR_VERDE = (0, 163, 79)         # #00A34F - positivo
    COLOR_ROJO = (229, 62, 62)         # #E53E3E - negativo
    COLOR_NARANJA = (221, 107, 32)     # #DD6B20
    COLOR_DORADO = (214, 158, 46)      # #D69E2E
    COLOR_GRIS = (160, 174, 192)       # #A0AEC0
    COLOR_GRIS_CLARO = (248, 250, 252) # bg-gray-50
    COLOR_GRIS_TEXTO = (100, 116, 139) # text-gray-500
    COLOR_TEXTO = (51, 51, 51)         # #333
    COLOR_BLANCO = (255, 255, 255)
    COLOR_AZUL_FONDO = (239, 246, 255) # bg-blue-50
    COLOR_AZUL_BORDE = (219, 234, 254) # border-blue-100

    FUENTE_OBSERVATORIO = "Observatorio del Delito Valle (www.observatoriodeldelitovalle.co)"

    DELITOS = [
        "Homicidio", "Lesiones Personales", "Hurto Personas", "Hurto Motocicletas",
        "Hurto Automotores", "Extorsión", "Violencia Intrafamiliar",
        "Hurto Entidades Comerciales", "Hurto Residencias",
        "Acceso Carnal O Acto Sexual Violento",
        "Actos Sexuales Con Menor De 14 Años",
        "Secuestro",
    ]

    def __init__(self, csv_path, output_path, escudo_path=None):
        self.csv_path = Path(csv_path)
        self.output_path = Path(output_path)
        self.escudo_path = Path(escudo_path) if escudo_path else None
        self.temp_dir = self.output_path.parent / "temp_boletin"
        self.temp_dir.mkdir(exist_ok=True)

        self.current_year = None
        self.prev_year = None

    # ================================================================
    #  UTILIDADES
    # ================================================================
    @staticmethod
    def _safe(text):
        """Convierte a latin-1 reemplazando caracteres no soportados (Helvetica)."""
        return str(text).encode('latin-1', 'replace').decode('latin-1')

    def _set_rgb(self, pdf, color):
        pdf.set_fill_color(*color)
        pdf.set_draw_color(*color)
        pdf.set_text_color(*color)

    # ================================================================
    #  EXTRACCIÓN DE DATOS
    # ================================================================
    def _load_data(self):
        df = pd.read_csv(self.csv_path, encoding="utf-8-sig")
        df['col_1'] = pd.to_numeric(df['col_1'], errors='coerce').fillna(0)
        df['col_9'] = df['col_9'].astype(str).str.strip()
        return df

    def _detect_years(self, df):
        """Detecta el año más reciente y el anterior."""
        years = sorted(df['col_9'].unique(), reverse=True)
        years = [y for y in years if y.isdigit()]
        self.current_year = years[0] if years else str(datetime.datetime.now().year)
        self.prev_year = years[1] if len(years) > 1 else str(int(self.current_year) - 1)
        return years

    def _get_crime_value(self, df, delito, year):
        """Obtiene el conteo de un delito específico en un año."""
        mask = (df['col_0'].str.contains(delito, case=False, na=False)) & \
               (df['col_9'] == str(year))
        rows = df[mask]
        if rows.empty:
            return 0
        return int(rows['col_1'].max())

    def _extract_indicadores(self, df):
        """Compara cada delito: año actual vs año anterior."""
        results = []
        for delito in self.DELITOS:
            v_curr = self._get_crime_value(df, delito, self.current_year)
            v_prev = self._get_crime_value(df, delito, self.prev_year)
            diff = v_curr - v_prev
            if v_prev > 0:
                var_pct = ((v_curr - v_prev) / v_prev) * 100
                var_str = f"{var_pct:+.1f}%"
            else:
                var_str = "+100%" if v_curr > 0 else "N/A"
            results.append({
                'name': delito, 'current': v_curr, 'prev': v_prev,
                'diff': diff, 'var': var_str,
            })
        results.sort(key=lambda x: x['current'], reverse=True)
        return results

    def _calc_totals(self, indicadores):
        """Suma los totales de todos los delitos por año."""
        total_curr = sum(r['current'] for r in indicadores)
        total_prev = sum(r['prev'] for r in indicadores)
        return total_curr, total_prev

    def _generate_chart(self, indicadores):
        """Gráfica de barras agrupadas: año actual vs anterior (top 8 delitos)."""
        top = indicadores[:8]
        names = []
        for r in top:
            n = r['name']
            if len(n) > 14:
                n = n[:12] + ".."
            names.append(n)
        current = [r['current'] for r in top]
        prev = [r['prev'] for r in top]

        fig, ax = plt.subplots(figsize=(9, 3.8))
        x = range(len(names))
        bw = 0.35
        ax.bar([i - bw / 2 for i in x], prev, bw,
               label=f"{self.prev_year}", color='#A0AEC0')
        ax.bar([i + bw / 2 for i in x], current, bw,
               label=f"{self.current_year}", color='#0033A0')

        ax.set_xticks(list(x))
        ax.set_xticklabels(names, rotation=25, ha='right', fontsize=8, color='#555')
        ax.set_ylabel('Hechos delictivos', fontsize=9, color='#666')
        ax.legend(fontsize=9, frameon=False)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.tick_params(colors='#888')
        ax.grid(axis='y', linestyle='--', alpha=0.25)
        plt.tight_layout()
        path = self.temp_dir / "evolucion.png"
        plt.savefig(path, dpi=200, bbox_inches='tight')
        plt.close()
        return path

    # ================================================================
    #  ELEMENTOS VISUALES (dibujo en el PDF)
    # ================================================================
    def _draw_header(self, pdf, large=True):
        """Header con escudo + título + borde amarillo inferior."""
        page_w = pdf.w
        margin = 15

        if large:
            escudo_h = 22
            escudo_w = 17
            title_size = 15
            border_w = 1.8  # 6px aprox
            gap = 5
        else:
            escudo_h = 12
            escudo_w = 9
            title_size = 10
            border_w = 1.2
            gap = 3

        y_start = 15

        # --- Lado izquierdo: escudo + título ---
        if self.escudo_path and self.escudo_path.exists():
            pdf.image(str(self.escudo_path),
                      x=margin, y=y_start, w=escudo_w, h=escudo_h)

        text_x = margin + escudo_w + gap

        # Título principal
        pdf.set_xy(text_x, y_start)
        pdf.set_font("Helvetica", "B", title_size)
        pdf.set_text_color(*self.COLOR_AZUL)
        if large:
            pdf.cell(page_w - text_x - margin - 60, title_size * 0.5,
                     self._safe("BOLETIN ESTADISTICO DE"), new_x="LMARGIN", new_y="NEXT")
            pdf.set_x(text_x)
            pdf.cell(page_w - text_x - margin - 60, title_size * 0.5,
                     self._safe("SEGURIDAD Y CONVIVENCIA"), new_x="LMARGIN", new_y="NEXT")
        else:
            pdf.cell(0, 7, self._safe("BOLETIN ESTADISTICO DE SEGURIDAD Y CONVIVENCIA"),
                     new_x="LMARGIN", new_y="NEXT")

        # Subtítulo
        pdf.set_x(text_x)
        pdf.set_font("Helvetica", "B", 7 if not large else 8)
        pdf.set_text_color(*self.COLOR_GRIS_TEXTO)
        pdf.cell(0, 4, self._safe("ALCALDIA MUNICIPAL DE JAMUNDI"),
                 new_x="LMARGIN", new_y="NEXT")

        # --- Lado derecho: fuente + corte ---
        right_w = 58
        rx = page_w - margin - right_w
        pdf.set_xy(rx, y_start + 1)
        pdf.set_font("Helvetica", "B", 9 if large else 7)
        pdf.set_text_color(*self.COLOR_AZUL)
        pdf.multi_cell(right_w, 4.5,
                       self._safe("Observatorio del Delito Valle"),
                       align="R")

        pdf.set_xy(rx, y_start + (12 if large else 8))
        pdf.set_font("Helvetica", "B", 7)
        pdf.set_text_color(*self.COLOR_GRIS_TEXTO)
        fecha_corte = datetime.datetime.now().strftime("%d/%m/%Y")
        pdf.multi_cell(right_w, 4,
                       self._safe(f"Corte: {fecha_corte}\nMunicipio: Jamundi"),
                       align="R")

        # --- Borde amarillo inferior ---
        line_y = y_start + escudo_h + 3
        pdf.set_draw_color(*self.COLOR_AMARILLO)
        pdf.set_line_width(border_w)
        pdf.line(margin, line_y, page_w - margin, line_y)

        return line_y + 6  # Y donde empieza el contenido

    def _draw_section_title(self, pdf, y, number, title):
        """Título de sección con borde amarillo izquierdo + texto azul."""
        margin = 15
        bar_w = 1.5
        pdf.set_fill_color(*self.COLOR_AMARILLO)
        pdf.rect(margin, y - 0.5, bar_w, 7, style='F')

        pdf.set_xy(margin + bar_w + 3, y)
        pdf.set_font("Helvetica", "B", 12)
        pdf.set_text_color(*self.COLOR_AZUL)
        pdf.cell(0, 7, self._safe(f"{number}. {title.upper()}"),
                 new_x="LMARGIN", new_y="NEXT")
        return y + 12

    def _draw_kpi_card(self, pdf, x, y, w, h, accent_color, label, value, subtext=""):
        """Tarjeta KPI con borde superior de color."""
        # Fondo blanco con borde gris
        pdf.set_fill_color(*self.COLOR_BLANCO)
        pdf.set_draw_color(*self.COLOR_GRIS)
        pdf.set_line_width(0.3)
        pdf.rect(x, y, w, h, style='DF')

        # Borde superior de color (acento)
        pdf.set_fill_color(*accent_color)
        pdf.set_draw_color(*accent_color)
        pdf.rect(x, y, w, 1.5, style='F')

        # Etiqueta (arriba)
        pdf.set_xy(x + 2, y + 3)
        pdf.set_font("Helvetica", "B", 7)
        pdf.set_text_color(*self.COLOR_GRIS_TEXTO)
        pdf.multi_cell(w - 4, 3.5, self._safe(label.upper()), align="C")

        # Valor (centro, grande)
        pdf.set_xy(x, y + h / 2 - 3)
        pdf.set_font("Helvetica", "B", 18)
        pdf.set_text_color(*accent_color)
        pdf.cell(w, 8, str(value), align="C", new_x="LMARGIN", new_y="NEXT")

        # Subtexto (abajo)
        if subtext:
            pdf.set_xy(x + 1, y + h - 7)
            pdf.set_font("Helvetica", "B", 6)
            pdf.set_text_color(*self.COLOR_GRIS_TEXTO)
            pdf.multi_cell(w - 2, 3, self._safe(subtext), align="C")

    def _draw_info_box(self, pdf, y, text, w=None):
        """Caja de información con fondo azul claro."""
        margin = 15
        if w is None:
            w = pdf.w - margin * 2
        lines = pdf.get_string_width(text)  # rough
        # Estimar altura
        pdf.set_font("Helvetica", "", 8)
        # Calcular líneas necesarias
        chars_per_line = int(w / 1.6)
        num_lines = max(1, len(text) / chars_per_line)
        box_h = num_lines * 4 + 6

        pdf.set_fill_color(*self.COLOR_AZUL_FONDO)
        pdf.set_draw_color(*self.COLOR_AZUL_BORDE)
        pdf.set_line_width(0.3)
        pdf.rect(margin, y, w, box_h, style='DF')

        pdf.set_xy(margin + 2, y + 2)
        pdf.set_font("Helvetica", "B", 8)
        pdf.set_text_color(30, 58, 138)  # blue-900
        pdf.multi_cell(w - 4, 4, self._safe(text), align="L")
        return y + box_h + 4

    def _draw_footer(self, pdf, page_num):
        """Footer fijo al pie de página."""
        margin = 15
        page_w = pdf.w
        page_h = pdf.h
        y = page_h - 18

        # Línea superior
        pdf.set_draw_color(*self.COLOR_GRIS)
        pdf.set_line_width(0.2)
        pdf.line(margin, y, page_w - margin, y)

        pdf.set_xy(margin, y + 2)
        pdf.set_font("Helvetica", "B", 7)
        pdf.set_text_color(*self.COLOR_GRIS_TEXTO)
        pdf.cell(page_w - margin * 2, 5,
                 self._safe(f"Fuente: {self.FUENTE_OBSERVATORIO}"),
                 align="L", new_x="LMARGIN", new_y="NEXT")

        pdf.set_xy(margin, y + 7)
        pdf.set_font("Helvetica", "", 7)
        pdf.set_text_color(*self.COLOR_GRIS)
        pdf.cell(page_w - margin * 2, 4,
                 self._safe(f"Pagina {page_num}  |  Alcaldia Municipal de Jamundi"),
                 align="R")

    def _draw_table(self, pdf, y, headers, rows, col_widths, col_aligns=None):
        """Tabla con header azul, filas alternadas, diferencias en color."""
        margin = 15
        if col_aligns is None:
            col_aligns = ['L'] + ['C'] * (len(headers) - 1)

        row_h = 7
        header_h = 8

        # --- Header ---
        x = margin
        pdf.set_fill_color(*self.COLOR_AZUL)
        pdf.set_draw_color(*self.COLOR_AZUL)
        pdf.rect(x, y, sum(col_widths), header_h, style='F')

        for i, h in enumerate(headers):
            pdf.set_xy(x, y + 1)
            pdf.set_font("Helvetica", "B", 7.5)
            pdf.set_text_color(*self.COLOR_BLANCO)
            pdf.cell(col_widths[i], header_h - 2,
                     self._safe(h.upper()), align='C')
            x += col_widths[i]

        y += header_h

        # --- Filas ---
        for idx, row in enumerate(rows):
            x = margin
            # Fondo alternado
            if idx % 2 == 1:
                pdf.set_fill_color(*self.COLOR_GRIS_CLARO)
                pdf.set_draw_color(*self.COLOR_GRIS_CLARO)
                pdf.rect(x, y, sum(col_widths), row_h, style='F')

            pdf.set_draw_color(220, 220, 220)
            pdf.set_line_width(0.2)
            pdf.rect(x, y, sum(col_widths), row_h, style='D')

            for i, val in enumerate(row):
                pdf.set_xy(x, y + 1)
                pdf.set_font("Helvetica", "", 7.5)

                # Color para diferencias (columnas DIF. o VAR.%)
                sval = str(val)
                is_diff_col = headers[i].startswith(('DIF', 'VAR'))
                if is_diff_col:
                    # Parsear valor numérico (quita + y %)
                    try:
                        num_val = float(sval.lstrip('+').rstrip('%'))
                    except ValueError:
                        num_val = 0  # "N/A" u otro texto

                    if num_val > 0:
                        # Más delitos vs año anterior = ROJO (empeoró)
                        pdf.set_text_color(*self.COLOR_ROJO)
                    elif num_val < 0:
                        # Menos delitos vs año anterior = VERDE (mejoró)
                        pdf.set_text_color(*self.COLOR_VERDE)
                    else:
                        # Sin cambios = gris neutro
                        pdf.set_text_color(*self.COLOR_GRIS_TEXTO)
                    pdf.set_font("Helvetica", "B", 7.5)
                elif i == 0:
                    pdf.set_text_color(*self.COLOR_TEXTO)
                    pdf.set_font("Helvetica", "B", 7.5)
                else:
                    pdf.set_text_color(*self.COLOR_TEXTO)
                    pdf.set_font("Helvetica", "B", 7.5)

                pdf.cell(col_widths[i], row_h - 2,
                         self._safe(sval), align=col_aligns[i])
                x += col_widths[i]

            y += row_h

        return y + 5

    # ================================================================
    #  CONSTRUCCIÓN DEL PDF
    # ================================================================
    def create_boletin(self):
        df = self._load_data()
        self._detect_years(df)
        indicadores = self._extract_indicadores(df)
        total_curr, total_prev = self._calc_totals(indicadores)

        # KPIs acumulados
        if total_prev > 0:
            var_ytd = ((total_curr - total_prev) / total_prev) * 100
            var_ytd_str = f"{var_ytd:+.1f}%"
            diff_ytd = total_curr - total_prev
        else:
            var_ytd_str = "N/A"
            diff_ytd = 0

        # Periodo más reciente aproximado (último mes con datos del año actual)
        fecha_corte = datetime.datetime.now().strftime("%B %Y")

        # Generar gráfica
        chart_path = self._generate_chart(indicadores)

        # --- PDF ---
        pdf = FPDF(orientation='P', unit='mm', format='A4')
        pdf.set_auto_page_break(auto=False)
        pdf.set_margins(15, 15, 15)

        # ===================== PÁGINA 1 =====================
        pdf.add_page()

        # Header
        content_y = self._draw_header(pdf, large=True)

        # --- Sección 1: Introducción ---
        content_y = self._draw_section_title(pdf, content_y, 1, "Introduccion y Alcance")

        pdf.set_xy(15, content_y)
        pdf.set_font("Helvetica", "", 9)
        pdf.set_text_color(*self.COLOR_TEXTO)
        intro = (f"El presente boletin presenta el analisis estadistico de los principales "
                 f"delitos del municipio de Jamundi para el ano {self.current_year}, "
                 f"comparado con el ano {self.prev_year}. Los datos fueron obtenidos "
                 f"directamente del Observatorio del Delito Valle.")
        pdf.multi_cell(180, 5, self._safe(intro), align="J")
        content_y = pdf.get_y() + 3

        # Caja de información
        info = ("Lectura correcta: el Panorama General corresponde al acumulado "
                f"del ano {self.current_year} hasta la fecha de corte, "
                f"frente al mismo periodo del ano {self.prev_year}. "
                f"Fuente: Observatorio del Delito Valle.")
        content_y = self._draw_info_box(pdf, content_y, info)

        # --- Sección 2: Panorama General Acumulado ---
        content_y = self._draw_section_title(pdf, content_y, 2, "Panorama General Acumulado")

        pdf.set_xy(15, content_y)
        pdf.set_font("Helvetica", "", 8)
        pdf.set_text_color(*self.COLOR_GRIS_TEXTO)
        pdf.multi_cell(180, 4,
                       self._safe(f"Periodo acumulado comparado: ano {self.current_year} "
                                  f"frente al ano {self.prev_year}."),
                       align="L")
        content_y = pdf.get_y() + 3

        # 3 tarjetas KPI
        card_w = 56
        card_h = 30
        card_gap = 6
        start_x = 15
        var_color = self.COLOR_VERDE if var_ytd <= 0 else self.COLOR_ROJO
        var_bg_note = f"Diferencia: {'+' if diff_ytd > 0 else ''}{diff_ytd} hechos"

        self._draw_kpi_card(pdf, start_x, content_y, card_w, card_h,
                            self.COLOR_AZUL,
                            f"Total {self.prev_year}", f"{total_prev:,}", "Acumulado anual")
        self._draw_kpi_card(pdf, start_x + card_w + card_gap, content_y, card_w, card_h,
                            self.COLOR_AZUL,
                            f"Total {self.current_year}", f"{total_curr:,}", "Acumulado anual")
        self._draw_kpi_card(pdf, start_x + 2 * (card_w + card_gap), content_y, card_w, card_h,
                            var_color,
                            "Variacion acumulada", var_ytd_str, var_bg_note)
        content_y += card_h + 10

        # --- Sección 3: Comportamiento por delito ---
        content_y = self._draw_section_title(pdf, content_y, 3,
                                             f"Top 5 Delitos - {self.current_year}")

        top5 = indicadores[:5]
        # 5 tarjetas horizontales más pequeñas
        c5_w = 33
        c5_gap = 3
        c5_h = 28
        for i, item in enumerate(top5):
            cx = 15 + i * (c5_w + c5_gap)
            self._draw_kpi_card(pdf, cx, content_y, c5_w, c5_h,
                                self.COLOR_AZUL,
                                item['name'][:18], f"{item['current']:,}",
                                f"vs {item['prev']} ({item['var']})")

        # Footer
        self._draw_footer(pdf, 1)

        # ===================== PÁGINA 2 =====================
        pdf.add_page()

        # Mini header
        content_y = self._draw_header(pdf, large=False)

        # --- Sección 4: Tabla comparativa acumulada ---
        content_y = self._draw_section_title(pdf, content_y, 4,
                                             f"Comparativo Acumulado por Delito")

        headers_4 = ["Delito", f"Acum. {self.prev_year}",
                     f"Acum. {self.current_year}", "Dif.", "Var. %"]
        widths_4 = [55, 30, 30, 25, 25]
        rows_4 = []
        for r in indicadores:
            diff_str = f"{'+' if r['diff'] > 0 else ''}{r['diff']}"
            rows_4.append([r['name'], f"{r['prev']:,}", f"{r['current']:,}",
                           diff_str, r['var']])

        content_y = self._draw_table(pdf, content_y, headers_4, rows_4, widths_4)

        # --- Sección 5: Gráfica de evolución ---
        content_y = self._draw_section_title(pdf, content_y, 5, "Evolucion Temporal del Delito")

        # Insertar gráfica
        if chart_path and chart_path.exists():
            # Calcular dimensiones manteniendo proporción
            img_w = 170
            pdf.image(str(chart_path), x=15, y=content_y, w=img_w)
            content_y += 70  # altura aproximada de la gráfica

        # Caja explicativa
        content_y += 5
        expl = (f"Grafica comparativa: ano {self.current_year} (azul) vs ano "
                f"{self.prev_year} (gris). Datos del Observatorio del Delito Valle "
                f"para Jamundi.")
        self._draw_info_box(pdf, content_y, expl)

        # Footer
        self._draw_footer(pdf, 2)

        # --- Output ---
        pdf.output(str(self.output_path))

        # Limpieza temporal
        for f in self.temp_dir.glob("*.png"):
            os.remove(f)
        try:
            self.temp_dir.rmdir()
        except OSError:
            pass

        return self.output_path


if __name__ == "__main__":
    current_dir = Path(__file__).parent.parent.parent
    csv = current_dir / "data" / "final" / "jamundi_analytics_master.csv"
    output = current_dir / "data" / "final" / "boletin_semanal_jamundi.pdf"
    escudo = current_dir / "assets" / "escudo_jamundi.png"

    if not csv.exists():
        print(f"ERROR: No se encontro el CSV en {csv}")
        exit(1)

    reporter = JamundiBoletinReporter(csv, output, escudo)
    result = reporter.create_boletin()
    print(f"Boletin generado: {result}")
