"""
Generador del Boletin Estadistico de Seguridad y Convivencia.
Diseno institucional (azul/amarillo) replicando el boletin de plataforma-seguridad.
Datos extraidos automaticamente del Observatorio del Delito Valle.
"""
import os
import datetime
from pathlib import Path

from fpdf import FPDF

from src.reporting.data_analyzer import DataAnalyzer
from src.reporting.chart_generator import ChartGenerator


class BoletinReporter:
    """Genera un PDF de 2 paginas con diseno institucional a partir del CSV maestro."""

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

    def __init__(self, csv_path, output_path, escudo_path=None, municipio=None):
        self.csv_path = Path(csv_path)
        self.output_path = Path(output_path)
        self.escudo_path = Path(escudo_path) if escudo_path else None
        self.municipio = municipio or "Jamundi"
        self.temp_dir = self.output_path.parent / "temp_boletin"
        self.temp_dir.mkdir(exist_ok=True)

        # Analizador de datos (composicion)
        self.analyzer = DataAnalyzer(self.csv_path, municipio=self.municipio)

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
    #  ELEMENTOS VISUALES (dibujo en el PDF)
    # ================================================================
    def _draw_header(self, pdf, large=True):
        """Header con escudo + titulo + borde amarillo inferior."""
        page_w = pdf.w
        margin = 15

        if large:
            escudo_h = 22
            escudo_w = 17
            title_size = 15
            border_w = 1.8
            gap = 5
        else:
            escudo_h = 12
            escudo_w = 9
            title_size = 10
            border_w = 1.2
            gap = 3

        y_start = 15

        # --- Lado izquierdo: escudo + titulo ---
        if self.escudo_path and self.escudo_path.exists():
            pdf.image(str(self.escudo_path),
                      x=margin, y=y_start, w=escudo_w, h=escudo_h)

        text_x = margin + escudo_w + gap

        # Titulo principal
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

        # Subtitulo
        pdf.set_x(text_x)
        pdf.set_font("Helvetica", "B", 7 if not large else 8)
        pdf.set_text_color(*self.COLOR_GRIS_TEXTO)
        pdf.cell(0, 4, self._safe(f"ALCALDIA MUNICIPAL DE {self.municipio.upper()}"),
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
                       self._safe(f"Corte: {fecha_corte}\nMunicipio: {self.municipio}"),
                       align="R")

        # --- Borde amarillo inferior ---
        line_y = y_start + escudo_h + 3
        pdf.set_draw_color(*self.COLOR_AMARILLO)
        pdf.set_line_width(border_w)
        pdf.line(margin, line_y, page_w - margin, line_y)

        return line_y + 6

    def _draw_section_title(self, pdf, y, number, title):
        """Titulo de seccion con borde amarillo izquierdo + texto azul."""
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
        """Tarjeta KPI con borde superior de color y fondo premium destacado."""
        # Detectar color de fondo y de texto del valor
        if accent_color == self.COLOR_VERDE:
            card_bg = (240, 253, 244)
            border_color = (187, 247, 208)
            val_color = (22, 101, 52)
        elif accent_color == self.COLOR_ROJO:
            card_bg = (254, 242, 242)
            border_color = (254, 202, 202)
            val_color = (153, 27, 27)
        else:
            card_bg = self.COLOR_BLANCO
            border_color = self.COLOR_GRIS
            val_color = accent_color

        # Dibujar fondo y borde
        pdf.set_fill_color(*card_bg)
        pdf.set_draw_color(*border_color)
        pdf.set_line_width(0.3)
        pdf.rect(x, y, w, h, style='DF')

        # Borde superior de acento (solid)
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
        pdf.set_text_color(*val_color)
        pdf.cell(w, 8, str(value), align="C", new_x="LMARGIN", new_y="NEXT")

        # Subtexto (abajo)
        if subtext:
            pdf.set_xy(x + 1, y + h - 7)
            pdf.set_font("Helvetica", "B", 6)
            pdf.set_text_color(*self.COLOR_GRIS_TEXTO)
            pdf.multi_cell(w - 2, 3, self._safe(subtext), align="C")

    def _draw_info_box(self, pdf, y, text, w=None):
        """Caja de informacion con fondo azul claro."""
        margin = 15
        if w is None:
            w = pdf.w - margin * 2
        # Estimar altura
        pdf.set_font("Helvetica", "", 8)
        chars_per_line = int(w / 1.6)
        num_lines = max(1, len(text) / chars_per_line)
        box_h = num_lines * 4 + 6

        pdf.set_fill_color(*self.COLOR_AZUL_FONDO)
        pdf.set_draw_color(*self.COLOR_AZUL_BORDE)
        pdf.set_line_width(0.3)
        pdf.rect(margin, y, w, box_h, style='DF')

        pdf.set_xy(margin + 2, y + 2)
        pdf.set_font("Helvetica", "B", 8)
        pdf.set_text_color(30, 58, 138)
        pdf.multi_cell(w - 4, 4, self._safe(text), align="L")
        return y + box_h + 4

    def _draw_footer(self, pdf, page_num):
        """Footer fijo al pie de pagina."""
        margin = 15
        page_w = pdf.w
        page_h = pdf.h
        y = page_h - 18

        # Linea superior
        pdf.set_draw_color(*self.COLOR_GRIS)
        pdf.set_line_width(0.2)
        pdf.line(margin, y, page_w - margin, y)

        pdf.set_xy(margin, y + 2)
        pdf.set_font("Helvetica", "B", 7)
        pdf.set_text_color(*self.COLOR_GRIS_TEXTO)
        pdf.cell(page_w - margin * 2, 5,
                 self._safe(f"Fuente: {self.analyzer.FUENTE_OBSERVATORIO}"),
                 align="L", new_x="LMARGIN", new_y="NEXT")

        pdf.set_xy(margin, y + 7)
        pdf.set_font("Helvetica", "", 7)
        pdf.set_text_color(*self.COLOR_GRIS)
        pdf.cell(page_w - margin * 2, 4,
                 self._safe(f"Pagina {page_num}  |  Alcaldia Municipal de {self.municipio}"),
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
                sval = str(val)
                is_diff_col = headers[i].upper().startswith(('DIF', 'VAR'))
                
                if is_diff_col:
                    try:
                        num_val = float(sval.lstrip('+').rstrip('%'))
                    except ValueError:
                        num_val = 0

                    if num_val > 0:
                        bg_color = (254, 226, 226)
                        text_color = (153, 27, 27)
                    elif num_val < 0:
                        bg_color = (220, 252, 231)
                        text_color = (22, 101, 52)
                    else:
                        bg_color = (241, 245, 249)
                        text_color = (71, 85, 105)
                    
                    badge_w = col_widths[i] - 4
                    badge_h = row_h - 2
                    badge_x = x + 2
                    badge_y = y + 1
                    
                    pdf.set_fill_color(*bg_color)
                    pdf.rect(badge_x, badge_y, badge_w, badge_h, style='F')
                    
                    pdf.set_xy(badge_x, badge_y + 0.5)
                    pdf.set_font("Helvetica", "B", 7.5)
                    pdf.set_text_color(*text_color)
                    pdf.cell(badge_w, badge_h - 1, self._safe(sval), align='C')
                else:
                    pdf.set_xy(x, y + 1)
                    if i == 0:
                        pdf.set_text_color(*self.COLOR_TEXTO)
                        pdf.set_font("Helvetica", "B", 7.5)
                    else:
                        pdf.set_text_color(*self.COLOR_TEXTO)
                        pdf.set_font("Helvetica", "", 7.5)
                    
                    pdf.cell(col_widths[i], row_h - 2,
                             self._safe(sval), align=col_aligns[i])
                
                x += col_widths[i]

            y += row_h

        return y + 5

    # ================================================================
    #  CONSTRUCCION DEL PDF
    # ================================================================
    def create_boletin(self):
        """Orquesta la generacion completa del boletin PDF de 2 paginas."""
        df = self.analyzer.load_data()
        self.analyzer.detect_years(df)
        self.analyzer.detect_corte_month(df)
        indicadores = self.analyzer.extract_indicadores(df)
        total_curr, total_prev = self.analyzer.calc_totals(indicadores)

        # KPIs acumulados
        if total_prev > 0:
            var_ytd = ((total_curr - total_prev) / total_prev) * 100
            var_ytd_str = f"{var_ytd:+.1f}%"
            diff_ytd = total_curr - total_prev
        else:
            var_ytd_str = "N/A"
            diff_ytd = 0

        current_year = self.analyzer.current_year
        prev_year = self.analyzer.prev_year
        latest_date_str = self.analyzer.latest_date_str

        # Generar grafica
        chart_path = ChartGenerator.generate(
            indicadores, current_year, prev_year, self.temp_dir
        )

        # --- PDF ---
        pdf = FPDF(orientation='P', unit='mm', format='A4')
        pdf.set_auto_page_break(auto=False)
        pdf.set_margins(15, 15, 15)

        # ===================== PAGINA 1 =====================
        pdf.add_page()

        # Header
        content_y = self._draw_header(pdf, large=True)

        # --- Seccion 1: Introduccion ---
        content_y = self._draw_section_title(pdf, content_y, 1, "Introduccion y Alcance")

        pdf.set_xy(15, content_y)
        pdf.set_font("Helvetica", "", 9)
        pdf.set_text_color(*self.COLOR_TEXTO)
        intro = (f"El presente boletin presenta el analisis estadistico de los principales "
                 f"delitos del municipio de {self.municipio} para el ano {current_year}, "
                 f"comparado con el ano {prev_year}. Los datos fueron obtenidos "
                 f"directamente del Observatorio del Delito Valle.")
        pdf.multi_cell(180, 5, self._safe(intro), align="J")
        content_y = pdf.get_y() + 3

        # Caja de informacion
        info = ("Lectura correcta: el Panorama General corresponde al acumulado "
                f"del ano {current_year} hasta la fecha de corte, "
                f"frente al mismo periodo del ano {prev_year}. "
                f"Fuente: Observatorio del Delito Valle.")
        content_y = self._draw_info_box(pdf, content_y, info)

        # --- Seccion 2: Panorama General Acumulado ---
        content_y = self._draw_section_title(pdf, content_y, 2, "Panorama General Acumulado")

        pdf.set_xy(15, content_y)
        pdf.set_font("Helvetica", "", 8)
        pdf.set_text_color(*self.COLOR_GRIS_TEXTO)
        pdf.multi_cell(180, 4,
                       self._safe(f"Periodo acumulado comparado: ano {current_year} "
                                  f"frente al ano {prev_year}."),
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
                            f"Total {prev_year}", f"{total_prev:,}", "Acumulado anual")
        self._draw_kpi_card(pdf, start_x + card_w + card_gap, content_y, card_w, card_h,
                            self.COLOR_AZUL,
                            f"Total {current_year}", f"{total_curr:,}", "Acumulado anual")
        self._draw_kpi_card(pdf, start_x + 2 * (card_w + card_gap), content_y, card_w, card_h,
                            var_color,
                            "Variacion acumulada", var_ytd_str, var_bg_note)
        content_y += card_h + 10

        # --- Seccion 3: Comportamiento por delito ---
        content_y = self._draw_section_title(pdf, content_y, 3,
                                             f"Top 5 Delitos - {current_year}")

        top5 = indicadores[:5]
        c5_w = 33
        c5_gap = 3
        c5_h = 28
        for i, item in enumerate(top5):
            cx = 15 + i * (c5_w + c5_gap)
            card_accent = self.COLOR_VERDE if item['diff'] < 0 else self.COLOR_ROJO if item['diff'] > 0 else self.COLOR_AZUL
            self._draw_kpi_card(pdf, cx, content_y, c5_w, c5_h,
                                card_accent,
                                item['name'][:18], f"{item['current']:,}",
                                f"vs {item['prev']} ({item['var']})\nCorte: {latest_date_str}")

        # Footer
        self._draw_footer(pdf, 1)

        # ===================== PAGINA 2 =====================
        pdf.add_page()

        # Mini header
        content_y = self._draw_header(pdf, large=False)

        # --- Seccion 4: Tabla comparativa acumulada ---
        content_y = self._draw_section_title(pdf, content_y, 4,
                                             "Comparativo Acumulado por Delito")

        headers_4 = ["Delito", "Ult. Dato", f"Acum. {prev_year}",
                     f"Acum. {current_year}", "Dif.", "Var. %"]
        widths_4 = [45, 25, 28, 28, 27, 27]
        col_aligns_4 = ['L', 'C', 'C', 'C', 'C', 'C']
        rows_4 = []
        for r in indicadores:
            diff_str = f"{'+' if r['diff'] > 0 else ''}{r['diff']}"
            rows_4.append([r['name'], latest_date_str, f"{r['prev']:,}", f"{r['current']:,}",
                           diff_str, r['var']])

        content_y = self._draw_table(pdf, content_y, headers_4, rows_4, widths_4, col_aligns_4)

        # --- Seccion 5: Grafica de evolucion ---
        content_y = self._draw_section_title(pdf, content_y, 5, "Evolucion Temporal del Delito")

        # Insertar grafica
        if chart_path and chart_path.exists():
            img_w = 170
            pdf.image(str(chart_path), x=15, y=content_y, w=img_w)
            content_y += 70

        # Caja explicativa
        content_y += 5
        expl = (f"Grafica comparativa: ano {current_year} (azul) vs ano "
                f"{prev_year} (gris). Datos del Observatorio del Delito Valle "
                f"para {self.municipio}.")
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
    from src.core.config import settings

    csv = settings.final_dir / "jamundi_analytics_master.csv"
    output = settings.final_dir / "boletin_semanal_jamundi.pdf"
    escudo = settings.base_dir / "assets" / "escudo_jamundi.png"

    if not csv.exists():
        print(f"ERROR: No se encontro el CSV en {csv}")
        exit(1)

    reporter = BoletinReporter(csv, output, escudo, municipio=settings.obs_municipio)
    result = reporter.create_boletin()
    print(f"Boletin generado: {result}")
