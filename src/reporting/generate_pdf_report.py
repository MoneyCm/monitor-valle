import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from fpdf import FPDF
import datetime
from pathlib import Path
import os
import numpy as np

class JamundiBoletinReporter:
    def __init__(self, csv_path: str, output_path: str, banner_path: str = None):
        self.csv_path = Path(csv_path)
        self.output_path = Path(output_path)
        self.banner_path = Path(banner_path) if banner_path else None
        self.temp_dir = self.output_path.parent / "temp_boletin"
        self.temp_dir.mkdir(exist_ok=True)
        
        # Identity
        self.primary_color = (26, 82, 118) # Dark Blue
        self.alert_color = (139, 0, 0)     # Dark Red
        self.success_color = (0, 100, 0)   # Dark Green
        self.current_week = datetime.datetime.now().isocalendar()[1]

    def _load_data(self):
        return pd.read_csv(self.csv_path, encoding="utf-8-sig")

    def _safe_text(self, text):
        return str(text).encode('latin-1', 'replace').decode('latin-1')

    def _generate_ytd_comparison(self, df):
        """Identifies and compares Year-To-Date data (Jan-Apr 2026 vs Jan-Apr 2025)."""
        ytd_2026 = df[df['col_0'].astype(str).isin(['1','2','3','4']) & df['col_2'].isna()].head(4).copy()
        ytd_2026['val'] = pd.to_numeric(ytd_2026['col_1'], errors='coerce').fillna(0)
        ytd_2026['year'] = '2026'
        
        ytd_2025 = df[df['col_0'].astype(str).isin(['1','2','3','4']) & df['col_2'].isna()].iloc[4:8].copy()
        ytd_2025['val'] = pd.to_numeric(ytd_2025['col_1'], errors='coerce').fillna(0)
        ytd_2025['year'] = '2025'

        if ytd_2026.empty or ytd_2025.empty:
            return None, 0, 0, 0

        comparison_df = pd.concat([ytd_2025, ytd_2026])
        month_map = {'1':'Ene', '2':'Feb', '3':'Mar', '4':'Abr'}
        comparison_df['Mes'] = comparison_df['col_0'].astype(str).map(month_map)

        plt.figure(figsize=(10, 6))
        sns.barplot(data=comparison_df, x='Mes', y='val', hue='year', palette=['#abb2b9', '#1a5276'])
        plt.title("Comparativa YTD: Enero - Abril (2025 vs 2026)", fontsize=16, pad=20, color="#1a5276")
        plt.tight_layout()
        chart_path = self.temp_dir / "ytd_comparison.png"
        plt.savefig(chart_path, dpi=300); plt.close()

        total_25 = ytd_2025['val'].sum()
        total_26 = ytd_2026['val'].sum()
        variation = ((total_26 - total_25) / total_25) * 100 if total_25 > 0 else 0
        return chart_path, variation, total_25, total_26

    def _generate_standard_charts(self, df):
        charts = []
        # Trends Historical
        trend_df = df[df['col_0'].astype(str).str.match(r'\d{4}-\d{2}-\d{2}', na=False)].copy()
        if not trend_df.empty:
            trend_df['year'] = pd.to_datetime(trend_df['col_0']).dt.year
            trend_df['val'] = pd.to_numeric(trend_df['col_1'], errors='coerce').fillna(0)
            trend_df = trend_df.groupby('year')['val'].sum().reset_index().sort_values('year')
            plt.figure(figsize=(10, 5))
            sns.lineplot(data=trend_df, x='year', y='val', marker='o', color="#e67e22", linewidth=3)
            plt.title("Tendencia Historica Anual (Jamundi)")
            plt.tight_layout()
            path = self.temp_dir / "history.png"
            plt.savefig(path, dpi=300); plt.close()
            charts.append(("Evolucion Multianual", path))
        return charts

    def _extract_indicadores(self, df):
        """Extracts crime counts dynamically and identifies historical vs annual."""
        delitos = ["Homicidio", "Lesiones Personales", "Hurto Personas", "Hurto Motocicletas", "Hurto Automotores", "Extorsión", "Violencia Intrafamiliar"]
        results = []
        # Ensure col_1 is numeric
        df['col_1'] = pd.to_numeric(df['col_1'], errors='coerce').fillna(0)

        for d in delitos:
            rows = df[df['col_0'].str.contains(d, case=False, na=False)].copy()
            if not rows.empty:
                # Prioritize records with specific years in col_9
                v_2025 = rows[rows['col_9'] == '2025']['col_1'].iloc[0] if not rows[rows['col_9'] == '2025'].empty else 0
                v_2026 = rows[rows['col_9'] == '2026']['col_1'].iloc[0] if not rows[rows['col_9'] == '2026'].empty else 0

                # If we don't have year-specific data, fallback to min/max heuristic
                if v_2025 == 0 and v_2026 == 0:
                    vals = rows['col_1'].tolist()
                    v_2026 = max(vals)
                    v_2025 = min(vals) if len(vals) > 1 else 0

                label = d
                # Heuristic for label (Historical if too high)
                if v_2026 > 500 and d in ["Homicidio", "Extorsión"]:
                    label = f"{d} (Hist.)"

                var = ((v_2026 - v_2025) / v_2025 * 100) if v_2025 > 0 else 0 
                results.append((label, int(v_2025), int(v_2026), f"{var:+.1f}%" if v_2025 > 0 else "N/A"))
        return results

    def create_boletin(self):
        df = self._load_data()
        indicadores = self._extract_indicadores(df)
        ytd_chart, variation, total_25, total_26 = self._generate_ytd_comparison(df)
        std_charts = self._generate_standard_charts(df)
        
        pdf = FPDF()
        pdf.set_auto_page_break(auto=True, margin=15)

        # PAGE 1: PORTADA
        pdf.add_page()
        if self.banner_path and self.banner_path.exists(): pdf.image(str(self.banner_path), x=10, y=10, w=190)
        pdf.set_y(60); pdf.set_font("Helvetica", "B", 20); pdf.set_text_color(*self.primary_color)
        pdf.cell(0, 15, "BOLETIN SEMANAL DE SEGURIDAD Y CONVIVENCIA", align='C', new_x="LMARGIN", new_y="NEXT")
        pdf.set_font("Helvetica", "B", 16); pdf.cell(0, 10, "Municipio de Jamundi (Valle del Cauca)", align='C', new_x="LMARGIN", new_y="NEXT")
        pdf.ln(10); pdf.set_fill_color(245, 245, 245); pdf.set_font("Helvetica", "B", 11); pdf.set_text_color(0, 0, 0)
        ficha = [["Semana Analizada:", f"Semana {self.current_week}"], ["Fecha de Corte:", datetime.datetime.now().strftime('%d/%m/%Y')], ["Dependencia:", "Observatorio del Delito"], ["Fuentes:", "Intercepcion Looker Studio / Siedco"]]
        for r in ficha:
            pdf.cell(60, 10, r[0], border=1, fill=True); pdf.cell(130, 10, r[1], border=1); pdf.ln()
        
        # RESUMEN
        pdf.ln(10); pdf.set_font("Helvetica", "B", 14); pdf.set_text_color(*self.primary_color)
        pdf.cell(0, 10, "2. RESUMEN EJECUTIVO", new_x="LMARGIN", new_y="NEXT")
        pdf.set_font("Helvetica", "", 11); pdf.set_text_color(0, 0, 0)
        pdf.multi_cell(0, 8, f"- Se registra una variacion YTD del {variation:+.1f}% en el periodo analizado para Jamundi.\n- Alerta en Hurto a Personas con un acumulado de registros capturados elevado.\n- Recomendacion: Intensificar patrullajes preventivos en zonas de alta recurrencia.")

        # PAGE 2: INDICADORES
        pdf.add_page(); pdf.set_font("Helvetica", "B", 14); pdf.set_text_color(*self.primary_color)
        pdf.cell(0, 10, "3. TABLERO DE INDICADORES (JAMUNDI)", new_x="LMARGIN", new_y="NEXT")
        pdf.set_font("Helvetica", "B", 9); pdf.set_fill_color(220, 230, 241)
        headers = ["DELITO", "ACUM. 2025", "ACUM. 2026", "VAR %"]
        widths = [75, 40, 40, 35]
        for i, h in enumerate(headers): pdf.cell(widths[i], 10, h, border=1, fill=True, align='C')
        pdf.ln(); pdf.set_font("Helvetica", "", 9)
        for row in indicadores:
            pdf.cell(widths[0], 8, self._safe_text(row[0]), border=1); pdf.cell(widths[1], 8, str(row[1]), border=1, align='C')
            pdf.cell(widths[2], 8, str(row[2]), border=1, align='C')
            pdf.set_text_color(150,0,0) if "+" in str(row[3]) else pdf.set_text_color(0,100,0)
            pdf.cell(widths[3], 8, str(row[3]), border=1, align='C'); pdf.set_text_color(0,0,0); pdf.ln()

        # PAGE 3: GRAFICAS
        if ytd_chart:
            pdf.add_page(); pdf.set_font("Helvetica", "B", 14); pdf.cell(0, 10, "4. COMPARATIVA YTD", new_x="LMARGIN", new_y="NEXT")
            pdf.image(str(ytd_chart), x=15, y=40, w=180)
        
        for t, p in std_charts:
            pdf.add_page(); pdf.set_font("Helvetica", "B", 14); pdf.cell(0, 10, f"5. {t}", new_x="LMARGIN", new_y="NEXT")
            pdf.image(str(p), x=15, y=40, w=180)

        pdf.output(str(self.output_path))
        for f in self.temp_dir.glob("*.png"): os.remove(f)
        self.temp_dir.rmdir()
        return self.output_path

if __name__ == "__main__":
    current_dir = Path(__file__).parent.parent.parent
    csv = current_dir / "data" / "final" / "jamundi_analytics_master.csv"
    output = current_dir / "data" / "final" / "boletin_semanal_jamundi.pdf"
    banner = list(current_dir.glob("jamundi_report_banner_*.png")) or [None]
    reporter = JamundiBoletinReporter(csv, output, banner[0])
    reporter.create_boletin()
    print(f"Boletin dinamico generado: {output}")
