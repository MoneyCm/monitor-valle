"""
Analizador de datos estadisticos del boletin municipal.
Extrae, normaliza y compara indicadores delictivos entre periodos.
"""
import datetime
from pathlib import Path

import pandas as pd


class DataAnalyzer:
    """Analiza el CSV maestro para producir indicadores comparativos del boletin."""

    FUENTE_OBSERVATORIO = "Observatorio del Delito Valle (www.observatoriodeldelitovalle.co)"

    DELITOS = [
        "Homicidio", "Lesiones Personales", "Hurto Personas", "Hurto Motocicletas",
        "Hurto Automotores", "Extorsión", "Violencia Intrafamiliar",
        "Hurto Entidades Comerciales", "Hurto Residencias",
        "Acceso Carnal O Acto Sexual Violento",
        "Actos Sexuales Con Menor De 14 Años",
        "Secuestro",
    ]

    def __init__(self, csv_path, municipio="Jamundi"):
        self.csv_path = Path(csv_path)
        self.municipio = municipio
        self.current_year = None
        self.prev_year = None
        self.latest_date_str = ""
        self.corte_month = None
        self.corte_month_name = None

    def load_data(self):
        """Carga y limpia el CSV maestro."""
        df = pd.read_csv(self.csv_path, encoding="utf-8-sig")
        df['col_1'] = pd.to_numeric(df['col_1'], errors='coerce').fillna(0)
        df['col_9'] = df['col_9'].astype(str).str.strip()
        return df

    def detect_years(self, df):
        """Detecta el ano mas reciente y el anterior."""
        years = sorted(df['col_9'].unique(), reverse=True)
        years = [y for y in years if y.isdigit()]
        self.current_year = years[0] if years else str(datetime.datetime.now().year)
        self.prev_year = years[1] if len(years) > 1 else str(int(self.current_year) - 1)
        return years

    def detect_corte_month(self, df):
        """Detecta el ultimo mes con datos mensuales en el ano actual.

        El Observatorio entrega filas donde col_0 es un numero 1-12 (mes)
        cuando is_compare=True, compare_index=1. Esas son las sumas mensuales.
        """
        monthly = df[
            (df['col_9'] == str(self.current_year)) &
            (df['is_compare'] == True) &
            (df['compare_index'] == 1) &
            (df['col_0'].astype(str).str.match(r'^\d+$', na=False))
        ].copy()
        if monthly.empty:
            # Fallback: intentar detectar el mes a partir de las fechas YYYY-MM-DD en col_0
            import re
            dates = df[
                (df['col_9'] == str(self.current_year)) & 
                (df['col_0'].astype(str).str.match(r'^\d{4}-\d{2}-\d{2}$', na=False))
            ]['col_0'].unique()
            
            parsed_dates = []
            for d in dates:
                try:
                    parsed_dates.append(datetime.datetime.strptime(str(d), "%Y-%m-%d").date())
                except Exception:
                    pass
            
            if parsed_dates:
                max_date = max(parsed_dates)
                self.latest_date_str = max_date.strftime("%d/%m/%Y")
                # Si el dia del mes es menor a 15, consideramos que el mes esta incompleto
                # y retrocedemos al mes anterior para que la comparacion sea justa
                if max_date.day < 15:
                    self.corte_month = max_date.month - 1 if max_date.month > 1 else 12
                else:
                    self.corte_month = max_date.month
                self.corte_month_name = self._month_name(self.corte_month)
                return self.corte_month

            # Fallback secundario: usar el mes actual del sistema
            self.corte_month = datetime.datetime.now().month
            self.corte_month_name = self._month_name(self.corte_month)
            self.latest_date_str = datetime.datetime.now().strftime("%d/%m/%Y")
            return self.corte_month

        months = pd.to_numeric(monthly['col_0'], errors='coerce').dropna().astype(int)
        self.corte_month = int(months.max())
        self.corte_month_name = self._month_name(self.corte_month)
        import calendar
        last_day = calendar.monthrange(int(self.current_year), self.corte_month)[1]
        self.latest_date_str = f"{last_day:02d}/{self.corte_month:02d}/{self.current_year}"
        return self.corte_month

    @staticmethod
    def _month_name(n):
        """Convierte numero de mes a abreviatura en espanol."""
        meses = {1: 'Ene', 2: 'Feb', 3: 'Mar', 4: 'Abr', 5: 'May', 6: 'Jun',
                 7: 'Jul', 8: 'Ago', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dic'}
        return meses.get(n, f'Mes {n}')

    def get_crime_ytd(self, df, delito, year, corte_month):
        """Obtiene el acumulado YTD de un delito (enero hasta corte_month).

        Si hay datos mensuales agregados (is_compare=True, compare_index=1),
        proporciona el YTD escalando el total anual del delito por la proporcion
        de casos mensuales que ocurrieron hasta corte_month.
        De lo contrario, utiliza el total anual del delito.
        """
        crime_total = self.get_crime_total(df, delito, year)
        
        monthly = df[
            (df['col_9'] == str(year)) &
            (df['is_compare'] == True) &
            (df['compare_index'] == 1) &
            (df['col_0'].astype(str).str.match(r'^\d+$', na=False))
        ].copy()
        monthly['mes'] = pd.to_numeric(monthly['col_0'], errors='coerce')

        if monthly.empty:
            return crime_total

        total_ytd_all = int(monthly[(monthly['mes'] >= 1) & (monthly['mes'] <= corte_month)]['col_1'].sum())
        total_full_all = int(monthly['col_1'].sum())
        
        if total_full_all > 0:
            ratio = total_ytd_all / total_full_all
            return int(round(crime_total * ratio))
        else:
            return crime_total

    def get_crime_total(self, df, delito, year):
        """Obtiene el total anual de un delito (fallback sin datos mensuales)."""
        mask = (df['col_0'].str.contains(delito, case=False, na=False)) & \
               (df['col_9'] == str(year)) & \
               (df['is_compare'] == False)
        rows = df[mask]
        if rows.empty:
            return 0
        return int(rows['col_1'].max())

    def extract_indicadores(self, df):
        """Compara cada delito: YTD ano actual vs YTD mismo periodo ano anterior."""
        results = []
        for delito in self.DELITOS:
            v_curr = self.get_crime_ytd(df, delito, self.current_year, self.corte_month)
            v_prev = self.get_crime_ytd(df, delito, self.prev_year, self.corte_month)
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

    def calc_totals(self, indicadores):
        """Suma los totales de todos los delitos por ano."""
        total_curr = sum(r['current'] for r in indicadores)
        total_prev = sum(r['prev'] for r in indicadores)
        return total_curr, total_prev
