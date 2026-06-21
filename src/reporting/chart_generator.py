"""
Generador de graficas comparativas de delitos para el boletin municipal.
"""
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from pathlib import Path


class ChartGenerator:
    """Genera graficas de barras agrupadas comparando anos."""

    @staticmethod
    def generate(indicadores, current_year, prev_year, output_dir) -> Path:
        """Genera una grafica de barras agrupadas: ano actual vs anterior (top 8 delitos).

        Args:
            indicadores: Lista de dicts con 'name', 'current', 'prev'.
            current_year: Ano actual (str).
            prev_year: Ano anterior (str).
            output_dir: Directorio donde guardar la imagen.

        Returns:
            Path de la imagen generada.
        """
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
               label=f"{prev_year}", color='#A0AEC0')
        ax.bar([i + bw / 2 for i in x], current, bw,
               label=f"{current_year}", color='#0033A0')

        ax.set_xticks(list(x))
        ax.set_xticklabels(names, rotation=25, ha='right', fontsize=8, color='#555')
        ax.set_ylabel('Hechos delictivos', fontsize=9, color='#666')
        ax.legend(fontsize=9, frameon=False)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.tick_params(colors='#888')
        ax.grid(axis='y', linestyle='--', alpha=0.25)
        plt.tight_layout()

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / "evolucion.png"
        plt.savefig(path, dpi=200, bbox_inches='tight')
        plt.close()
        return path
