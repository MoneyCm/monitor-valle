import argparse
import asyncio
import sys
from src.core.logging_config import logger
from src.modes.capture import CaptureMode

async def main():
    parser = argparse.ArgumentParser(description="Reverse Engineering Extractor for Observatorio Valle")
    subparsers = parser.add_subparsers(dest="mode", help="Modo de ejecucion")

    # Modo captura
    capture_parser = subparsers.add_parser("capture", help="Descubrir y capturar peticiones internas")

    args = parser.parse_args()

    if args.mode == "capture":
        mode = CaptureMode()
        await mode.run()
    else:
        parser.print_help()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("Interrumpido por el usuario.")
    except Exception as e:
        logger.exception(f"Error fatal: {str(e)}")
        sys.exit(1)
