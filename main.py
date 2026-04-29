import argparse
import asyncio
import sys
from src.core.logging_config import logger
from src.modes.capture import CaptureMode
from src.modes.extract import ExtractMode

async def main():
    parser = argparse.ArgumentParser(description="Reverse Engineering Extractor for Observatorio Valle")
    subparsers = parser.add_subparsers(dest="mode", help="Execution mode")

    # Capture Mode
    capture_parser = subparsers.add_parser("capture", help="Discover and capture internal requests")
    
    # Extract Mode
    extract_parser = subparsers.add_parser("extract", help="Replicate requests for mass extraction")
    extract_parser.add_argument("--municipio", default="Jamundí", help="Target municipality")
    extract_parser.add_argument("--full", action="store_true", help="Perform full historical extraction")

    args = parser.parse_args()

    if args.mode == "capture":
        mode = CaptureMode()
        await mode.run()
    elif args.mode == "extract":
        mode = ExtractMode(municipio=args.municipio, full=args.full)
        await mode.run()
    else:
        parser.print_help()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("Interrupted by user.")
    except Exception as e:
        logger.exception(f"Fatal error: {str(e)}")
        sys.exit(1)
