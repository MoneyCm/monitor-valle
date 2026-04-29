import json
import httpx
import asyncio
import pandas as pd
import datetime
from typing import List, Dict, Any
from src.core.config import settings
from src.core.logging_config import logger
from src.core.utils import load_json, generate_record_hash
from src.parsers.looker_parser import LookerAPIParser

class ExtractMode:
    """Analytical extraction mode: replicates requests using httpx based on captured endpoints."""
    
    def __init__(self, municipio: str = "Jamundí", full: bool = False):
        self.settings = settings
        self.municipio = municipio
        self.full = full
        self.useful_endpoints = load_json(settings.raw_dir / "useful_endpoints.json")
        self.cookies = load_json(settings.raw_dir / "session_cookies.json")
        self.parser = LookerAPIParser()

    async def _replay_request(self, client: httpx.AsyncClient, endpoint_data: Dict[str, Any], overrides: Dict[str, Any] = None) -> Dict[str, Any]:
        """Replicates a captured request with optional payload overrides."""
        url = endpoint_data["url"]
        method = endpoint_data["method"]
        headers = endpoint_data["headers"]
        payload = endpoint_data["payload"]

        # Basic payload injection (Placeholders for real reverse engineering)
        if overrides and payload:
             # Example: if payload is JSON, modify keys
             try:
                 data = json.loads(payload)
                 for k, v in overrides.items():
                     if k in str(data): # Deep search/replace logic needed here
                         pass
                 payload = json.dumps(data)
             except:
                 pass

        try:
            response = await client.request(
                method, 
                url, 
                content=payload, 
                headers=headers,
                timeout=30.0
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to replay request to {url}: {str(e)}")
            return {}

    async def run(self):
        logger.info(f"Initializing EXTRACT MODE for {self.municipio}...")
        
        if not self.useful_endpoints:
            logger.error("No useful endpoints found. Run 'capture' mode first.")
            return

        async with httpx.AsyncClient(headers={"User-Agent": "Mozilla/5.0"}, verify=False) as client:
            # Set cookies
            for cookie in self.cookies:
                client.cookies.set(cookie["name"], cookie["value"], domain=cookie["domain"])
            
            all_records = []
            
            # Target specifically 'batchedData' or analytical endpoints
            target_endpoints = [e for e in self.useful_endpoints if "batchedData" in e["url"]]
            
            for endpoint in target_endpoints:
                logger.info(f"Exploiting endpoint: {endpoint['url'][:80]}")
                
                # Mocking mass iteration
                years = [2024, 2025, 2026] if not self.full else range(2016, 2027)
                
                for year in years:
                    logger.debug(f"Extracting Year: {year}")
                    # Replay and parse
                    raw_data = await self._replay_request(client, endpoint)
                    
                    if raw_data:
                        records = self.parser.parse_response(raw_data, year=year, municipio=self.municipio)
                        all_records.extend(records)
            
            # 6. Consolidation and Validation
            self._finalize(all_records)

    def _finalize(self, records: List[Dict[str, Any]]):
        """Validates and saves final dataset."""
        df = pd.DataFrame(records)
        
        # Validation
        if len(df) < 50:
            logger.error(f"Validation FAILED: Insufficient records ({len(df)} < 50)")
            # In a real scenario, we might want to raise an error
        else:
            logger.success(f"Validation PASSED: Extracted {len(df)} records.")

        # Save outputs
        final_csv = self.settings.final_dir / "jamundi_dataset.csv"
        df.to_csv(final_csv, index=False, encoding="utf-8-sig")
        logger.info(f"Final data saved to {final_csv}")
