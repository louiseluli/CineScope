"""
CineScope Does the Dog Die? (DDD) API Client

This module provides a client for making authenticated requests to the
Does the Dog Die? API to fetch crowd-sourced content warnings.
"""
import requests
import logging
from typing import Optional, Dict, Any

from src.core.config import settings

logger = logging.getLogger(__name__)

class DDDClient:
    """
    A client for interacting with the Does the Dog Die? (DDD) API.
    """
    BASE_URL = "https://www.doesthedogdie.com/"

    def __init__(self, api_key: str = settings.DDD_API_KEY):
        """
        Initializes the DDDClient.

        Args:
            api_key (str): The DDD API key for authentication.
        
        Raises:
            ValueError: If the DDD_API_KEY is not provided in the .env file.
        """
        if not api_key:
            raise ValueError("Does the Dog Die? API key (DDD_API_KEY) is required.")
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "X-API-KEY": api_key
        })

    def get_ddd_info_by_imdb_id(self, imdb_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetches DDD information using a movie's IMDb ID.
        The DDD API first requires a search to get the internal ID, then a
        second call to get the media details.

        Args:
            imdb_id (str): The IMDb ID, including the 'tt' prefix.

        Returns:
            dict or None: A dictionary containing all topic stats (triggers),
                          or None if not found or an error occurs.
        """
        # Step 1: Search by IMDb ID to get the internal DDD ID
        search_url = f"{self.BASE_URL}dddsearch"
        try:
            search_response = self.session.get(search_url, params={"imdb": imdb_id.replace("tt", "")})
            search_response.raise_for_status()
            search_data = search_response.json()

            if not search_data or not search_data.get("items"):
                logger.debug(f"No DDD item found for IMDb ID: {imdb_id}")
                return None
            
            # Assuming the first result is the correct one
            ddd_internal_id = search_data["items"][0].get("id")
            if not ddd_internal_id:
                logger.warning(f"Found item for {imdb_id}, but it has no internal DDD ID.")
                return None

            # Step 2: Fetch the media details using the internal ID
            media_url = f"{self.BASE_URL}media/{ddd_internal_id}"
            media_response = self.session.get(media_url)
            media_response.raise_for_status()
            
            return media_response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"DDD API request failed for IMDb ID {imdb_id}: {e}")
            return None