"""
CineScope OMDb API Client

This module provides a client for making authenticated requests to the
OMDb API to fetch supplementary movie and series data.
"""
import requests
import logging
from typing import Optional, Dict, Any

from src.core.config import settings

logger = logging.getLogger(__name__)

class OMDbClient:
    """
    A client for interacting with the OMDb API.
    """
    BASE_URL = "http://www.omdbapi.com/"

    def __init__(self, api_key: str = settings.OMDB_API_KEY):
        """
        Initializes the OMDbClient.

        Args:
            api_key (str): The OMDb API key.
        
        Raises:
            ValueError: If the OMDb API key is not provided.
        """
        if not api_key:
            raise ValueError("OMDb API key is required for the OMDbClient.")
        self.api_key = api_key
        self.session = requests.Session()

    def get_details_by_imdb_id(self, imdb_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetches movie or series details from OMDb using its IMDb ID.

        Args:
            imdb_id (str): The IMDb ID (e.g., 'tt0111161').

        Returns:
            dict or None: A dictionary of details, or None if an error occurs or
                          the item is not found.
        """
        params = {
            "i": imdb_id,
            "apikey": self.api_key,
            "plot": "full",  # Get the full plot details
            "r": "json"
        }

        try:
            response = self.session.get(self.BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()

            if data.get("Response") == "True":
                return data
            else:
                # OMDb returns a 200 OK even for "Movie not found."
                logger.warning(f"OMDb API did not find a match for {imdb_id}. Reason: {data.get('Error')}")
                return None

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for OMDb API for IMDb ID {imdb_id}: {e}")
            return None