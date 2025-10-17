"""
CineScope TMDb API Client

This module provides a dedicated client for making authenticated requests
to The Movie Database (TMDb) API. It handles finding movies and TV shows
by IMDb ID and fetching their detailed information.
"""
import requests
import time
import logging
from typing import Optional, Dict, Any, Tuple

from src.core.config import settings

logger = logging.getLogger(__name__)

class TMDbClient:
    """
    A client for interacting with The Movie Database (TMDb) API.
    """
    BASE_URL = "https://api.themoviedb.org/3"

    def __init__(self, api_key: str = settings.TMDB_API_KEY):
        """
        Initializes the TMDbClient.

        Args:
            api_key (str): The TMDb API key for authentication.
        
        Raises:
            ValueError: If the API key is not provided.
        """
        if not api_key:
            raise ValueError("TMDb API key is required.")
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            "accept": "application/json",
            "Authorization": f"Bearer {settings.TMDB_READ_TOKEN}"
        })

    def _make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        Makes a request to the TMDb API and handles common errors.

        Args:
            endpoint (str): The API endpoint to request (e.g., "/find/...").
            params (dict, optional): A dictionary of query parameters.

        Returns:
            dict or None: The JSON response as a dictionary, or None if an error occurs.
        """
        if params is None:
            params = {}
        
        url = f"{self.BASE_URL}{endpoint}"
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                logger.warning("Rate limit exceeded. Waiting for 10 seconds.")
                time.sleep(10)
                return self._make_request(endpoint, params)
            logger.error(f"HTTP Error for {url}: {e}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
        return None

    # --- CHANGED: This method now finds movies OR TV shows. ---
    def find_by_imdb_id(self, imdb_id: str) -> Optional[Tuple[str, Dict[str, Any]]]:
        """
        Finds a movie or TV show on TMDb using its IMDb ID.

        Args:
            imdb_id (str): The IMDb ID (e.g., 'tt0111161').

        Returns:
            A tuple containing the media type ('movie' or 'tv') and a dictionary 
            of the basic TMDb info, or None if not found.
        """
        endpoint = f"/find/{imdb_id}"
        params = {"external_source": "imdb_id"}
        data = self._make_request(endpoint, params)

        if data:
            if data.get("movie_results"):
                return "movie", data["movie_results"][0]
            if data.get("tv_results"):
                return "tv", data["tv_results"][0]
        
        logger.debug(f"Media with IMDb ID '{imdb_id}' not found on TMDb.")
        return None

    def get_movie_details(self, tmdb_id: int) -> Optional[Dict[str, Any]]:
        """
        Fetches comprehensive details for a movie using its TMDb ID.

        Args:
            tmdb_id (int): The TMDb movie ID.

        Returns:
            dict or None: A dictionary of detailed movie information.
        """
        endpoint = f"/movie/{tmdb_id}"
        params = {"append_to_response": "credits,keywords,videos,release_dates"}
        return self._make_request(endpoint, params)

    # --- NEW: A dedicated method to get details for TV series. ---
    def get_tv_details(self, tmdb_id: int) -> Optional[Dict[str, Any]]:
        """
        Fetches comprehensive details for a TV series using its TMDb ID.

        Args:
            tmdb_id (int): The TMDb TV series ID.

        Returns:
            dict or None: A dictionary of detailed TV series information.
        """
        endpoint = f"/tv/{tmdb_id}"
        params = {"append_to_response": "credits,keywords,videos,content_ratings"}
        return self._make_request(endpoint, params)