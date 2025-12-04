"""
CineScope Does the Dog Die? (DDD) API Client

This module provides a client for making authenticated requests to the
Does the Dog Die? API to fetch crowd-sourced content warnings about
sensitive topics in movies and TV shows.

API Reference:
- Search: GET /dddsearch?imdb={imdbID}
- Media: GET /media/{itemId}

Topics include: dogs dying, violence, gore, profanity, nudity, sexual content,
substance abuse, and many other sensitive subjects that viewers want to know about.
"""
import requests
import logging
from typing import Optional, Dict, Any, List
import json

from src.core.config import settings

logger = logging.getLogger(__name__)


class DDDClient:
    """
    A client for interacting with the Does the Dog Die? (DDD) API.
    
    Provides access to crowd-sourced content warnings and viewer discussions
    about sensitive topics in movies and TV shows.
    """
    BASE_URL = "https://www.doesthedogdie.com/"
    TIMEOUT = 10

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
        
        The DDD API requires two steps:
        1. Search by IMDb ID to get the internal DDD item ID
        2. Fetch media details and content warnings (topicItemStats)

        Args:
            imdb_id (str): The IMDb ID, including the 'tt' prefix (e.g., 'tt0111161')

        Returns:
            dict or None: A dictionary containing:
                - item: Basic item metadata
                - topicItemStats: List of content warning topics with community votes
                - summary: Parsed summary of main warnings
                Or None if not found or an error occurs
        """
        # Step 1: Search by IMDb ID to get the internal DDD ID
        ddd_internal_id = self._search_by_imdb_id(imdb_id)
        if not ddd_internal_id:
            return None
        
        # Step 2: Fetch the media details and parse content warnings
        return self._get_media_details(ddd_internal_id, imdb_id)

    def _search_by_imdb_id(self, imdb_id: str) -> Optional[int]:
        """
        Search DDD by IMDb ID to get the internal DDD item ID.
        
        Args:
            imdb_id (str): The IMDb ID including 'tt' prefix
            
        Returns:
            int or None: The internal DDD ID, or None if not found
        """
        search_url = f"{self.BASE_URL}dddsearch"
        
        # IMDb ID can be passed with or without 'tt' prefix
        imdb_clean = imdb_id.replace("tt", "") if imdb_id.startswith("tt") else imdb_id
        
        try:
            response = self.session.get(
                search_url,
                params={"imdb": imdb_clean},
                timeout=self.TIMEOUT
            )
            response.raise_for_status()
            search_data = response.json()

            if not search_data or not search_data.get("items"):
                logger.debug(f"No DDD item found for IMDb ID: {imdb_id}")
                return None
            
            # Use the first (most relevant) result
            ddd_internal_id = search_data["items"][0].get("id")
            if not ddd_internal_id:
                logger.warning(f"Found DDD item for {imdb_id}, but no internal ID.")
                return None
            
            logger.debug(f"Found DDD ID {ddd_internal_id} for IMDb {imdb_id}")
            return ddd_internal_id

        except requests.exceptions.Timeout:
            logger.error(f"DDD API request timed out for IMDb ID {imdb_id}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"DDD API search failed for IMDb ID {imdb_id}: {e}")
            return None

    def _get_media_details(self, ddd_id: int, imdb_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch media details and parse content warnings from topicItemStats.
        
        Args:
            ddd_id (int): The internal DDD item ID
            imdb_id (str): Original IMDb ID (for logging)
            
        Returns:
            dict or None: Enriched response with parsed warnings, or None on error
        """
        media_url = f"{self.BASE_URL}media/{ddd_id}"
        
        try:
            response = self.session.get(media_url, timeout=self.TIMEOUT)
            response.raise_for_status()
            data = response.json()
            
            # Parse the topicItemStats into a more usable format
            if data and 'topicItemStats' in data:
                data['parsed_warnings'] = self._parse_warnings(data['topicItemStats'])
                data['warning_summary'] = self._create_warning_summary(data['parsed_warnings'])
            
            return data

        except requests.exceptions.Timeout:
            logger.error(f"DDD API media request timed out for DDD ID {ddd_id} (IMDb {imdb_id})")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"DDD API media fetch failed for DDD ID {ddd_id}: {e}")
            return None

    def _parse_warnings(self, topic_items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """
        Parse topicItemStats into a structured warnings dictionary.
        
        Each topic gets: yes/no votes, top comment, consensus.
        
        Args:
            topic_items: List of topicItemStat objects from DDD API
            
        Returns:
            dict: Parsed warnings by topic name
        """
        warnings = {}
        
        for topic_item in topic_items:
            if not isinstance(topic_item, dict):
                continue
            
            topic = topic_item.get('topic', {})
            topic_name = topic.get('name', 'Unknown')
            
            # Parse votes
            yes_votes = topic_item.get('yesSum', 0)
            no_votes = topic_item.get('noSum', 0)
            total_votes = yes_votes + no_votes
            
            # Determine consensus
            if total_votes == 0:
                consensus = 'No votes'
                percentage = 0
            else:
                percentage = (yes_votes / total_votes) * 100
                if percentage >= 70:
                    consensus = 'Likely Yes'
                elif percentage >= 40:
                    consensus = 'Mixed'
                else:
                    consensus = 'Likely No'
            
            # Get top comment
            top_comment = topic_item.get('comment', '')
            
            warnings[topic_name] = {
                'yes_votes': yes_votes,
                'no_votes': no_votes,
                'total_votes': total_votes,
                'percentage': round(percentage, 1),
                'consensus': consensus,
                'top_comment': top_comment,
                'num_comments': topic_item.get('numComments', 0)
            }
        
        return warnings

    def _create_warning_summary(self, parsed_warnings: Dict[str, Dict[str, Any]]) -> str:
        """
        Create a human-readable summary of the main content warnings.
        
        Focuses on topics with strong consensus (>70% or <30% yes votes).
        
        Args:
            parsed_warnings: Parsed warnings dictionary
            
        Returns:
            str: Summary of main warnings
        """
        main_warnings = []
        
        for topic_name, warning_data in parsed_warnings.items():
            if warning_data['total_votes'] < 3:
                # Not enough votes to be significant
                continue
            
            if warning_data['percentage'] >= 70:
                main_warnings.append(f"⚠️ {topic_name}")
            elif warning_data['percentage'] <= 20:
                # Unlikely to have this issue
                pass
        
        if not main_warnings:
            return "No significant content warnings"
        
        return " | ".join(main_warnings[:5])  # Limit to top 5

    def get_raw_response(self, imdb_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the raw API response without parsing (for debugging).
        
        Args:
            imdb_id (str): IMDb ID including 'tt' prefix
            
        Returns:
            dict or None: Raw API response
        """
        ddd_id = self._search_by_imdb_id(imdb_id)
        if not ddd_id:
            return None
        
        media_url = f"{self.BASE_URL}media/{ddd_id}"
        try:
            response = self.session.get(media_url, timeout=self.TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to get raw response for {imdb_id}: {e}")
            return None
