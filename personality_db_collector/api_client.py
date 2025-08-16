"""API client for personality-database.com"""

import requests
import time
import logging
from typing import Dict, List, Optional, Any, Iterator
from urllib.parse import urljoin
from datetime import datetime

from .data_models import PersonData, RatingData, ProfileData, CategoryData, to_dict


logger = logging.getLogger(__name__)


class PersonalityDBClient:
    """Client for accessing the personality-database.com API."""
    
    BASE_URL = "https://api.personality-database.com/api/v1/"
    
    def __init__(self, rate_limit_delay: float = 0.12):  # ~500 requests/min = 0.12s delay
        """Initialize the API client.
        
        Args:
            rate_limit_delay: Delay between requests in seconds to respect rate limits
        """
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Socionics-Research/1.0 (Educational Research)',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate',
        })
        self.rate_limit_delay = rate_limit_delay
        self.last_request_time = 0.0
        
    def _make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make a rate-limited request to the API.
        
        Args:
            endpoint: API endpoint path
            params: Query parameters
            
        Returns:
            JSON response data
            
        Raises:
            requests.RequestException: If request fails
        """
        # Rate limiting
        now = time.time()
        time_since_last = now - self.last_request_time
        if time_since_last < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - time_since_last)
        
        url = urljoin(self.BASE_URL, endpoint)
        
        try:
            logger.debug(f"Making request to {url} with params {params}")
            response = self.session.get(url, params=params)
            response.raise_for_status()
            self.last_request_time = time.time()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
            raise
    
    def get_profiles(self, offset: int = 0, limit: int = 100, 
                    cid: Optional[int] = None, pid: Optional[int] = None,
                    cat_id: Optional[int] = None, property_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get profile data from the profiles endpoint.
        
        Args:
            offset: Starting offset for pagination
            limit: Number of results per page (max 100)
            cid: Category ID filter
            pid: Person ID filter  
            cat_id: Category ID filter (alternate parameter name)
            property_id: Property ID filter
            
        Returns:
            List of profile data dictionaries
        """
        params = {
            'offset': offset,
            'limit': min(limit, 100)  # API limit
        }
        
        # Add optional filters
        if cid is not None:
            params['cid'] = cid
        if pid is not None:
            params['pid'] = pid
        if cat_id is not None:
            params['cat_id'] = cat_id
        if property_id is not None:
            params['property_id'] = property_id
            
        response_data = self._make_request('profiles', params)
        
        # Handle different response formats
        if isinstance(response_data, dict):
            if 'data' in response_data:
                return response_data['data']
            elif 'results' in response_data:
                return response_data['results']
            elif 'profiles' in response_data:
                return response_data['profiles']
            else:
                # If it's a single profile wrapped in dict
                return [response_data]
        elif isinstance(response_data, list):
            return response_data
        else:
            logger.warning(f"Unexpected response format: {type(response_data)}")
            return []
    
    def get_all_profiles(self, batch_size: int = 100, max_profiles: Optional[int] = None,
                        **filters) -> Iterator[List[Dict[str, Any]]]:
        """Get all profiles with pagination.
        
        Args:
            batch_size: Number of profiles per request
            max_profiles: Maximum total profiles to fetch (None for unlimited)
            **filters: Additional filters to pass to get_profiles
            
        Yields:
            Batches of profile data
        """
        offset = 0
        total_fetched = 0
        
        while True:
            # Calculate batch size for this request
            if max_profiles is not None:
                remaining = max_profiles - total_fetched
                if remaining <= 0:
                    break
                current_batch_size = min(batch_size, remaining)
            else:
                current_batch_size = batch_size
            
            try:
                batch = self.get_profiles(offset=offset, limit=current_batch_size, **filters)
                
                if not batch:
                    logger.info("No more profiles found, stopping pagination")
                    break
                
                yield batch
                total_fetched += len(batch)
                offset += len(batch)
                
                logger.info(f"Fetched {len(batch)} profiles (total: {total_fetched}, offset: {offset})")
                
                # If we got fewer results than requested, we've reached the end
                if len(batch) < current_batch_size:
                    logger.info("Received partial batch, assuming end of data")
                    break
                    
            except Exception as e:
                logger.error(f"Error fetching batch at offset {offset}: {e}")
                break
    
    def get_categories(self) -> List[Dict[str, Any]]:
        """Get all available categories."""
        response_data = self._make_request('categories')
        
        if isinstance(response_data, dict):
            if 'data' in response_data:
                return response_data['data']
            elif 'categories' in response_data:
                return response_data['categories']
            else:
                return [response_data]
        elif isinstance(response_data, list):
            return response_data
        else:
            return []
    
    def get_person(self, person_id: int) -> Optional[Dict[str, Any]]:
        """Get data for a specific person by ID."""
        try:
            response_data = self._make_request(f'person/{person_id}')
            return response_data
        except requests.RequestException:
            return None
    
    def get_ratings(self, person_id: int) -> List[Dict[str, Any]]:
        """Get all personality ratings for a specific person."""
        try:
            response_data = self._make_request(f'person/{person_id}/ratings')
            
            if isinstance(response_data, dict):
                if 'data' in response_data:
                    return response_data['data']
                elif 'ratings' in response_data:
                    return response_data['ratings']
            elif isinstance(response_data, list):
                return response_data
                
            return []
        except requests.RequestException:
            return []
    
    def convert_raw_to_person_data(self, raw_data: Dict[str, Any]) -> PersonData:
        """Convert raw API response to PersonData object."""
        return PersonData(
            id=raw_data.get('id', 0),
            name=raw_data.get('name', ''),
            description=raw_data.get('description'),
            image_url=raw_data.get('image_url') or raw_data.get('image'),
            category_id=raw_data.get('category_id') or raw_data.get('cat_id'),
            category_name=raw_data.get('category_name') or raw_data.get('category'),
            subcategory_id=raw_data.get('subcategory_id'),
            subcategory_name=raw_data.get('subcategory_name') or raw_data.get('subcategory'),
            created_at=self._parse_datetime(raw_data.get('created_at')),
            updated_at=self._parse_datetime(raw_data.get('updated_at')),
            metadata=raw_data.copy()
        )
    
    def convert_raw_to_rating_data(self, raw_data: Dict[str, Any], person_id: int) -> RatingData:
        """Convert raw API response to RatingData object."""
        return RatingData(
            id=raw_data.get('id'),
            person_id=person_id,
            personality_type=raw_data.get('personality_type', ''),
            personality_system=raw_data.get('personality_system', ''),
            rating_value=raw_data.get('rating_value') or raw_data.get('value'),
            confidence=raw_data.get('confidence'),
            votes_count=raw_data.get('votes_count') or raw_data.get('votes'),
            created_at=self._parse_datetime(raw_data.get('created_at')),
            updated_at=self._parse_datetime(raw_data.get('updated_at')),
            metadata=raw_data.copy()
        )
    
    @staticmethod
    def _parse_datetime(date_str: Optional[str]) -> Optional[datetime]:
        """Parse datetime string from API response."""
        if not date_str:
            return None
        
        # Try common datetime formats
        formats = [
            '%Y-%m-%dT%H:%M:%S.%fZ',
            '%Y-%m-%dT%H:%M:%SZ', 
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d'
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        logger.warning(f"Could not parse datetime: {date_str}")
        return None