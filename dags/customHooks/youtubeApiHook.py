#will only get the search results from youtube api

import requests
from airflow.hooks.base import BaseHook

class YoutubeApiHook(BaseHook):
    BASE_URL = "https://www.googleapis.com/youtube/v3"
    def __init__(self, youtube_conn_id="youtube_connection"):
        super().__init__()
        self.youtube_conn_id = youtube_conn_id

    def get_api_key(self):
        conn = self.get_connection(self.youtube_conn_id)
        return conn.password
    
    def _request(self, endpoint, params=None):
        api_key = self.get_api_key()
        if params is None:
            params = {}

        params['key'] = api_key
        url = f"{self.BASE_URL}/{endpoint}"
        response = requests.get(url, params = params)
        response.raise_for_status()
        return response.json()
    
    def search_videos(self, query, max_results=100, page_token=None):
        params = {
            'part':'snippet',
            'q': query,
            'type':'video',
            'maxResults':max_results
        }
        if page_token:
            params['pageToken'] = page_token

        return self._request('search', params)
    
    def get_categories(self, query, max_results=100, region_code='IN'):
        params = {
            'part':'snippet',
            'regionCode': region_code,
            'maxResults': max_results
        }
        return self._request('videoCategories', params)
    


