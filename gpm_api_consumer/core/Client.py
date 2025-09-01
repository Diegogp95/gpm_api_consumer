import json, os, requests


class APIClient:
    def __init__(self, base_url):
        self.base_url = base_url

    def get(self, endpoint, headers=None, params=None):
        response = requests.get(f"{self.base_url}{endpoint}",
                                    headers=headers, params=params)

        response.raise_for_status()
        try:
            # Attempt to parse the response as JSON
            return response.json()
        except json.JSONDecodeError:
            # May be not content
            return None

    def post(self, endpoint, json=None, headers=None):
        response = requests.post(f"{self.base_url}{endpoint}",
                                    json=json, headers=headers)
        response.raise_for_status()
        try:
            # Attempt to parse the response as JSON
            return response.json()
        except json.JSONDecodeError:
            # May be not content
            return None

    def __str__(self):
        return f"APIClient with base URL: {self.base_url}"
