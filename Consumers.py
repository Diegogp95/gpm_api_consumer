from Client import APIClient
from ConfigManager import ConfigManager
import requests


class GPMConsumer:
    '''
    API Consumer for GPM (Green Power Monitor) API.
    '''

    datalistv2Keys = {
        # query parameters for datalistv2 endpoint
        'datasourceIds': list,
        'grouping': str,
        'granularity': int,
        'aggregation': int,
    }

    def __init__(self, prefix='gpm'):
        self.config_manager = ConfigManager(
            config_path=f'{prefix}_config.json',
            env_path=f'{prefix}.env',
            config_keys=GPMConsumer.datalistv2Keys
        )
        self.client = APIClient(self.config_manager._env['API_BASE_URL'])

    def get(self, endpoint, params=None):
        '''
        Get data from the GPM API.
        '''
        token = self.config_manager.get('api_token')
        headers = { 'Authorization': f'Bearer {token}' }
        response = self.client.get(endpoint, headers=headers, params=params)
        return response

    def post(self, endpoint, data=None):
        '''
        Post data to the GPM API.
        '''
        token = self.config_manager.get('api_token')
        headers = { 'Authorization': f'Bearer {token}' }
        response = self.client.post(endpoint, json=data, headers=headers)
        return response

    def login(self):
        '''
        Login to the API and get a token.
        '''
        username = self.config_manager._env['API_USERNAME']
        password = self.config_manager._env['API_PASSWORD']
        data = { 'username': username, 'password': password }
        response = self.post('/api/Account/Token', data=data)
        if 'AccessToken' in response:
            self.config_manager.set('api_token', response['AccessToken'])
            return response['AccessToken']
        else:
            raise Exception("Failed to login and get token.")

    def ping(self):
        '''
        Check if the API is reachable and the token is valid.
        '''
        return self.get('/api/Account/Ping')

    def datalistv2(self, params=None):
        '''
        Get the list of data from the API.
        '''
        return self.get('/api/DataList/v2', params=params)

    def plant(self, plant_id=None, params=None):
        '''
        Get the plants data from the API. Or get a specific plant by ID.
        '''
        return (self.get(f'/api/Plant/{plant_id}', params=params) if
                plant_id else self.get('/api/Plant', params=params))

    def element(self, plant_id, element_id=None, params=None):
        '''
        Get the elements data for a specific plant. Or get a specific element by ID.
        '''
        return (self.get(f'/api/Plant/{plant_id}/Element/{element_id}', params=params) if
                element_id else self.get(f'/api/Plant/{plant_id}/Element', params=params))

    def datasources(self, plant_id, element_id=None, params=None):
        '''
        Get the data source for an element or plant.
        '''
        return (self.get(f'/api/Plant/{plant_id}/Element/{element_id}/Datasource', params=params) if
                element_id else self.get(f'/api/Plant/{plant_id}/Datasource', params=params))
