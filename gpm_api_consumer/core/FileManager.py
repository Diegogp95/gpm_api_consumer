import os, sys
import logging
import json


class FileManager:
    base_data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
    base_sources_map_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'datasources_maps'))

    def __init__(self, prefix, data_path=None, sources_map_path=None):
        self.prefix = prefix
        self.data_path = data_path or os.path.join(self.base_data_path, prefix + '_data')
        self.sources_map_path = sources_map_path or os.path.join(self.base_sources_map_path, prefix + '_datasources_map')
        self._create_directories()

    def _create_directories(self):
        if not os.path.exists(self.data_path):
            if not self.data_path.startswith(self.base_data_path):
                raise ValueError(
                    f"Cannot create data directory outside of base path: {self.data_path}. Make the data_path {self.data_path} exists.")
            logging.info(f"Creating data directory: {self.data_path}")
            os.makedirs(self.data_path)
        if not os.path.exists(self.sources_map_path):
            if not self.sources_map_path.startswith(self.base_sources_map_path):
                raise ValueError(
                    f"Cannot create sources map directory outside of base path: {self.sources_map_path}. Make the sources_map_path {self.sources_map_path} exists.")
            logging.info(f"Creating sources map directory: {self.sources_map_path}")
            os.makedirs(self.sources_map_path)

    def save_data(self, filename, data):
        '''
        Saves data in json format to the specified file.
        '''
        file_path = os.path.join(self.data_path, filename)
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)
        logging.info(f"Saved data to {file_path}")
        return file_path

    def load_data(self, filename):
        file_path = os.path.join(self.data_path, filename)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File {file_path} not found.")
        with open(file_path, 'r') as file:
            data = json.load(file)
        logging.info(f"Loaded data from {file_path}")
        return data

    def save_sources_map(self, filename, data):
        '''
        Saves sources map in json format to the specified file.
        '''
        file_path = os.path.join(self.sources_map_path, filename)
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)
        logging.info(f"Saved sources map to {file_path}")
        return file_path

    def load_sources_map(self, filename):
        file_path = os.path.join(self.sources_map_path, filename)
        if not os.path.exists(file_path):
            logging.error(f"File {file_path} not found.")
            raise FileNotFoundError(f"File {file_path} not found.")
        with open(file_path, 'r') as file:
            data = json.load(file)
        logging.info(f"Loaded sources map from {file_path}")
        return data
