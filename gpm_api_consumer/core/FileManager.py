import os, sys
import logging
import json

logger = logging.getLogger(__name__)

class GPMFileManager:
    '''
    File Manager to handle data saving and loading for GPMConsumer.
    '''

    base_data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))

    def __init__(self, prefix, data_path=None):
        self.prefix = prefix
        self.data_path = data_path or os.path.join(self.base_data_path, prefix + '_data')
        self._create_directories()

    def _create_directories(self):
        if not os.path.exists(self.data_path):
            if not self.data_path.startswith(self.base_data_path):
                raise ValueError(
                    f"Cannot create data directory outside of base path: {self.data_path}. Make the data_path {self.data_path} exists.")
            logger.info(f"Creating default data directory: {self.data_path}")
            os.makedirs(self.data_path)

    def save_data(self, data, filename=None, full_path=None):
        '''
        Saves data in json format to the specified file.
        '''
        default_folder = self.data_path
        file_path = os.path.join(default_folder, filename) if not full_path else full_path
        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)
        logger.info(f"Saved data to {file_path}")
        return file_path

    def load_data(self, filename):
        file_path = os.path.join(self.data_path, filename)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File {file_path} not found.")
        with open(file_path, 'r') as file:
            data = json.load(file)
        logger.info(f"Loaded data from {file_path}")
        return data

