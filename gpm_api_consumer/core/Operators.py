from gpm_api_consumer.core.Consumers import GPMConsumer
from gpm_api_consumer.core.Interpreters import GPMInterpreter
from gpm_api_consumer.core.FileManager import FileManager
from requests.exceptions import HTTPError
import logging
from typing import List, Dict, Union
import re
from gpm_api_consumer.core import exceptions as ex
from gpm_api_consumer.utils.utils import chunked_iterable, set_logger_level
from gpm_api_consumer.utils.decorators import handle_authentication
import os
logger = logging.getLogger(__name__)

class GPMOperator:
    '''
    GPM Operator for handling various operations related to the GPM API.
    '''
    def __init__(self, prefix='biwo', data_path=None, sources_map_path=None):
        self.consumer = GPMConsumer(prefix)
        self.file_manager = FileManager(prefix, data_path, sources_map_path)

    @handle_authentication
    def check_auth(self):
        """Check if the authentication token is valid."""
        try:
            self.consumer.ping()
            logger.info("Authentication successful")
        except HTTPError as e:
            logger.error("Authentication failed")
            logger.debug(f"Error: {e}")
            raise e

    @handle_authentication
    def get_raw_plants(self, plant_id=None):
        """Handle the 'plants' operation."""
        try:
            plant_response = self.consumer.plant(plant_id=plant_id)
            logger.info("Plant info retrieved successfully")
            return plant_response
        except HTTPError as e:
            logger.error("Failed to retrieve plant from GPM API")
            logger.debug(f"Error: {e}")
            raise e

    @handle_authentication
    def handle_elements(self, plant_id, **kwargs):
        
        """Handle the 'elements' operation."""
        try:
            elements_response = self.consumer.element(plant_id=plant_id)
            logger.info(f"Retrieved elements successfully for plant ID {plant_id}")
            return elements_response
        except HTTPError as e:
            logger.error("Failed to retrieve elements")
            logger.debug(f"Error: {e}")
            raise e

    @handle_authentication
    def handle_element_details(self, plant_id, element_id, **kwargs):
        """Handle the 'element details' operation."""
        try:
            element_response = self.consumer.element(plant_id=plant_id, element_id=element_id)
            logger.info(f"Element details retrieved successfully for element ID {element_id} in plant ID {plant_id}")
            return element_response
        except HTTPError as e:
            logger.error("Failed to retrieve element details")
            logger.debug(f"Error: {e}")
            raise e

    @handle_authentication
    def handle_datasources(self, plant_id, signals=None, element_id=None, response=None, **kwargs):
        """Handle the 'datasources' operation."""
        try:
            if response is None:
                response = self.consumer.datasources(plant_id=plant_id, element_id=element_id)
            result = []
            if signals is None:
                return response
            for signal in signals:
                if signal == 'active_power':
                    matched_signals = self.interpreter.extract_active_power_from_response(response)
                elif signal == 'active_energy':
                    matched_signals = self.interpreter.extract_active_energy_from_response(response)
                else:
                    raise ValueError("Unsupported signal type")
                result.extend(matched_signals)
            logger.info(f"Retrieved datasources successfully for plant ID {plant_id} and signals {signals}{' and element ID ' + str(element_id) if element_id else ''}")
            return result
        except HTTPError as e:
            logger.error("Failed to retrieve datasources")
            logger.debug(f"Error: {e}")
            raise e

    @handle_authentication
    def handle_datalistv2(self, dataSourceIds: List[int], startDate: str, endDate: str,
                          grouping: str, granularity: int, aggregationType: int, **kwargs):
        """Handle the 'datalistv2' operation."""
        try:
            params = {
                'datasourceIds': ','.join(map(str, dataSourceIds)),
                'startDate': startDate,
                'endDate': endDate,
                'grouping': grouping,
                'granularity': granularity,
                'aggregationType': aggregationType,
            }
            response = self.consumer.datalistv2(params=params)
            logger.info("Retrieved datalistv2 successfully")
            return response
        except HTTPError as e:
            logger.error("Failed to retrieve datalistv2 from GPM API")
            logger.debug(f"Error: {e}")
            raise e

    def handle_datasources_map(self, plant_id: int, table: str):
        """
        Handle the 'datasources_map' operation.
        This method constructs a map of datasources for a given plant ID and table (gen or weather).
        The map should not vary often, and the pipeline is expensive, so it is not meant to be run all the time.
        """
        plant = self._find_plant(plant_id=plant_id)
        if table == 'gen':
            map = self.gen_datasources_map_pipeline(plant_id=plant['id'])
        elif table == 'weather':
            map = self.weather_datasources_map_pipeline(plant_id=plant['id'])
        else:
            raise ValueError("Invalid table type. Expected 'gen' or 'weather'.")
        path = self.file_manager.save_sources_map(f"{plant['safe_name']}_{table}_map.json", map)
        logger.info(f"Datasources map saved successfully for plant {plant['name']} in {path}")
        return path

    def args_handler(self, args, keys):
        """
        Manages the parsed arguments, if -f loads from config file, else uses positional arguments parsed from CLI.
        This method is used to parse the arguments for all operations.
        """
        ## Need to handle exceptions
        kwargs = {}
        if args.file:
            for key in keys:
                if key in self.consumer.config_manager._config:
                    kwargs[key] = self.consumer.config_manager.get(key)
        else:
            for key in keys:
                if key in args:
                    expected_type = self.consumer.config_manager.config_keys[key]
                    if getattr(args, key) is None:
                        raise ValueError(f"Argument '{key}' cannot be None")
                    if getattr(args, key) == 'None':
                        kwargs[key] = None
                    elif isinstance(expected_type, tuple):
                        values = [expected_type[1](v) for v in args[key].split(',')]
                        kwargs[key] = values
                    else:
                        kwargs[key] = expected_type(getattr(args, key))
        return kwargs

    def generate_paths(self, plant, startDate: str, endDate: str):
        """
        Generates the file paths for the data files based on the plant's safe name and the date range.
        """
        gen_path = os.path.join(
            self.file_manager.data_path,
            f"{plant['safe_name']}_gen_{startDate.replace(':', '')}_{endDate.replace(':', '')}.json"
        )
        weather_path = os.path.join(
            self.file_manager.data_path,
            f"{plant['safe_name']}_weather_{startDate.replace(':', '')}_{endDate.replace(':', '')}.json"
        )
        return gen_path, weather_path

    def get_start_end_dates(self, date, start_date, end_date):
        """
        Formats the date or start_date and end_date into the required format for the API.
        """

        from datetime import datetime, timedelta
        if date:
            startDate = date + "T00:00:00"
            endDate = (datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00")
        elif start_date and end_date:
            startDate = start_date + "T00:00:00"
            endDate = (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00")
        else:
            raise ValueError("Either date or both start_date and end_date must be provided.")

        return startDate, endDate
