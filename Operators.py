from Consumers import GPMConsumer
from Interpreters import GPMInterpreter
from requests.exceptions import HTTPError
import logging
import json
from functools import wraps
from typing import List, Dict, Union


class GPMOperator:
    '''
    GPM Operator for handling various operations related to the GPM API.
    '''
    def __init__(self):
        self.consumer = GPMConsumer(prefix='biwo')
        self.interpreter = GPMInterpreter()
        
    @staticmethod
    def handle_authentication(func):
        """
        Decorator to handle authentication for operations.
        Retries the operation if a 401 Unauthorized error occurs.
        """
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except HTTPError as e:
                if e.response.status_code == 401:
                    logging.info("Token expired. Re-authenticating...")
                    self.consumer.login()
                    logging.info("Re-authentication successful. Retrying operation...")
                    return func(self, *args, **kwargs)
                else:
                    logging.error("Operation failed")
                    logging.debug(f"Error: {e}")
                    raise e
            except Exception as e:
                logging.error("Unexpected error occurred")
                logging.debug(f"Error: {e}")
                raise e
        return wrapper

    @handle_authentication
    def check_auth(self):
        """Check if the authentication token is valid."""
        try:
            self.consumer.ping()
            logging.info("Authentication successful")
        except HTTPError as e:
            logging.error("Authentication failed")
            logging.debug(f"Error: {e}")
            raise e

    @handle_authentication
    def handle_plants(self, **kwargs):
        """Handle the 'plants' operation."""
        try:
            plant_response = self.consumer.plant()
            plants = self.interpreter.interpret_plants_response(plant_response)
            logging.info("Plants retrieved successfully")
            return plants
        except HTTPError as e:
            logging.error("Failed to retrieve plants")
            logging.debug(f"Error: {e}")
            raise e

    @handle_authentication
    def handle_plant_details(self, plant_id, **kwargs):
        """Handle the 'plant details' operation."""
        try:
            plant_response = self.consumer.plant(plant_id=plant_id)
            logging.info(f"Plant details retrieved successfully for plant ID {plant_id}")
            return plant_response
        except HTTPError as e:
            logging.error("Failed to retrieve plant details")
            logging.debug(f"Error: {e}")
            raise e
    
    @handle_authentication
    def handle_elements(self, plant_id, **kwargs):
        """Handle the 'elements' operation."""
        try:
            elements_response = self.consumer.element(plant_id=plant_id)
            grouped_elements, element_types = self.interpreter.interpret_elements_response(elements_response)
            logging.info(f"Retrieved elements successfully for plant ID {plant_id}")
            return grouped_elements, element_types
        except HTTPError as e:
            logging.error("Failed to retrieve elements")
            logging.debug(f"Error: {e}")
            raise e

    @handle_authentication
    def handle_element_details(self, plant_id, element_id, **kwargs):
        """Handle the 'element details' operation."""
        try:
            element_response = self.consumer.element(plant_id=plant_id, element_id=element_id)
            logging.info(f"Element details retrieved successfully for element ID {element_id} in plant ID {plant_id}")
            return element_response
        except HTTPError as e:
            logging.error("Failed to retrieve element details")
            logging.debug(f"Error: {e}")
            raise e

    @handle_authentication
    def handle_datasources(self, plant_id, signals, element_id=None, response=None, **kwargs):
        """Handle the 'datasources' operation."""
        try:
            if response is None:
                response = self.consumer.datasources(plant_id=plant_id, element_id=element_id)
            result = {}
            for signal in signals:
                if signal == 'active_power':
                    matched_signals = self.interpreter.extract_active_power_from_response(response)
                elif signal == 'active_energy':
                    matched_signals = self.interpreter.extract_active_energy_from_response(response)
                else:
                    raise ValueError("Unsupported signal type")
                result.update({signal: matched_signals})
            logging.info(f"Retrieved datasources successfully for plant ID {plant_id} and signals {signals}{' and element ID ' + str(element_id) if element_id else ''}")
            return matched_signals
        except HTTPError as e:
            logging.error("Failed to retrieve datasources")
            logging.debug(f"Error: {e}")
            raise e

    @handle_authentication
    def handle_datalistv2(self, dataSourceIds: List[int], startDate: str, endDate: str,
                          grouping: str, granularity: int, aggregationType: int, **kwargs):
        """Handle the 'datalistv2' operation."""
        try:
            params = {
                'datasourceIds': dataSourceIds,
                'startDate': startDate,
                'endDate': endDate,
                'grouping': grouping,
                'granularity': granularity,
                'aggregationType': aggregationType,
            }
            response = self.consumer.datalistv2(params=params)
            logging.info("Retrieved datalistv2 successfully")
            return response
        except HTTPError as e:
            logging.error("Failed to retrieve datalistv2")
            logging.debug(f"Error: {e}")
            raise e

    def datalist_pipeline(self, plant_id: int, type: str,
                          startDate: str, endDate: str, grouping: str,
                          granularity: int, aggregation: int,
                          element_id: int = None, **kwargs):
        """Handle the 'datalist pipeline' operation."""
        if element_id:
            datasources = self.handle_datasources(plant_id=plant_id, type=type, element_id=element_id)
        else:
            datasources = self.handle_datasources(plant_id=plant_id, type=type)
        ids = [datasource['id'] for datasource in datasources.values()]
        ## Max. 10 ids per request, so split and iterate if needed
        if len(ids) > 10:
            responses = []
            for i in range(0, len(ids), 10):
                chunk_ids = ids[i:i+10]
                response = self.handle_datalistv2(ids=chunk_ids, startDate=startDate, endDate=endDate,
                                                   grouping=grouping, granularity=granularity,
                                                   aggregation=aggregation)
                responses.append(response)
            joined_response = self.interpreter.join_time_series_responses(responses)
            logging.info(f"Retrieved datalist pipeline successfully for plant ID {plant_id} and type {type}")
            return joined_response
        else:
            response = self.handle_datalistv2(ids=ids, startDate=startDate, endDate=endDate,
                                           grouping=grouping, granularity=granularity,
                                           aggregation=aggregation)
            logging.info(f"Retrieved datalist pipeline successfully for plant ID {plant_id} and type {type}")
            return response
