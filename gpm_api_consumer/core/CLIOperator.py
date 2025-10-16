import logging
from requests.exceptions import HTTPError
from gpm_api_consumer.core.Consumers import GPMConsumer
from gpm_api_consumer.core.FileManager import GPMFileManager
from typing import List
import json
logger = logging.getLogger(__name__)


class CLIOperator:
    '''
    CLI Operator to handle operations and manage interactions between GPMConsumer and GPMFileManager.
    '''

    def __init__(self, prefix, data_path= None):
        self.consumer = GPMConsumer(prefix=prefix)
        self.file_manager = GPMFileManager(prefix=prefix, data_path=data_path)

    def check_auth(self):
        """Check if the authentication token is valid."""
        try:
            self.consumer.ping()
            logger.info("Authentication successful")
        except HTTPError as e:
            logger.error("Authentication failed")
            logger.debug(f"Error: {e}")
            raise e

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
                        
                        logger.info(f"Parsing list argument for key '{key}' with value '{getattr(args, key)}'")
                        #logger.info(f"Expected type for key '{key}' is '{args[key]}'")
                        values = [expected_type[1](v) for v in getattr(args, key).split(',')]
                        kwargs[key] = values
                    else:
                        kwargs[key] = expected_type(getattr(args, key))
        return kwargs

    def handle_elements(self, plant_id, **kwargs):
            """Handle the 'elements' operation."""
            try:
                elements_response = self.consumer.element(plant_id=plant_id)
                logger.info(f"Retrieved raw elements successfully for plant ID {plant_id}")
                return elements_response
            except HTTPError as e:
                logger.error("Failed to retrieve elements")
                logger.debug(f"Error: {e}")
                raise e

    def handle_plants(self, **kwargs):
        """Handle the 'plants' operation."""
        try:
            plant_response = self.consumer.plant()
            logger.info("Plant info retrieved successfully")
            return plant_response
        except HTTPError as e:
            logger.error("Failed to retrieve plants")
            logger.debug(f"Error: {e}")
            raise e

    def handle_plant_details(self, plant_id, **kwargs):
        """Handle the 'plant details' operation."""
        try:
            plant_response = self.consumer.plant(plant_id=plant_id)
            logger.info(f"Plant details retrieved successfully for plant ID {plant_id}")
            return plant_response
        except HTTPError as e:
            logger.error("Failed to retrieve plant details")
            logger.debug(f"Error: {e}")
            raise e

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

    def handle_datasources(self, plant_id,element_id=None, response=None, **kwargs):
        """Handle the 'datasources' operation."""
        try:
            response = self.consumer.datasources(plant_id=plant_id, element_id=element_id)
            logger.info(f"Retrieved datasources successfully for plant ID {plant_id} {' and element ID ' + str(element_id) if element_id else ''}")
            return response
        except HTTPError as e:
            logger.error("Failed to retrieve datasources")
            logger.debug(f"Error: {e}")
            raise e

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

    def handle_output(self, data, operation_name, output_arg=None):
        """Helper function to handle data output consistently"""
        if output_arg is not None:
            if output_arg is True:
                # Generate filename dynamically based on operation
                filename = f"{operation_name}_{self.get_timestamp()}.json"  # or your logic
                self.file_manager.save_data(data=data, filename=filename)
            else:
                # Use provided filename
                filename = output_arg
                self.file_manager.save_data(full_path=filename, data=data)
            
            logger.info(f"{operation_name.title()} data saved to {filename}")
        else:
            logger.info(f"{operation_name.title()}:")
            logger.info(json.dumps(data, indent=4))

    def get_timestamp(self):
        """Generate a timestamp string for filenames."""
        from datetime import datetime
        return datetime.now().strftime("%Y%m%d_%H%M%S")
