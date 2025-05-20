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

class GPMOperator:
    '''
    GPM Operator for handling various operations related to the GPM API.
    '''
    def __init__(self, prefix='biwo', data_path=None, sources_map_path=None):
        self.consumer = GPMConsumer(prefix)
        self.interpreter = GPMInterpreter()
        self.file_manager = FileManager(prefix, data_path, sources_map_path)

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
            logging.info(f"Retrieved datasources successfully for plant ID {plant_id} and signals {signals}{' and element ID ' + str(element_id) if element_id else ''}")
            return result
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
                'datasourceIds': ','.join(map(str, dataSourceIds)),
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

    def handle_datasources_map(self, plant:dict, table: str):
        """
        Handle the 'datasources_map' operation.
        This method constructs a map of datasources for a given plant ID and table (gen or weather).
        The map should not vary often, and the pipeline is expensive, so it is not meant to be run all the time.
        """
        if table == 'gen':
            map = self.gen_datasources_map_pipeline(plant_id=plant['id'])
        elif table == 'weather':
            map = self.weather_datasources_map_pipeline(plant_id=plant['id'])
        else:
            raise ValueError("Invalid table type. Expected 'gen' or 'weather'.")
        path = self.file_manager.save_sources_map(f"{plant['safe_name']}_{table}_map.json", map)
        logging.info(f"Datasources map saved successfully for plant {plant['name']} in {path}")
        return path

    def gen_datasources_map_pipeline(self, plant_id: int):
        """
        Creates the datasources map for the generation table.
        """
        elements, element_types = self.handle_elements(plant_id=plant_id)
        # First we look for inverters' power and meter's power, since we need to aggregate them
        # by avarage

        inverters_pattern = re.compile(r"inverter|inversor", re.IGNORECASE)
        meter_pattern = re.compile(r"meter|medidor", re.IGNORECASE)
        strings_patterns = re.compile(r"string", re.IGNORECASE)

        inverters_match = next((key for key in element_types if re.fullmatch(inverters_pattern, key)), None)
        meter_match = next((key for key in element_types if re.fullmatch(meter_pattern, key)), None)
        strings_match = next((key for key in element_types if re.fullmatch(strings_patterns, key)), None)

        if not inverters_match:
            logging.error("No inverters found")
            raise ex.NoInvertersFoundError("No inverters match with the plant's elements")
        if not meter_match:
            logging.error("No meter found")
            raise ex.NoMeterFoundError("No meter match with the plant's elements")
        if not strings_match:
            logging.error("No strings found")
            raise ex.NoStringsFoundError("No strings match with the plant's elements")

        inverter_elements = elements[inverters_match]
        meter_elements = elements[meter_match]
        strings_elements = elements[strings_match]

        # Count inverters per ct, in case inverter number is not reseted per ct
        inverters_per_ct = []
        # This flag is used to check if the inverter index need to be reseted for every ct
        reset_inverter_index_flag = False

        ct_patterns = [
            re.compile(r"CT(\d+)\.(\d+)", re.IGNORECASE),
            re.compile(r"Inverter\s+(\d+)\.(\d+)", re.IGNORECASE),
            re.compile(r"INV-(\d+)\.(\d+)", re.IGNORECASE),
        ]

        for inverter in inverter_elements:
            ct_index = None
            for pattern in ct_patterns:
                match = pattern.search(inverter['name'])
                if match:
                    ct_index = int(match.group(1))
                    break

            if ct_index is None:
                logging.warning(f"No CT index found for inverter name: {inverter['name']}")
                continue

            while len(inverters_per_ct) < ct_index:
                inverters_per_ct.append(0)

            inverters_per_ct[ct_index - 1] += 1

        # Check if the inverter index need to be reseted for every ct
        for inverter in inverter_elements:
            ct_index = None
            inverter_index = None
            for pattern in ct_patterns:
                match = pattern.search(inverter['name'])
                if match:
                    ct_index = int(match.group(1))
                    inverter_index = int(match.group(2))
                    break
            if ct_index is None or inverter_index is None:
                logging.warning(f"No CT index found for inverter name: {inverter['name']}")
                continue
            # if at least one inverter has a number greater than the number of inverters per ct, we need to reset the index
            if inverter_index > inverters_per_ct[ct_index - 1]:
                reset_inverter_index_flag = True
                break

        # Now we need to get the datasources for every element

        inverters_datasources = []
        meter_datasources = []
        strings_datasources = []

        logging.info(f"Retrieving gen table datasources for plant ID {plant_id}")
        for inverter in inverter_elements:
            inverter_datasources = set_logger_level(logging.WARNING)(self.handle_datasources)(
                plant_id=plant_id, signals=['active_power'], element_id=inverter['id']
            )
            inverters_datasources.append({
                'name': self.interpreter.format_inverter_name(inverter['name'], inverters_per_ct, reset_inverter_index_flag),
                'element_id': inverter['id'],
                'datasource_id': inverter_datasources[0]['id'],
                'datasource_name': inverter_datasources[0]['name'],
                'datasource_unit': inverter_datasources[0]['units'],
            })
        for meter in meter_elements:
            meter_power_sources = set_logger_level(logging.WARNING)(self.handle_datasources)(
                plant_id=plant_id, signals=['active_power'], element_id=meter['id']
            )
            meter_energy_sources = set_logger_level(logging.WARNING)(self.handle_datasources)(
                plant_id=plant_id, signals=['active_energy'], element_id=meter['id']
            )
            if len(meter_power_sources) > 1:
                logging.warning(f"More than one power source found for meter {meter['name']}, using the first one: {meter_power_sources[0]['name']}")
            if len(meter_energy_sources) > 1:
                logging.warning(f"More than one energy source found for meter {meter['name']}, using the first one: {meter_energy_sources[0]['name']}")

            meter_datasources.append({
                # name for meter signals is traduced to the signal name
                'name': 'act_power',
                'element_id': meter['id'],
                'datasource_id': meter_power_sources[0]['id'],
                'datasource_name': meter_power_sources[0]['name'],
                'datasource_unit': meter_power_sources[0]['units'],
            })
            meter_datasources.append({
                # name for meter signals is traduced to the signal name
                'name': 'act_energy',
                'element_id': meter['id'],
                'datasource_id': meter_energy_sources[0]['id'],
                'datasource_name': meter_energy_sources[0]['name'],
                'datasource_unit': meter_energy_sources[0]['units'],
            })
        for string in strings_elements:
            string_datasources = set_logger_level(logging.WARNING)(self.handle_datasources)(
                plant_id=plant_id, signals=['active_energy'], element_id=string['id']
            )
            strings_datasources.append({
                'name': self.interpreter.format_string_name(string['name'], inverters_per_ct, reset_inverter_index_flag),
                'element_id': string['id'],
                'datasource_id': string_datasources[0]['id'],
                'datasource_name': string_datasources[0]['name'],
                'datasource_unit': string_datasources[0]['units'],
            })

        logging.info(f"Gen datasources map retrieved successfully for plant ID {plant_id}")
        return inverters_datasources + meter_datasources + strings_datasources

    def weather_datasources_map_pipeline(self, plant_id: int):
        """
        Creates the datasources map for the weather table.
        """
        elements, element_types = self.handle_elements(plant_id=plant_id)
        # First we look for inverters' power and meter's power, since we need to aggregate them
        # by avarage

        weather_station_patterns = [
            re.compile(r"Weather\s*Station\s*CT(\d+)", re.IGNORECASE),
            re.compile(r"Meteo\s+CT(\d+)", re.IGNORECASE),
            re.compile(r"\bws\b", re.IGNORECASE),
        ]
        ghi_pattern = re.compile(r"\bGHI Irradiance", re.IGNORECASE)
        poa_pattern = re.compile(r"\bPOA\s*Irradiance", re.IGNORECASE)
        rpoa_pattern = re.compile(r"\bRPOA\s*Irradiance", re.IGNORECASE)
        temp_panel_pattern = re.compile(r"\bPOA\s*Internal\s*Temp", re.IGNORECASE)
        clean_cell_pattern = re.compile(r"Soiling Clean", re.IGNORECASE)
        dirty_cell_pattern = re.compile(r"Soiling Soiled", re.IGNORECASE)

        weather_stations_matches = []

        for element_type, _elements in elements.items():
            for element in _elements:
                for pattern in weather_station_patterns:
                    match = pattern.match(element['name'])
                    if match:
                        try:
                            ct_number = int(match.group(1))
                        except (IndexError, ValueError):
                            ct_number = 1
                        weather_stations_matches.append({
                            'ct_number': ct_number,
                            'name': element['name'],
                            'element_id': element['id'],
                        })
                        break

        weather_datasources = []
        logging.info(f"Retrieving weather table datasources for plant ID {plant_id}")
        for weather_station in weather_stations_matches:
            # No need for INFO logs
            weather_station_datasources = set_logger_level(logging.WARNING)(self.handle_datasources)(
                plant_id=plant_id, signals=None, element_id=weather_station['element_id']
            )
            for i, source in enumerate(weather_station_datasources):
                if re.search(ghi_pattern, source['DataSourceName']):
                    weather_datasources.append({
                        'name': f'ct{weather_station["ct_number"]:02d}_pyr1_h',
                        'element_id': weather_station['element_id'],
                        'datasource_id': source['DataSourceId'],
                        'datasource_name': source['DataSourceName'],
                        'datasource_unit': source['Units'],
                    })
                elif re.search(poa_pattern, source['DataSourceName']):
                    weather_datasources.append({
                        'name': f'ct{weather_station["ct_number"]:02d}_albedo1_up',
                        'element_id': weather_station['element_id'],
                        'datasource_id': source['DataSourceId'],
                        'datasource_name': source['DataSourceName'],
                        'datasource_unit': source['Units'],
                    })
                elif re.search(rpoa_pattern, source['DataSourceName']):
                    weather_datasources.append({
                        'name': f'ct{weather_station["ct_number"]:02d}_albedo1_down',
                        'element_id': weather_station['element_id'],
                        'datasource_id': source['DataSourceId'],
                        'datasource_name': source['DataSourceName'],
                        'datasource_unit': source['Units'],
                    })
                elif re.search(temp_panel_pattern, source['DataSourceName']):
                    weather_datasources.append({
                        'name': f'ct{weather_station["ct_number"]:02d}_temp_p1',
                        'element_id': weather_station['element_id'],
                        'datasource_id': source['DataSourceId'],
                        'datasource_name': source['DataSourceName'],
                        'datasource_unit': source['Units'],
                    })
                elif re.search(clean_cell_pattern, source['DataSourceName']):
                    weather_datasources.append({
                        'name': f'ct{weather_station["ct_number"]:02d}_clean_cell1',
                        'element_id': weather_station['element_id'],
                        'datasource_id': source['DataSourceId'],
                        'datasource_name': source['DataSourceName'],
                        'datasource_unit': source['Units'],
                    })
                elif re.search(dirty_cell_pattern, source['DataSourceName']):
                    weather_datasources.append({
                        'name': f'ct{weather_station["ct_number"]:02d}_dirty_cell1',
                        'element_id': weather_station['element_id'],
                        'datasource_id': source['DataSourceId'],
                        'datasource_name': source['DataSourceName'],
                        'datasource_unit': source['Units'],
                    })
        logging.info(f"Weather datasources map retrieved successfully for plant ID {plant_id}")
        return weather_datasources
    
    def _find_plant(self, plant_id: int = None, plant_name: str = None):
        """
        Searches for a plant by ID or safe_name.
        If both are provided, it will prioritize the ID.
        If not found, it raises a PlantNotFoundException.
        """
        plants = self.handle_plants()
        if plant_id is not None:
            plant = next((p for p in plants if p['id'] == plant_id), None)
        elif plant_name is not None:
            plant = next((p for p in plants if p['safe_name'].lower() == plant_name.lower()), None)
        else:
            plant = None
        if plant is None:
            raise ex.PlantNotFoundException(plant_id=plant_id, safe_name=plant_name)
        return plant

    def handle_plant_id_name_data_pipeline(self, startDate: str, endDate: str, plant_id: int=None,
                                           plant_name: str=None):
        """
        Handles the data pipeline for a specific plant ID or safe_name.
        """
        plant = self._find_plant(plant_id=plant_id, plant_name=plant_name)
        logging.info(f"Starting data pipeline for plant ID {plant['id']}")
        gen_path, weather_path = self.handle_plant_data_pipeline(plant, startDate, endDate)
        logging.info(f"Data pipeline completed for plant ID {plant['id']}")
        return gen_path, weather_path

    def handle_plant_data_pipeline(self, plant, startDate: str, endDate: str):
        datalist_params = {
            ## datasources and agregationType depends on the signal type, we add it for each request
            'startDate': startDate,
            'endDate': endDate,
            'grouping': 'minute',
            'granularity': 15,
        }
        logging.info(f"Starting data pipeline for plant {plant['name']}")
        try:
            gen_datasources_map = self.file_manager.load_sources_map(f"{plant['safe_name']}_gen_map.json")
            logging.info(f"Gen datasources map loaded for plant {plant['name']}")
        except FileNotFoundError:
            logging.info(f"Gen datasources map not found for plant {plant['name']}, creating it...")
            gen_datasources_map = self.gen_datasources_map_pipeline(plant_id=plant['id'])
            gen_map_path = self.file_manager.save_sources_map(f"{plant['safe_name']}_gen_map.json", gen_datasources_map)
            logging.info(f"Gen datasources map created for plant {plant['name']}")
        try:
            weather_datasources_map = self.file_manager.load_sources_map(f"{plant['safe_name']}_weather_map.json")
            logging.info(f"Weather datasources map loaded for plant {plant['name']}")
        except FileNotFoundError:
            logging.info(f"Weather datasources map not found for plant {plant['name']}, creating it...")
            weather_datasources_map = self.weather_datasources_map_pipeline(plant_id=plant['id'])
            weather_map_path = self.file_manager.save_sources_map(f"{plant['safe_name']}_weather_map.json", weather_datasources_map)
            logging.info(f"Weather datasources map created for plant {plant['name']}")
        power_sources = [source for source in gen_datasources_map if re.search(r"power", source['datasource_name'], re.IGNORECASE)]
        energy_sources = [source for source in gen_datasources_map if re.search(r"energy", source['datasource_name'], re.IGNORECASE)]
        power_params = {
            **datalist_params,
            'aggregationType': 1,
        }
        energy_params = {
            **datalist_params,
            'aggregationType': 0,
        }
        power_datasource_ids = [source['datasource_id'] for source in power_sources]
        energy_datasource_ids = [source['datasource_id'] for source in energy_sources]
        power_raw_responses = [
            self.handle_datalistv2(
                dataSourceIds=ids_chunk,
                **power_params
            ) for ids_chunk in chunked_iterable(power_datasource_ids, 10)
        ]
        energy_raw_responses = [
            self.handle_datalistv2(
                dataSourceIds=ids_chunk,
                **energy_params
            ) for ids_chunk in chunked_iterable(energy_datasource_ids, 10)
        ]
        logging.info(f"Data retrieved for plant {plant['name']}, formatting it...")
        traduced_responses = [
            self.interpreter.traduce_datalist_response(response, gen_datasources_map)
            for response in power_raw_responses + energy_raw_responses
        ]
        joined_gen_response = self.interpreter.join_time_series_responses(traduced_responses)
        gen_path = self.file_manager.save_data(
                f'{plant["safe_name"]}_gen_{startDate.replace(":", "")}_{endDate.replace(":", "")}.json',
                joined_gen_response)
        weather_params = {
            **datalist_params,
            'aggregationType': 1,
        }
        weather_datasource_ids = [source['datasource_id'] for source in weather_datasources_map]
        weather_raw_responses = [
            self.handle_datalistv2(
                dataSourceIds=ids_chunk,
                **weather_params
            ) for ids_chunk in chunked_iterable(weather_datasource_ids, 10)
        ]
        weather_traduced_responses = [
            self.interpreter.traduce_datalist_response(response, weather_datasources_map)
            for response in weather_raw_responses
        ]
        joined_weather_response = self.interpreter.join_time_series_responses(weather_traduced_responses)
        weather_path = self.file_manager.save_data(
                f'{plant["safe_name"]}_weather_{startDate.replace(":", "")}_{endDate.replace(":", "")}.json',
                joined_weather_response)
        logging.info(f"Data pipeline completed for plant {plant['name']}")
        return gen_path, weather_path

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
