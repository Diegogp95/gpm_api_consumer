from typing import List, Dict, Union
import re
import logging
import unicodedata

class GPMInterpreter:
    '''
    Interpreter for GPM (Green Power Monitor) API responses.
    '''

    def interpret_plants_response(self, response: List[Dict]) -> List[Dict]:
        '''
        Interpret the plant response from the GPM API.
        '''
        def normalize_name(name: str) -> str:
            # Elimina tildes y caracteres raros, deja solo letras, números y guion bajo
            nfkd = unicodedata.normalize('NFKD', name)
            ascii_name = "".join([c for c in nfkd if not unicodedata.combining(c)])
            ascii_name = ascii_name.replace(" ", "_").replace("-", "_")
            safe = "".join([c for c in ascii_name if c.isalnum() or c == "_"])
            return safe

        plants = [
            {
                'id': plant['Id'],
                'name': plant['Name'],
                'safe_name': normalize_name(plant['Name']),
            }
            for plant in response
        ]
        return plants

    def interpret_elements_response(self, response: Dict):
        '''
        Interpret the elements response from the GPM API.
        Groups elements by their type.
        '''
        element_types = list({element['TypeString'] for element in response})
        grouped_elements = {
            _type: [
                {
                    'id': element['Identifier'],
                    'name': element['Name'],
                }
                for element in response if element['TypeString'] == _type
            ]
            for _type in element_types
        }
        return grouped_elements, element_types

    def extract_active_power_from_response(self, response: Dict):
        '''
        Extract active power from the response.
        '''
        name_patterns = [
            re.compile(r"active\s*power", re.IGNORECASE),
            re.compile(r"\bPower\b", re.IGNORECASE),
            re.compile(r"^Active\s+Power\s+Total\s+\(kw\)", re.IGNORECASE),
        ]
        unit_patterns = [
            re.compile(r"\bkw\b", re.IGNORECASE),
        ]
        return self.extract_datasource_from_response_by_name_unit(response, name_patterns, unit_patterns)

    def extract_active_energy_from_response(self, response: Dict):
        '''
        Extract active energy from the response.
        '''
        name_patterns = [
            re.compile(r"active\s*energy", re.IGNORECASE),
            re.compile(r"^energy$", re.IGNORECASE),
            re.compile(r"\bexported\s*active\s*energy\b", re.IGNORECASE),
        ]
        unit_patterns = [
            re.compile(r"\bkwh\b", re.IGNORECASE),
        ]
        return self.extract_datasource_from_response_by_name_unit(response, name_patterns, unit_patterns)

    def extract_datasource_from_response_by_name_unit(self, response: Dict, name_patterns: List[re.Pattern], unit_patterns: List[re.Pattern]):
        '''
        Extract datasources from the response based on name and unit patterns.
        '''
        matched_signals = []
        for datasource in response:
            name = datasource.get("DataSourceName", "")
            units = datasource.get("Units", "")
            if any(pattern.fullmatch(name) for pattern in name_patterns) and any(pattern.fullmatch(units) for pattern in unit_patterns):
                matched_signals.append({
                    "id": datasource.get("DataSourceId"),
                    "name": name,
                    "units": units
                })
        return matched_signals

    def join_time_series_responses(self, responses: List[List[Dict]]) -> List[Dict]:
        '''
        Join multiple timestamp indexed responses into a single response.
        '''
        joined_response = {}
        for response in responses:
            for entry in response:
                timestamp = entry['timestamp']
                if timestamp not in joined_response:
                    joined_response[timestamp] = {}
                joined_response[timestamp].update(entry)
        return sorted(joined_response.values(), key=lambda x: x['timestamp'])

    def traduce_datalist_response(self, response: List[Dict], elements_translations: List[Dict]) -> List[Dict]:
        '''
        Reads a spreaded json response and returns a list of dictionaries grouped by same timestamps.
        '''
        try:
            datasource_to_name = {
                translation["datasource_id"]: translation["name"]
                for translation in elements_translations
            }
            grouped_data = {}

            for entry in response:
                timestamp = entry["Date"]
                datasource_id = entry["DataSourceId"]
                value = entry["Value"]
                name = datasource_to_name.get(datasource_id, f"Unknown_{datasource_id}")

                if timestamp not in grouped_data:
                    grouped_data[timestamp] = {"timestamp": timestamp}

                grouped_data[timestamp][name] = value

            result = sorted(grouped_data.values(), key=lambda x: x["timestamp"])
            return result

        except Exception as e:
            logging.error("Failed to traduce datalist response")
            logging.debug(f"Error: {e}")
            raise e

    def format_inverter_name(self, inverter_name: str, inverters_per_ct: List[int], reset_inv: bool) -> str:
        '''
        Format the inverter name to a standardized format like: ct01_inv01.
        '''
        patterns = [
            re.compile(r"Inverter\s*CT(\d+)\.(\d+)", re.IGNORECASE),
            re.compile(r"Inverter\s+(\d+)\.(\d+)", re.IGNORECASE),
            re.compile(r"INV-(\d+)\.(\d+)", re.IGNORECASE),
        ]

        for pattern in patterns:
            match = pattern.match(inverter_name)
            if match:
                ct_index = int(match.group(1))
                inv_number = int(match.group(2))
                if reset_inv and ct_index > 1:
                    inv_number = inv_number - sum(inverters_per_ct[:ct_index - 1])
                if inv_number < 1 or inv_number > inverters_per_ct[ct_index - 1]:
                    raise ValueError(f"Invalid inverter number {inv_number} for CT {ct_index}. While processing inverter name {inverter_name}.")
                logging.debug(f"Formatted {inverter_name} -> ct{ct_index:02d}_inv{inv_number:02d}")
                return f"ct{ct_index:02d}_inv{inv_number:02d}"
        logging.warning(f"No pattern matched for inverter name: {inverter_name}")
        return inverter_name

    def format_string_name(self, name: str, inverters_per_ct: List[int], reset_inv: bool) -> str:
        '''
        Format the string name to a standardized format like: ct01_01_str01.
        '''
        patterns = [
            re.compile(r"String\s*CT(\d+)\.(\d+)\.(\d+)", re.IGNORECASE),
            re.compile(r"String\s*CT(\d+)\.(\d+)\s+(\d+)", re.IGNORECASE),
            re.compile(r"String\s*(\d+)\.(\d+)\.(\d+)", re.IGNORECASE),
            re.compile(r"String\s*-?\s*(\d+)\.(\d+)\s+(\d+)", re.IGNORECASE),
        ]

        for pattern in patterns:
            match = pattern.match(name)
            if match:
                ct_index = int(match.group(1))
                inv_number = int(match.group(2))
                string_number = int(match.group(3))
                if reset_inv and ct_index > 1:
                    inv_number = inv_number - sum(inverters_per_ct[:ct_index - 1])
                if inv_number < 1 or inv_number > inverters_per_ct[ct_index - 1]:
                    raise ValueError(f"Invalid inverter number {inv_number} for CT {ct_index}. While processing string name {name}.")
                logging.debug(f"Formatted {name} -> ct{ct_index:02d}_{inv_number:02d}_str{string_number:02d}")
                return f"ct{ct_index:02d}_{inv_number:02d}_str{string_number:02d}"

        logging.warning(f"No pattern matched for string name: {name}")
        return name
