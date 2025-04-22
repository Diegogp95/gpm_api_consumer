from typing import List, Dict, Union
import re
import logging

class GPMInterpreter:
    '''
    Interpreter for GPM (Green Power Monitor) API responses.
    '''

    def interpret_plants_response(self, response: List[Dict]) -> List[Dict]:
        '''
        Interpret the plant response from the GPM API.
        '''
        plants = [
            {
                'id': plant['Id'],
                'name': plant['Name'],
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
            re.compile(r"^energy$", re.IGNORECASE)
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
