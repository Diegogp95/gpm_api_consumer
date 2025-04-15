from typing import List, Dict, Union
import re
import logging

class GPMInterpreter:
    '''
    Interpreter for GPM (Green Power Monitor) API responses.
    '''

    def interpret_plants_response(self, response: Dict):
        '''
        Interpret the plant response from the GPM API.
        '''
        plants = [
            {
                'id': plant['Id'],
                'name': plant['Name'],
            }
            for plant in response.get('Plants', [])
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

    def interpret_inverters_datasources_response(self, response: Dict):
        '''
        Interpret the inverters datasources response from the GPM API.
        Returns relevant datasources for inverters.
        '''
        patterns = {
            "active_power": re.compile(r"active\s*power", re.IGNORECASE),
            "active_energy": re.compile(r"active\s*energy", re.IGNORECASE),
        }
        matched_signals = {
            "active_power": None,
            "active_energy": None,
        }
        for datasource in response:
            name = datasource.get("DataSourceName", "")
            units = datasource.get("Units", "")
            if patterns["active_power"].search(name) or units.lower() == "kw":
                matched_signals["active_power"] = {
                    "id": datasource.get("DataSourceId"), "name": name, "units": units
                }
            elif patterns["active_energy"].search(name) or units.lower() == "kwh":
                matched_signals["active_energy"] = {
                    "id": datasource.get("DataSourceId"), "name": name, "units": units
                }
        return matched_signals
    
    def interpret_strings_datasources_response(self, response: Dict):
        '''
        Interpret the strings datasources response from the GPM API.
        Returns relevant datasources for strings.
        '''
        patterns = {
            "energy": re.compile(r"energy", re.IGNORECASE),
            "power": re.compile(r"power", re.IGNORECASE),
        }
        matched_signals = {
            "energy": None,
            "power": None,
        }
        for datasource in response:
            name = datasource.get("DataSourceName", "")
            units = datasource.get("Units", "")
            if patterns["energy"].search(name) or units.lower() == "kwh":
                matched_signals["energy"] = {
                    "id": datasource.get("DataSourceId"), "name": name, "units": units
                }
            elif patterns["power"].search(name) or units.lower() == "kw":
                matched_signals["power"] = {
                    "id": datasource.get("DataSourceId"), "name": name, "units": units
                }
        return matched_signals
