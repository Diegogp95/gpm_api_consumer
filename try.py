from Consumers import GPMConsumer
from requests.exceptions import HTTPError
import json, getopt, sys
import logging


def main(argv):
    help_message = """

Usage: script.py [options]
Options:
    -h, --help            Show this help message and exit
    -i, --interactive     Run in interactive mode
    -l, --loglevel=LEVEL  Set the logging level (default: INFO)
    
"""

    shortopts = "hil:"
    longopts = ["help", "interactive", "loglevel="]
    loglevel = logging.INFO
    interactive = False
    try:
        opts, args = getopt.gnu_getopt(argv, shortopts, longopts)
    except getopt.GetoptError as err:
        print(err)
        sys.exit(2)

    for o, a in opts:
        if o in ("-h", "--help"):
            print(help_message)
            sys.exit(0)
        elif o in ("-i", "--interactive"):
            interactive = True
        elif o in ("-l", "--loglevel"):
            loglevel = getattr(logging, a.upper(), logging.INFO)
        else:
            assert False, "Unhandled option"

    logging.basicConfig(level=loglevel, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    logger.setLevel(loglevel)

    gpm = GPMConsumer(prefix='biwo')
    try:
        gpm.ping()
        print("Ping successful!")
    except HTTPError as e:
        print(f"Ping failed: {e}")
        try:
            gpm.login()
            print("Login successful!")
        except HTTPError as e:
            print(f"Login failed: {e}")
            return
    plant_response = gpm.plant()
    plants = [
        {
            'id': plant['Id'],
            'name': plant['Name'],
        }
        for plant in plant_response
    ]
    print("\n", "Select a plant:")
    for i, plant in enumerate(plants):
        print(f"{i + 1}. {plant['name']}")
    choice = int(input("\nEnter the number of your choice: ")) - 1
    if 0 <= choice < len(plants):
        selected_plant = plants[choice]
        print(f"You selected: {selected_plant['name']}")
    else:
        print("Invalid choice.")
        return
    
    elements_response = gpm.element(selected_plant['id'])
    inverters = [
        {
            'id': element['Identifier'],
            'name': element['Name'],
        }
        for element in elements_response if element['TypeString'] == 'Inverter'
    ]
    strings = [
        {
            'id': element['Identifier'],
            'name': element['Name'],
        }
        for element in elements_response if element['TypeString'] == 'String'
    ]
    element_types = list({element['TypeString'] for element in elements_response})

    grouped_elements = {
        _type: [
            {
                'id': element['Identifier'],
                'name': element['Name'],
            }
            for element in elements_response if element['TypeString'] == _type
        ]
        for _type in element_types
    }

    print("\n", "Select an element type:")
    for i, element_type in enumerate(element_types):
        print(f"{i + 1}. {element_type}")
    choice = int(input("\nEnter the number of your choice: ")) - 1
    if 0 <= choice < len(element_types):
        selected_element_type = element_types[choice]
        print(f"You selected: {selected_element_type}")
    else:
        print("Invalid choice.")
        return

    print("\n", f"{selected_element_type} elements:")
    for element in grouped_elements[selected_element_type]:
        print(f"{element['id']}: {element['name']}")
    
    choice = int(input("\nEnter the ID of your choice: "))
    if any(element['id'] == choice for element in grouped_elements[selected_element_type]):
        selected_element = next(element for element in grouped_elements[selected_element_type] if element['id'] == choice)
        print(f"You selected: {selected_element['name']}")
    else:
        print("Invalid choice.")
        return

    datasources_response = gpm.datasources(selected_plant['id'], selected_element['id'])
    print("\n", "Data sources:")
    print(json.dumps(datasources_response, indent=4))


if __name__ == "__main__":
    main(sys.argv[1:])