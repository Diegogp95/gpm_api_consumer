from Operators import GPMOperator
import logging
import argparse
import json


def main():
    parser = argparse.ArgumentParser(
        'python gpm_consumer_cli.py',
        description='GPM Consumer CLI',
        formatter_class=argparse.RawTextHelpFormatter,
    )

    ## General options
    parser.add_argument('-l', '--loglevel', type=str, default='info',
                        help='Set the logging level (debug, info, warning, error, critical)')
    parser.add_argument('-i', '--interactive', action='store_true',
                        help='Run in interactive mode')
    parser.add_argument('-f', '--file', action='store_true',
                        help='Load parameters from the config file')

    subparsers = parser.add_subparsers(dest='operation', required=True,
                            help='Available operations:')
    
    # Operation: config
    config_parser = subparsers.add_parser('config', help='Manage config values')
    config_subparsers = config_parser.add_subparsers(dest='action', required=True)

    # Operation: config set
    config_set = config_subparsers.add_parser('set', help='Set config values')
    config_set.add_argument('pairs', nargs='+', help='Key-value pairs. Example: plant 4 element 447')

    # Operation: config get
    config_subparsers.add_parser('show', help='Show current config')

    # Operation: config reset
    config_reset = config_subparsers.add_parser('reset', help='Reset config to default values')
    config_reset.add_argument('keys', nargs='*', help='Keys to reset. If not provided, all keys will be reset.')

    # Operation: plants
    plant_parser = subparsers.add_parser('plants', help='List all plants')

    # Operation: plant_detail
    plant_detail_parser = subparsers.add_parser('plant_detail', help='Get details of a specific plant')
    plant_detail_parser.add_argument('plant_id', type=int, nargs='?', default=None,
                            help='ID of the plant')

    # Operation: elements
    elements_parser = subparsers.add_parser('elements', help='List all elements of a plant')
    elements_parser.add_argument('plant_id', type=int, nargs='?', default=None,
                            help='ID of the plant')

    # Operation: element_detail
    element_detail_parser = subparsers.add_parser('element_detail', help='Get details of a specific element')
    element_detail_parser.add_argument('plant_id', type=int, nargs='?', default=None,
                            help='ID of the plant')
    element_detail_parser.add_argument('element_id', type=int, nargs='?', default=None,
                            help='ID of the element')

    # Operation: datasources (per element)
    datasources_parser = subparsers.add_parser('datasources',
                                help='List all datasources of an element of a plant')
    datasources_parser.add_argument('plant_id', type=int, nargs='?', default=None,
                            help='ID of the plant')
    datasources_parser.add_argument('element_id', type=int, nargs='?', default=None,
                            help='ID of the element (optional, use -e to specify)')
    datasources_parser.add_argument('signals', type=str, nargs='?', default=None,
                            help='Type of the datasource signal (e.g., active_power, active_energy)')

    # Operation: plant_datasources
    plant_datasources_parser = subparsers.add_parser('plant_datasources',
                                help='List all datasources of a plant')
    plant_datasources_parser.add_argument('plant_id', type=int, nargs='?', default=None,
                            help='ID of the plant')
    plant_datasources_parser.add_argument('signals', type=str, nargs='?', default=None,
                            help='Type of the datasource signal (e.g., active_power, active_energy)')

    # Operation: datalistv2
    datalist_parser = subparsers.add_parser('datalistv2',
                                help='Get data from the GPM API')
    datalist_parser.add_argument('dataSourceIds', type=str, nargs='?',
                            help='List of datasource IDs (comma-separated)')
    datalist_parser.add_argument('startDate', type=str, nargs='?',
                            help='Start date in YYYY-MM-DDTHH:MM:SS format')
    datalist_parser.add_argument('endDate', type=str, nargs='?',
                            help='End date in YYYY-MM-DDTHH:MM:SS format')
    datalist_parser.add_argument('grouping', type=str, nargs='?',
                            help='Grouping type (e.g., "raw", "minute", "hour", "day")')
    datalist_parser.add_argument('granularity', type=int, nargs='?',
                            help='Granularity for the grouping method (e.g., 1, 5, 15)')
    datalist_parser.add_argument('aggregationType', type=int, nargs='?',
                            help='Aggregation type (e.g., 0 for sum w/o zeros, 1 for average)')

    args = parser.parse_args()

    # Logging setup
    loglevel = getattr(logging, args.loglevel.upper(), logging.INFO)
    logging.basicConfig(level=loglevel, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    logger.setLevel(loglevel)

    operator = GPMOperator()
    if args.operation != 'config':
        operator.check_auth()

    if args.operation == 'config':
        if args.action == 'set':
            print(f"Set config: {args.pairs}")
            if len(args.pairs) % 2 != 0:
                print("Error: You must provide an even number of arguments (key value pairs).")
                return
            for i in range(0, len(args.pairs), 2):
                key = args.pairs[i]
                value = args.pairs[i + 1]
                operator.consumer.config_manager.set(key, value)
            print("Config set successfully.")
        elif args.action == 'show':
            operator.consumer.config_manager.show_config()
        elif args.action == 'reset':
            operator.consumer.config_manager._reset_config(args.keys)
            print("Config reset successfully.")
        else:
            print(f"Usage: {parser.format_help()}")
    elif args.operation == 'plants':
        plants = operator.handle_plants()
        print("Plants:")
        for plant in plants:
            print(f"\t{plant['name']} (ID: {plant['id']})")
    elif args.operation in ['plant_detail', 'elements']:
        if args.file:
            plant_id = operator.consumer.config_manager.get('plant_id')
            if plant_id is None:
                print("Error: Plant ID not found in config file.")
                return
            else:
                logging.info(f"Loaded plant ID {plant_id} from config file.")
        else:
            plant_id = args.plant_id
        if plant_id is None:
            print("Error: Plant ID is required. Provide it as an argument or use -f to load from config file.")
            return
        if args.operation == 'plant_detail':
            result = operator.handle_plant_details(plant_id=plant_id)
            print(f"Details of plant {plant_id}:")
            print(json.dumps(result, indent=4))
        elif args.operation == 'elements':
            result = operator.handle_elements(plant_id=plant_id)
            print(f"Types of elements in plant {plant_id}:")
            print(json.dumps(result[1], indent=4))
            print(f"Elements in plant {plant_id}:")
            for _type, elements in result[0].items():
                print(f"\t{_type}:")
                for element in elements:
                    print(f"\t\t{element['name']} (ID: {element['id']})")
    elif args.operation == 'element_detail':
        if args.file:
            plant_id = operator.consumer.config_manager.get('plant_id')
            element_id = operator.consumer.config_manager.get('element_id')
            if plant_id is None or element_id is None:
                print("Error: Plant ID or Element ID not found in config file.")
                return
            else:
                logging.info(f"Loaded plant ID {plant_id} and element ID {element_id} from config file.")
        else:
            plant_id = args.plant_id
            element_id = args.element_id
        if plant_id is None or element_id is None:
            print("Error: Plant ID and Element ID are required. Provide them as arguments or use -f to load from config file.")
            return
        result = operator.handle_element_details(plant_id=plant_id, element_id=element_id)
        print(f"Details of element {element_id} in plant {plant_id}:")
        print(json.dumps(result, indent=4))
    elif args.operation in ['datasources', 'plant_datasources']:
        if args.file:
            plant_id = operator.consumer.config_manager.get('plant_id')
            signals = operator.consumer.config_manager.get('signals')
            if args.operation == 'datasources':
                element_id = operator.consumer.config_manager.get('element_id')
            else:
                element_id = None
            if plant_id is None or signals is None:
                print("Error: Plant ID or signals type not found in config file.")
                return
            if element_id is None and args.operation == 'datasources':
                print("Error: Element ID not found in config file.")
                return
            logging.info(f"Loaded plant ID {plant_id}, signals {signals} {'and element ID {element_id}' if args.operation == 'datasources' else ''} from config file.")
        else:
            plant_id = args.plant_id
            signals = args.signals.split(',')
            if args.operation == 'datasources':
                element_id = args.element_id
            else:
                element_id = None
        if plant_id is None or signals is None:
            print("Error: Plant ID and signals type are required. Provide them as arguments or use -f to load from config file.")
            return
        if args.operation == 'datasources':
            if element_id is None:
                print("Error: Element ID is required for datasources operation.")
                return
            logging.info(f"Retrieving datasources of signals {signals} in plant {plant_id} and element {element_id}.")
            result = operator.handle_datasources(plant_id=plant_id, signals=signals, element_id=element_id)
            print(f"Datasources of signals {signals} in plant {plant_id} and element {element_id}:")
            print(json.dumps(result, indent=4))
        else:
            logging.info(f"Retrieving datasources of signals {signals} in plant {plant_id}.")
            result = operator.handle_datasources(plant_id=plant_id, signals=signals)
            print(f"Datasources of signals {signals} in plant {plant_id}:")
            print(json.dumps(result, indent=4))
    elif args.operation == 'datalistv2':
        if args.file:
            plant_id = operator.consumer.config_manager.get('plant_id')
            dataSourceIds = operator.consumer.config_manager.get('dataSourceIds')
            startDate = operator.consumer.config_manager.get('startDate')
            endDate = operator.consumer.config_manager.get('endDate')
            grouping = operator.consumer.config_manager.get('grouping')
            granularity = operator.consumer.config_manager.get('granularity')
            aggregationType = operator.consumer.config_manager.get('aggregationType')
            if plant_id is None or dataSourceIds is None or startDate is None or endDate is None or grouping is None or granularity is None or aggregationType is None:
                print("Error: Plant ID, dataSourceIds, startDate, endDate, grouping, granularity and aggregationType not found in config file.")
                return
            logging.info(f"Loaded plant ID {plant_id}, dataSourceIds {dataSourceIds}, startDate {startDate}, endDate {endDate}, grouping {grouping}, granularity {granularity} and aggregationType {aggregationType} from config file.")
        else:
            plant_id = args.plant_id
            dataSourceIds = [int(x) for x in args.dataSourceIds.split(',')]
            startDate = args.startDate
            endDate = args.endDate
            grouping = args.grouping
            granularity = args.granularity
            aggregationType = args.aggregationType
        if plant_id is None or dataSourceIds is None or startDate is None or endDate is None or grouping is None or granularity is None or aggregationType is None:
            print("Error: Plant ID, dataSourceIds, startDate, endDate, grouping, granularity and aggregationType are required. Provide them as arguments or use -f to load from config file.")
            return
        logging.info(f"Retrieving datalistv2 for plant {plant_id} with dataSourceIds {dataSourceIds}, startDate {startDate}, endDate {endDate}, grouping {grouping}, granularity {granularity} and aggregationType {aggregationType}.")
        result = operator.handle_datalistv2(dataSourceIds=dataSourceIds, startDate=startDate,
                                            endDate=endDate, grouping=grouping,
                                            granularity=granularity, aggregationType=aggregationType)
        print(f"Datalistv2 for plant {plant_id} with dataSourceIds {dataSourceIds}, startDate {startDate}, endDate {endDate}, grouping {grouping}, granularity {granularity} and aggregationType {aggregationType}:")
        print(json.dumps(result, indent=4))

if __name__ == "__main__":
    main()