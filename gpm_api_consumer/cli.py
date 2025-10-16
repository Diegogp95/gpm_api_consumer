from gpm_api_consumer.core.CLIOperator import CLIOperator
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
    parser.add_argument('-f', '--file', action='store_true',
                        help='Load parameters from the config file')
    parser.add_argument("-p", "--portfolio", type=str, required=False, default="biwo",
        help="Portfolio name (default: 'biwo')")

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
    plant_parser.add_argument('-o','--output', type=str, nargs='?', const=True, default=None,
                        help='Path to save data files. If no filename provided, generates one automatically')

    # Operation: plant_detail
    plant_detail_parser = subparsers.add_parser('plant_detail', help='Get details of a specific plant')
    plant_detail_parser.add_argument('plant_id', type=int, nargs='?', default=None,
                            help='ID of the plant')
    plant_detail_parser.add_argument('-o','--output', type=str, nargs='?', const=True, default=None,
                        help='Path to save data files. If no filename provided, generates one automatically')

    # Operation: elements
    elements_parser = subparsers.add_parser('elements', help='List all elements of a plant')
    elements_parser.add_argument('plant_id', type=int, nargs='?', default=None,
                            help='ID of the plant')
    elements_parser.add_argument('-o','--output', type=str, nargs='?', const=True, default=None,
                        help='Path to save data files. If no filename provided, generates one automatically')

    # Operation: element_detail
    element_detail_parser = subparsers.add_parser('element_detail', help='Get details of a specific element')
    element_detail_parser.add_argument('plant_id', type=int, nargs='?', default=None,
                            help='ID of the plant')
    element_detail_parser.add_argument('element_id', type=int, nargs='?', default=None,
                            help='ID of the element')
    element_detail_parser.add_argument('-o','--output', type=str, nargs='?', const=True, default=None,
                        help='Path to save data files. If no filename provided, generates one automatically')

    # Operation: datasources (per element)
    datasources_parser = subparsers.add_parser('datasources',
                                help='List all datasources of an element of a plant')
    datasources_parser.add_argument('plant_id', type=int, nargs='?', default=None,
                            help='ID of the plant')
    datasources_parser.add_argument('element_id', type=int, nargs='?', default=None,
                            help='ID of the element')
    datasources_parser.add_argument('-o','--output', type=str, nargs='?', const=True, default=None,
                        help='Path to save data files. If no filename provided, generates one automatically')

    # Operation: plant_datasources
    plant_datasources_parser = subparsers.add_parser('plant_datasources',
                                help='List all datasources of a plant')
    plant_datasources_parser.add_argument('plant_id', type=int, nargs='?', default=None,
                            help='ID of the plant')
    plant_datasources_parser.add_argument('-o','--output', type=str, nargs='?', const=True, default=None,
                        help='Path to save data files. If no filename provided, generates one automatically')

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
    datalist_parser.add_argument('-o','--output', type=str, nargs='?', const=True, default=None,
                            help='Path to save data files. If no filename provided, generates one automatically')

    args = parser.parse_args()

    # Logging setup
    loglevel = getattr(logging, args.loglevel.upper(), logging.INFO)
    logging.basicConfig(level=loglevel, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    logger.setLevel(loglevel)

    operator = CLIOperator(prefix=args.portfolio)
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
        operator.handle_output(plants, "plants", getattr(args, 'output', None))

    elif args.operation == 'plant_detail':
        kwargs = operator.args_handler(args, ['plant_id'])
        result = operator.handle_plant_details(**kwargs)
        print(f"Details of plant {kwargs['plant_id']}:")
        operator.handle_output(result, "plant_detail", getattr(args, 'output', None))

    elif args.operation == 'elements':
        kwargs = operator.args_handler(args, ['plant_id'])
        raw_elements = operator.handle_elements(**kwargs)
        print(f"Elements in plant {kwargs['plant_id']}:")
        operator.handle_output(raw_elements, "elements", getattr(args, 'output', None))

    elif args.operation == 'element_detail':
        kwargs = operator.args_handler(args, ['plant_id', 'element_id'])
        result = operator.handle_element_details(**kwargs)
        print(f"Details of element {kwargs['element_id']} in plant {kwargs['plant_id']}:")
        operator.handle_output(result, "element_detail", getattr(args, 'output', None))

    elif args.operation == 'datasources':
        kwargs = operator.args_handler(args, ['plant_id', 'element_id'])
        result = operator.handle_datasources(**kwargs)
        print(f"Datasources in plant {kwargs['plant_id']} and element {kwargs['element_id']}:")
        operator.handle_output(result, "datasources", getattr(args, 'output', None))

    elif args.operation == 'plant_datasources':
        kwargs = operator.args_handler(args, ['plant_id'])
        result = operator.handle_datasources(**kwargs)
        print(f"Datasources in plant {kwargs['plant_id']}:")
        operator.handle_output(result, "plant_datasources", getattr(args, 'output', None))

    elif args.operation == 'datalistv2':
        kwargs = operator.args_handler(args, ['dataSourceIds', 'startDate',
                        'endDate', 'grouping', 'granularity', 'aggregationType'])
        result = operator.handle_datalistv2(**kwargs)
        print(f"Datalistv2 with dataSourceIds {kwargs['dataSourceIds']}, startDate {kwargs['startDate']}, endDate {kwargs['endDate']}, grouping {kwargs['grouping']}, granularity {kwargs['granularity']} and aggregationType {kwargs['aggregationType']}:")
        operator.handle_output(result, "datalistv2", getattr(args, 'output', None))


if __name__ == "__main__":
    main()