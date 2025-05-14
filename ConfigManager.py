import json, os
from dotenv import load_dotenv, dotenv_values

class ConfigManager:
    """
    A simple configuration manager that loads, saves, and manages configuration settings.
    It uses a JSON file to store the configuration data.
    """

    def __init__(self, prefix, config_keys, config_path='config.json', env_path='.env'):
        self.prefix = prefix
        self.path = config_path
        self._config = self._load_config()
        self.env_path = env_path
        self._env_loaded = False
        self._env = {}
        self._load_env()
        self.config_keys = config_keys

    def _load_env(self):
        if os.path.exists(self.env_path):
            load_dotenv(dotenv_path=self.env_path, override=True)
            self._env = dotenv_values(self.env_path)
            self._env_loaded = True

    def print_credentials(self):
        '''
        Might be temporal
        '''
        if self._env_loaded:
            print("\nEnvironment Variables:")
            for key, value in self._env.items():
                print(f"\t{key}:\t{value}")
            print("\n")
        else:
            print("No environment variables loaded.")

    def show_config(self):
        '''
        Show the current configuration settings.
        '''
        if self._config:
            print("\nCurrent Configuration:\n")
            max_key_length = max(len(key) for key in self._config.keys())
            for key, value in self._config.items():
                if key == 'api_token' and value:
                    value = '**********'  # Hide the api_token value
                print(f"\t{key.ljust(max_key_length)}:\t{value if value is not None else ''}")
            print("\n")
        else:
            print("No configuration loaded.")

    def _load_config(self):
        if os.path.exists(self.path):
            with open(self.path, 'r') as file:
                return json.load(file)
        else:
            return self._create_default_config()

    def _create_default_config(self):
        default_config = {key: None for key, value in self.config_keys.items()}
        with open(self.path, 'w') as file:
            json.dump(default_config, file, indent=4)
        return default_config

    def get(self, key, default=None):
        return self._config.get(key, default)

    def set(self, key, value):
        if key in self.config_keys:
            if isinstance(self.config_keys[key], tuple):
                # expected type should be a list of types self.config_keys[key][1]
                # split the string by commas and cast each value to the expected type
                if isinstance(value, str) and self.config_keys[key][0] == list:
                    self.config_keys[key] = self.config_keys[key][1]
                    values = [self.config_keys[key](v) for v in value.split(',')]
                    self._config[key] = values
                    self._save_config()
                else:
                    raise TypeError(f"Expected a list of type {self.config_keys[key][1].__name__} for key '{key}', got {type(value).__name__}")
            elif isinstance(value, self.config_keys[key]):
                self._config[key] = value
                self._save_config()
            else:
                try:
                    value = self.config_keys[key](value)
                    self._config[key] = value
                    self._save_config()
                except (ValueError, TypeError):
                    raise TypeError(f"Expected type {self.config_keys[key].__name__} for key '{key}', got {type(value).__name__}")
        else:
            raise KeyError(f"Invalid configuration key: {key}")

    def _save_config(self):
        with open(self.path, 'w') as file:
            json.dump(self._config, file, indent=4)

    def _reset_config(self, keys=None):
        if keys is not None:
            for key in keys:
                if key in self._config:
                    self._config[key] = None
                    self._save_config()
                else:
                    raise KeyError(f"Invalid configuration key: {key}")
                return
        try:
            os.remove(self.path)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file '{self.path}' not found.")
        self._config = self._create_default_config()

    def __str__(self):
            max_key_length = max(len(key) for key in self._config.keys())
            config_str = "\n".join([f"\t{key.ljust(max_key_length)}:\t{value}" for key, value in self._config.items()])
            return f"\nConfiguration:\n{config_str}\n"

    def save_json_file(self, directory, filename, data, dir_name=None):
        """
        Saves or creeates a file with the current configuration.
        """
        directory = directory.split('/')[:-1] + [f"{self.prefix}{'_' + dir_name if dir_name else ''}"]
        directory = os.path.join(*directory)
        if not os.path.exists(directory):
            os.makedirs(directory)
        path = os.path.join(directory, filename)
        with open(path, 'w') as file:
            json.dump(data, file, indent=4)
        print(f"Saved data to {path}")

    def load_json_file(self, directory, filename, dir_name=None):
        """
        Loads a file with the current configuration.
        """
        directory = directory.split('/')[:-1] + [f"{self.prefix}{'_' + dir_name if dir_name else ''}"]
        directory = os.path.join(*directory)
        path = os.path.join(directory, filename)
        if not os.path.exists(path):
            raise FileNotFoundError(f"File {path} not found.")
        with open(path, 'r') as file:
            data = json.load(file)
        return data
