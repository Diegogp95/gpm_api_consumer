import json, os
from dotenv import load_dotenv, dotenv_values

class ConfigManager:
    """
    A simple configuration manager that loads, saves, and manages configuration settings.
    It uses a JSON file to store the configuration data.
    """

    config_keys = {
        'api_token': str,
        'date': str,
        'start_date': str,
        'end_date': str,
    }

    def __init__(self, config_path='config.json', env_path='.env',
                    config_keys=None):
        self.path = config_path
        self._config = self._load_config()
        self.env_path = env_path
        self._env_loaded = False
        self._env = {}
        self._load_env()
        if config_keys:
            self.config_keys.update(config_keys)

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

    def _load_config(self):
        if os.path.exists(self.path):
            with open(self.path, 'r') as file:
                return json.load(file)
        else:
            return self._create_default_config()

    def _create_default_config(self):
        default_config = {key: value() for key, value in self.config_keys.items()}
        with open(self.path, 'w') as file:
            json.dump(default_config, file, indent=4)
        return default_config

    def get(self, key, default=None):
        return self._config.get(key, default)

    def set(self, key, value):
        if key in self.config_keys:
            if isinstance(value, self.config_keys[key]):
                self._config[key] = value
                self._save_config()
            else:
                raise TypeError(f"Expected type {self.config_keys[key].__name__} for key '{key}', got {type(value).__name__}")
        else:
            raise KeyError(f"Invalid configuration key: {key}")

    def _save_config(self):
        with open(self.path, 'w') as file:
            json.dump(self._config, file, indent=4)

    def _reset_config(self):
        try:
            os.remove(self.path)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file '{self.path}' not found.")
        self._config = self._create_default_config()

    def __str__(self):
            max_key_length = max(len(key) for key in self._config.keys())
            config_str = "\n".join([f"\t{key.ljust(max_key_length)}:\t{value}" for key, value in self._config.items()])
            return f"\nConfiguration:\n{config_str}\n"
