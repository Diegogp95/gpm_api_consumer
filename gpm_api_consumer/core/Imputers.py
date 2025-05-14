import json, os, sys


class Imputer:
    def __init__(self, path, fields, output_path=None):
        self.path = path
        self.fields = fields
        self.output_path = output_path

    def load_data(self):
        """
        Load the data from the specified path.
        """
        if os.path.exists(self.path):
            with open(self.path, 'r') as file:
                return json.load(file)
        else:
            raise FileNotFoundError(f"File not found: {self.path}")
        return None

    def save_data(self, data):
        """
        Save the data to the specified path.
        """
        if self.output_path:
            with open(self.output_path, 'w') as file:
                json.dump(data, file, indent=4)
        else:
            raise ValueError("Output path not specified.")

    def save_incidents(self, incidents):
        """
        Save the incidents to the specified path.
        """
        if self.output_path:
            incidents_path = os.path.splitext(self.output_path)[0] + "_incidents.json"
            with open(incidents_path, 'w') as file:
                json.dump(incidents, file, indent=4)
        else:
            raise ValueError("Output path not specified.")

    def impute(self, data):
        """
        Impute the missing values in the data.
        """
        field_count = len(self.fields)
        incidents = []
        for entry in data:
            # -1 beacuase the timestamp is not included in the field count
            if len(entry)-1 != field_count:
                # immpute missing values
                missing_fields = []
                for field in self.fields:
                    if field not in entry:
                        entry[field] = 0.0
                        missing_fields.append(field)
                if missing_fields:
                    incidents.append({
                        "timestamp": entry.get("timestamp", None),
                        "missing_fields": missing_fields
                    })
        return data, incidents

    def run(self):
        """
        Run the imputer
        """
        data = self.load_data()
        if data:
            imputed_data, incidents = self.impute(data)
            self.save_data(imputed_data)
            self.save_incidents(incidents)
        else:
            raise ValueError("No data to impute.")
        return None
