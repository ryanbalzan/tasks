import yaml
import os
import pandas as pd
import hashlib

#Hash function. Skip hashing for NaN
def hash_column(value):
    if pd.isna(value):
        return value
    return hashlib.sha256(str(value).encode()).hexdigest()

load_object = "Player"

folder_path = os.getcwd()
metadata_path = os.path.join(folder_path,"catalog","Objects","Tables")
yaml_file_path = os.path.join(metadata_path,"Player.yaml")

metadata_catalog = None

# Open and read the YAML file
with open(yaml_file_path, "r") as file:
    metadata_catalog = yaml.safe_load(file)
    
source_path = metadata_catalog["metadata_catalog"]["source"].strip("/").split("/")

#read data and load to dataframe
df = pd.read_csv(os.path.join(folder_path, *source_path))

transformation_details = metadata_catalog["metadata_catalog"].get("transformation_details", {})

#apply transformations on columns
pii_fields = [field["name"] for field in metadata_catalog["metadata_catalog"]["fields"] if "PII" in field.get("tags",[])]
for pii_field in pii_fields:
    df[pii_field] = df[pii_field].apply(hash_column)

destination_path = metadata_catalog["metadata_catalog"]["destination"].strip("/").split("/")
destination_path = os.path.join(folder_path, *destination_path)

#save dataframe data
df.to_csv(destination_path,index=False)


print(f"File saved to {destination_path}")