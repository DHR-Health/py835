import pandas as pd 
import numpy as np
import math

class CustomDataFrame(pd.DataFrame):
    @property 
    def _constructor(self):
        return CustomDataFrame

    def flatten(self, prefix=None, descriptions=False):
        # Dictionary to store the results
        row_dict = {}

        # Iterate through each row in the DataFrame
        for _, row in self.iterrows():
            # Generate the unique key for the identifiers (header_id, functional_group_id, etc.)
            identifier_key = tuple(row[col] for col in self.columns if col.endswith('_ID'))

            # Create a new entry in row_dict if the key does not exist
            if identifier_key not in row_dict:
                row_dict[identifier_key] = {col: row[col] for col in self.columns if col.endswith('_ID')}
            # Build the column name from segment, field, and name
            segment = row['segment']
            field = row['field']
            name = row['name']
            
            # Construct the column name
            if prefix:
                col_name = f"{prefix}{segment} {field}"
            else:
                col_name = f"{segment} {field}"
            
            if descriptions and name:
                col_name = f"{col_name} {name}"
                if 'description' in row:
                    if not pd.isna(row['description']):
                        col_name = f"{col_name} ({row['description']})"
            
            # Check if column already exists, and if so, append a new version with an incrementing number
            col_version = 1
            base_col_name = col_name
            while col_name in row_dict[identifier_key]:
                col_version += 1
                col_name = f"{base_col_name}_{col_version}"
            
            # Add the value to the row
            row_dict[identifier_key][col_name] = row['value']

        # Convert the row_dict back to a DataFrame
        flattened_df = pd.DataFrame(list(row_dict.values()))

        return flattened_df
    
    def aggregate_leaf(self,group,name):
        if 'LEAF_ID' not in self.columns:
            print("This function is for CAS, REF, DTM, LQ, AMT, and PLB segments only")
            return None
        grouped = group.groupby('LEAF_ID').apply(
            lambda x: {f"{row['field']}": row['value'] for _, row in x.iterrows()},
            include_groups = False
        ).reset_index(name=name)
        return grouped[name]
    
    def aggregate_leaves(self,name=None):
        if 'LEAF_ID' not in self.columns:
            print("This function is for CAS, REF, DTM, LQ, AMT, and PLB segments only")
            return None
        if not name:
            name = f"{self['segment'].iloc[0]}"
        ids = ['ISA_ID','GS_ID','ST_ID','CLP_ID','SVC_ID','level']
        test = self[ids+['LEAF_ID','field','name','value']].groupby(ids,dropna=False).apply(
            lambda x: list(self.aggregate_leaf(x, name)), 
            include_groups=False
        ).reset_index(name=name)
        return test

def pandify(data):
    def parse(data=None, prefix=None, descriptions=False, flatten=False):
        
        rows = []
        ids = {k: v for k, v in data.items() if k.endswith('_ID')}
        working_on = data.get('segment', None)

        # Handle extra fields
        for key in ['CONTROL_NUMBER', 'INTERCHANGE_DATE', 'INTERCHANGE_TIME','level']:
            if key in data:
                ids[key] = data[key]

        # Process segments 
        for segment in data.get('segments', []):
            row = ids.copy()
            row.update({'segment': segment['segment']})
            for key, d in segment.items():
                if key != 'segment':
                    if flatten:
                        col_name = key
                        if prefix:
                            col_name = f"{prefix}{col_name}"
                        if descriptions:
                            col_name = f"{col_name}_{d['name']}"
                        row[col_name] = d['value']
                    else:
                        row['field'] = key
                        row['name'] = d['name']
                        row['value'] = d['value']
                        rows.append(row.copy())
            if flatten:
                rows.append(row)

        # Process non-segment fields
        if flatten:
            for key, value in data.items():
                if not key.endswith('_id') and key != 'segments':
                    col_name = key
                    if prefix:
                        col_name = f"{prefix}{col_name}"
                    if descriptions and isinstance(value, dict):
                        col_name = f"{col_name}_{value['name']}"
                    for row in rows:
                        row[col_name] = value['value'] if isinstance(value, dict) else value

        result_df = pd.DataFrame(rows)
        return result_df
    TABLES = {}

    for name, table in data.items():
        if table:
            TABLES[name] = pd.concat([parse(x) for x in table])
            TABLES[name] = CustomDataFrame(TABLES[name])
    return TABLES