import pyx12
import pyx12.error_handler
import pyx12.x12context
import pyx12.params
import random
import string
from io import StringIO
import pandas as pd 
from .PandasClass import pandify

# Use the generate_id function to create a new object ID
def generate_id():
    # Define the character set: digits and uppercase letters
    characters = string.ascii_letters + string.digits
    
    # Generate four segments, each with six random characters
    segments = [''.join(random.choice(characters) for _ in range(6)) for _ in range(4)]
    
    # Join the segments with dashes
    api_key = '-'.join(segments)
    
    return api_key

current_state = {
    'header': {},
    'header_cas': {},
    'header_ref': {},
    'header_lq': {},
    'header_dtm': {},
    'GS':None,
    'ST':None,
    'CLP':None,
    'SVC':None,
    'CAS':None,
    'REF':None,
    'LQ':None,
    'DTM':None
}

class Parser:
    def __init__(self,filepath):
        self.filepath = filepath 
        self.load()

    def load_file_content(self):
        with open(self.filepath, 'r') as edi_file:
            return edi_file.read()

    def load_context(self):
        params = pyx12.params.params()
        errh = pyx12.error_handler.errh_null()
        edi_file_stream = StringIO(self.load_file_content())
        return pyx12.x12context.X12ContextReader(params, errh, edi_file_stream)

    def unpack(self,segment):
        seg_id = segment.id 
        seg_data = segment.seg_data 
        seg_node = segment.x12_map_node

        # Append codes
        if seg_id in ['REF','CAS','LQ','DTM']:
            seg_id += seg_data.get_value(seg_id+'01')

        result = {
            'segment': seg_id
        }
        for child in seg_node.children:
            name = child.name
            value = seg_data.get_value(child.id)
            if type(value) == str:
                value = value.strip()
            result[child.id] = {
                'name':name, 
                'value': value
            }
        return result

    def load(self):
        reader = self.load_context()

        # All available tables start empty.
        # Some of these will always be empty, but we include them for consistency.
        result = {
            'HEADER': [],
            'HEADER_CAS': [],
            'HEADER_REF': [],
            'HEADER_LQ': [],
            'HEADER_DTM': [],
            'FUNCTIONAL_GROUPS': [],
            'FUNCTIONAL_GROUPS_CAS': [],
            'FUNCTIONAL_GROUPS_REF': [],
            'FUNCTIONAL_GROUPS_LQ': [],
            'FUNCTIONAL_GROUPS_DTM': [],
            'STATEMENTS': [],
            'STATEMENTS_REF': [],
            'STATEMENTS_DTM': [],
            'CLAIMS': [],
            'CLAIMS_CAS': [],
            'CLAIMS_REF': [],
            'CLAIMS_DTM': [],
            'CLAIMS_LQ': [],
            'SERVICES': [],
            'SERVICES_CAS': [],
            'SERVICES_REF': [],
            'SERVICES_DTM': [],
            'SERVICES_LQ': [],
            'FOOTER': []
        }
        header_id = None 
        functional_group_id = None
        statement_id = None
        claim_id = None
        service_id = None

        for segment in reader.iter_segments():
            seg_id = segment.id
            seg_data = segment.seg_data 
            seg_node = segment.x12_map_node
            ######################################
            # Header
            ######################################
            if seg_id == 'ISA':
                header_id = generate_id()
                current_state['header'] = {
                    'header_id': header_id,
                    'segments':[]
                }
                current_state['header']['segments'].append(self.unpack(segment))
                result['HEADER'].append(current_state['header'])
            ######################################
            # Functional Groups
            ######################################
            elif seg_id == 'GS':
                # Add the current GS segment to the header
                if current_state['GS'] is not None:
                    result['FUNCTIONAL_GROUPS'].append(current_state['GS'])
                    # reset ids
                    functional_group_id = None
                    statement_id = None
                    claim_id = None
                    service_id = None
                
                # Reset cascading states 
                current_state['GS'] = None
                current_state['ST'] = None
                current_state['CLP'] = None
                current_state['SVC'] = None

                # Start a new current state
                functional_group_id = generate_id()
                current_state['GS'] = {
                    'header_id':header_id,
                    'functional_group_id': functional_group_id,
                    'segments':[]
                }
                current_state['GS']['segments'].append(self.unpack(segment))
            ######################################
            # Statements
            ######################################
            elif seg_id =='ST':
                # Add the current ST segment to the functional group
                if current_state['ST'] is not None:
                    result['STATEMENTS'].append(current_state['ST'])
                    # reset ids
                    statement_id = None
                    claim_id = None
                    service_id = None

                # Reset cascading states 
                current_state['ST'] = None
                current_state['CLP'] = None
                current_state['SVC'] = None
                
                # Start a new current state
                statement_id = generate_id()
                current_state['ST'] = {
                    'header_id': header_id,
                    'functional_group_id':functional_group_id,
                    'statement_id': statement_id,
                    'segments':[]
                }
                current_state['ST']['segments'].append(self.unpack(segment))
            ######################################
            # Claims
            ######################################
            elif seg_id == 'CLP':
                # Add the current CLP segment to the statement
                if current_state['CLP'] is not None:
                    result['CLAIMS'].append(current_state['CLP'])
                    # reset ids
                    claim_id = None
                    service_id = None
                    cas_id = None
                    ref_id = None
                    lq_id = None
                    dtm_id = None

                # Reset cascading states
                current_state['CLP'] = None
                current_state['SVC'] = None
                
                # Start a new current state
                claim_id = generate_id()
                current_state['CLP'] = {
                    'header_id': header_id,
                    'functional_group_id': functional_group_id,
                    'statement_id': statement_id,
                    'claim_id': claim_id,
                    'segments':[]
                }
                current_state['CLP']['segments'].append(self.unpack(segment))
            ######################################
            # Services
            ######################################
            elif seg_id == 'SVC':
                # Add the current SVC segment to the claim
                if current_state['SVC'] is not None:
                    result['SERVICES'].append(current_state['SVC'])
                    # reset ids
                    service_id = None

                # Reset cascading states
                current_state['SVC'] = None

                # Start a new current state
                service_id = generate_id()
                current_state['SVC'] = {
                    'header_id': header_id,
                    'functional_group_id': functional_group_id,
                    'statement_id': statement_id,
                    'claim_id': claim_id,
                    'service_id': service_id,
                    'segments':[]
                }
                current_state['SVC']['segments'].append(self.unpack(segment))
            ######################################
            # CAS
            ######################################
            elif seg_id == 'CAS':
                current_state['CAS'] = None
                # Start a new current state
                cas_id = generate_id()
                current_state['CAS'] = {
                    'header_id': header_id,
                    'functional_group_id': functional_group_id,
                    'statement_id': statement_id,
                    'claim_id': claim_id,
                    'service_id': service_id,
                    'cas_id': cas_id,
                    'segments':[]
                }
                if current_state['SVC'] is not None:
                    current_state['CAS']['segments'].append(self.unpack(segment))
                    result['SERVICES_CAS'].append(current_state['CAS'])
                elif current_state['CLP'] is not None:
                    current_state['CAS'].pop('service_id')
                    current_state['CAS']['segments'].append(self.unpack(segment))
                    result['CLAIMS_CAS'].append(current_state['CAS'])
                elif current_state['ST'] is not None:
                    current_state['CAS'].pop('service_id')
                    current_state['CAS'].pop('claim_id')
                    current_state['CAS']['segments'].append(self.unpack(segment))
                    result['GS'].append(current_state['CAS'])
                elif current_state['FUNCTIONAL_GROUPS'] is not None:
                    current_state['CAS'].pop('service_id')
                    current_state['CAS'].pop('claim_id')
                    current_state['CAS'].pop('statement_id')
                    current_state['CAS']['segments'].append(self.unpack(segment))
                    result['FUNCTIONAL_GROUPS_CAS'].append(current_state['CAS'])
                else:
                    current_state['CAS'].pop('service_id')
                    current_state['CAS'].pop('claim_id')
                    current_state['CAS'].pop('statement_id')
                    current_state['CAS'].pop('functional_group_id')
                    current_state['CAS']['segments'].append(self.unpack(segment))
                    result['HEADER_CAS'].append(current_state['CAS'])
            ######################################
            # REF
            ######################################
            elif seg_id == 'REF':
                # Start a new current state
                ref_id = generate_id()
                current_state['REF'] = {
                    'header_id': header_id,
                    'functional_group_id': functional_group_id,
                    'statement_id': statement_id,
                    'claim_id': claim_id,
                    'service_id': service_id,
                    'ref_id': ref_id,
                    'segments':[]
                }
                if current_state['SVC'] is not None:
                    current_state['REF']['segments'].append(self.unpack(segment))
                    result['SERVICES_REF'].append(current_state['REF'])
                elif current_state['CLP'] is not None:
                    current_state['REF'].pop('service_id')
                    current_state['REF']['segments'].append(self.unpack(segment))
                    result['CLAIMS_REF'].append(current_state['REF'])
                elif current_state['ST'] is not None:
                    current_state['REF'].pop('service_id')
                    current_state['REF'].pop('claim_id')
                    current_state['REF']['segments'].append(self.unpack(segment))
                    result['STATEMENTS_REF'].append(current_state['REF'])
                elif current_state['GS'] is not None:
                    current_state['REF'].pop('service_id')
                    current_state['REF'].pop('claim_id')
                    current_state['REF'].pop('statement_id')
                    current_state['REF']['segments'].append(self.unpack(segment))
                    result['FUNCTIONAL_GROUPS_REF'].append(current_state['REF'])
                else:
                    current_state['REF'].pop('service_id')
                    current_state['REF'].pop('claim_id')
                    current_state['REF'].pop('statement_id')
                    current_state['REF'].pop('functional_group_id')
                    current_state['REF']['segments'].append(self.unpack(segment))
                    result['HEADER_REF'].append(current_state['REF'])
            ######################################
            # LQs
            ######################################
            elif seg_id == 'LQ':
                # Reset cascading states
                current_state['LQ'] = None
                
                # Start a new current state
                lq_id = generate_id()
                current_state['LQ'] = {
                    'header_id': header_id,
                    'functional_group_id': functional_group_id,
                    'statement_id': statement_id,
                    'claim_id': claim_id,
                    'service_id': service_id,
                    'lq_id': lq_id,
                    'segments':[]
                }
                if current_state['SVC'] is not None:
                    current_state['LQ']['segments'].append(self.unpack(segment))
                    result['SERVICES_LQ'].append(current_state['LQ'])
                elif current_state['CLP'] is not None:
                    current_state['LQ'].pop('service_id')
                    current_state['LQ']['segments'].append(self.unpack(segment))
                    result['CLAIMS_LQ'].append(current_state['LQ'])
                elif current_state['ST'] is not None:
                    current_state['LQ'].pop('service_id')
                    current_state['LQ'].pop('claim_id')
                    current_state['LQ']['segments'].append(self.unpack(segment))
                    result['STATEMENTS_LQ'].append(current_state['LQ'])
                elif current_state['GS'] is not None:
                    current_state['LQ'].pop('service_id')
                    current_state['LQ'].pop('claim_id')
                    current_state['LQ'].pop('statement_id')
                    current_state['LQ']['segments'].append(self.unpack(segment))
                    result['FUNCTIONAL_GROUPS_LQ'].append(current_state['LQ'])
                else:
                    current_state['LQ'].pop('service_id')
                    current_state['LQ'].pop('claim_id')
                    current_state['LQ'].pop('statement_id')
                    current_state['LQ'].pop('functional_group_id')
                    current_state['LQ']['segments'].append(self.unpack(segment))
                    result['HEADER_LQ'].append(current_state['LQ'])
            ######################################
            # DTMs
            ######################################
            elif seg_id == 'DTM':
                # Reset cascading states
                current_state['DTM'] = None
                
                # Start a new current state
                dtm_id = generate_id()
                current_state['DTM'] = {
                    'header_id': header_id,
                    'functional_group_id': functional_group_id,
                    'statement_id': statement_id,
                    'claim_id': claim_id,
                    'service_id': service_id,
                    'dtm_id': dtm_id,
                    'segments':[]
                }
                if current_state['SVC'] is not None:
                    current_state['DTM']['segments'].append(self.unpack(segment))
                    result['SERVICES_DTM'].append(current_state['DTM'])
                elif current_state['CLP'] is not None:
                    current_state['DTM'].pop('service_id')
                    current_state['DTM']['segments'].append(self.unpack(segment))
                    result['CLAIMS_DTM'].append(current_state['DTM'])
                elif current_state['ST'] is not None:
                    current_state['DTM'].pop('service_id')
                    current_state['DTM'].pop('claim_id')
                    current_state['DTM']['segments'].append(self.unpack(segment))
                    result['STATEMENTS_DTM'].append(current_state['DTM'])
                elif current_state['GS'] is not None:
                    current_state['DTM'].pop('service_id')
                    current_state['DTM'].pop('claim_id')
                    current_state['DTM'].pop('statement_id')
                    current_state['DTM']['segments'].append(self.unpack(segment))
                    result['FUNCTIONAL_GROUPS_DTM'].append(current_state['DTM'])
                else:
                    current_state['DTM'].pop('service_id')
                    current_state['DTM'].pop('claim_id')
                    current_state['DTM'].pop('statement_id')
                    current_state['DTM'].pop('functional_group_id')
                    current_state['DTM']['segments'].append(self.unpack(segment))
                    result['HEADER_DTM'].append(current_state['DTM'])

            ######################################
            # End of a Statement
            ######################################
            elif seg_id == 'SE':
                # Add the last CLP segment if exists
                if current_state['CLP'] is not None:
                    result['CLAIMS'].append(current_state['CLP'])
                # Add the last service if exists 
                if current_state['SVC'] is not None:
                    result['SERVICES'].append(current_state['SVC'])
                # Add the last claim if exists
                if current_state['CLP'] is not None:
                    result['CLAIMS'].append(current_state['CLP'])
                # Add the last statement if exists
                if current_state['ST'] is not None:
                    result['STATEMENTS'].append(current_state['ST'])
                
                # Reset cascading states 
                current_state['ST'] = None
                current_state['CLP'] = None
                current_state['SVC'] = None
                current_state['CAS'] = None
            ######################################
            # End of a functional group
            ######################################
            elif seg_id == 'GE':
                # Add the last CLP segment if exists
                if current_state['CLP'] is not None:
                    result['CLAIMS'].append(current_state['CLP'])
                # Add the last service if exists 
                if current_state['SVC'] is not None:
                    result['SERVICES'].append(current_state['SVC'])
                # Add the last claim if exists
                if current_state['CLP'] is not None:
                    result['CLAIMS'].append(current_state['CLP'])
                # Add the last statement if exists
                if current_state['ST'] is not None:
                    result['STATEMENTS'].append(current_state['ST'])
                # Add the last functional group if exists
                if current_state['GS'] is not None:
                    result['FUNCTIONAL_GROUPS'].append(current_state['GS'])
                
                # Reset cascading states 
                current_state['GS'] = None
                current_state['ST'] = None
                current_state['CLP'] = None
                current_state['SVC'] = None
                current_state['CAS'] = None
            ######################################
            # End of file
            ######################################
            elif seg_id == 'IEA':
                # Add the last CLP segment if exists
                if current_state['CLP'] is not None:
                    result['CLAIMS'].append(current_state['CLP'])
                # Add the last service if exists 
                if current_state['SVC'] is not None:
                    result['SERVICES'].append(current_state['SVC'])
                # Add the last claim if exists
                if current_state['CLP'] is not None:
                    result['CLAIMS'].append(current_state['CLP'])
                # Add the last statement if exists
                if current_state['ST'] is not None:
                    result['STATEMENTS'].append(current_state['ST'])
                # Add the last functional group if exists
                if current_state['GS'] is not None:
                    result['FUNCTIONAL_GROUPS'].append(current_state['GS'])
                current_state['CLP'] = {
                    'header_id': header_id
                }
                result['FOOTER'].append(self.unpack(segment))
            else:
                if current_state['SVC'] is not None:
                    current_state['SVC'].update(self.unpack(segment))
                elif current_state['CLP'] is not None:
                    current_state['CLP'].update(self.unpack(segment))
                elif current_state['ST'] is not None:
                    current_state['ST'].update(self.unpack(segment))
                elif current_state['GS'] is not None:
                    current_state['GS'].update(self.unpack(segment))
                else:
                    result['HEADER'].append(self.unpack(segment))

        self.dict = {x:result[x] for x in result if result[x]}
        self.TABLES = pandify(self.dict)

    def flatten(self,prefix = None,table_names = True, descriptions=False):
        flattend_dfs = {}
        for key, table in self.TABLES.items():
            prefix_string = ''
            if prefix:
                prefix_string = f"{prefix}"
            if table_names:
                prefix_string = f"{prefix_string}{key} "
            flattend_dfs[key] = table.flatten(prefix=prefix_string,descriptions=descriptions)


        df = flattend_dfs['HEADER']
        if 'HEADER_CAS' in flattend_dfs:
            df = df.merge(flattend_dfs['HEADER_CAS'],on=['header_id'],how='left')
        if 'HEADER_REF' in flattend_dfs:
            df = df.merge(flattend_dfs['HEADER_REF'],on=['header_id'],how='left')
        if 'HEADER_LQ' in flattend_dfs:
            df = df.merge(flattend_dfs['HEADER_LQ'],on=['header_id'],how='left')
        if 'HEADER_DTM' in flattend_dfs:
            df = df.merge(flattend_dfs['HEADER_DTM'],on=['header_id'],how='left')
        if 'FUNCTIONAL_GROUPS' in flattend_dfs.keys():
            df = df.merge(flattend_dfs['FUNCTIONAL_GROUPS'],on=['header_id'],how='left')
        if 'FUNCTIONAL_GROUPS_CAS' in flattend_dfs:
            df = df.merge(flattend_dfs['FUNCTIONAL_GROUPS_CAS'],on=['header_id','functional_group_id'],how='left')
        if 'FUNCTIONAL_GROUPS_REF' in flattend_dfs:
            df = df.merge(flattend_dfs['FUNCTIONAL_GROUPS_REF'],on=['header_id','functional_group_id'],how='left')
        if 'FUNCTIONAL_GROUPS_LQ' in flattend_dfs:
            df = df.merge(flattend_dfs['FUNCTIONAL_GROUPS_LQ'],on=['header_id','functional_group_id'],how='left')
        if 'FUNCTIONAL_GROUPS_DTM' in flattend_dfs:
            df = df.merge(flattend_dfs['FUNCTIONAL_GROUPS_DTM'],on=['header_id','functional_group_id'],how='left')
        if 'STATEMENTS' in flattend_dfs:
            df = df.merge(flattend_dfs['STATEMENTS'],on=['header_id','functional_group_id'],how='left')
        if 'STATEMENTS_CAS' in flattend_dfs:
            df = df.merge(flattend_dfs['STATEMENTS_CAS'],on=['header_id','functional_group_id','statement_id'],how='left')
        if 'STATEMENTS_REF' in flattend_dfs:
            df = df.merge(flattend_dfs['STATEMENTS_REF'],on=['header_id','functional_group_id','statement_id'],how='left')
        if 'STATEMENTS_LQ' in flattend_dfs:
            df = df.merge(flattend_dfs['STATEMENTS_LQ'],on=['header_id','functional_group_id','statement_id'],how='left')
        if 'STATEMENTS_DTM' in flattend_dfs:
            df = df.merge(flattend_dfs['STATEMENTS_DTM'],on=['header_id','functional_group_id','statement_id'],how='left')
        if 'CLAIMS' in flattend_dfs:
            df = df.merge(flattend_dfs['CLAIMS'],on=['header_id','functional_group_id','statement_id'],how='left')
        if 'CLAIMS_CAS' in flattend_dfs:
            df = df.merge(flattend_dfs['CLAIMS_CAS'],on=['header_id','functional_group_id','statement_id','claim_id'],how='left')
        if 'CLAIMS_REF' in flattend_dfs:
            df = df.merge(flattend_dfs['CLAIMS_REF'],on=['header_id','functional_group_id','statement_id','claim_id'],how='left')
        if 'CLAIMS_LQ' in flattend_dfs:
            df = df.merge(flattend_dfs['CLAIMS_LQ'],on=['header_id','functional_group_id','statement_id','claim_id'],how='left')
        if 'CLAIMS_DTM' in flattend_dfs:
            df = df.merge(flattend_dfs['CLAIMS_DTM'],on=['header_id','functional_group_id','statement_id','claim_id'],how='left')
        if 'SERVICES' in flattend_dfs:
            df = df.merge(flattend_dfs['SERVICES'],on=['header_id','functional_group_id','statement_id','claim_id'],how='left')
        if 'SERVICES_CAS' in flattend_dfs:
            df = df.merge(flattend_dfs['SERVICES_CAS'],on=['header_id','functional_group_id','statement_id','claim_id','service_id'],how='left')
        if 'SERVICES_REF' in flattend_dfs:
            df = df.merge(flattend_dfs['SERVICES_REF'],on=['header_id','functional_group_id','statement_id','claim_id','service_id'],how='left')
        if 'SERVICES_LQ' in flattend_dfs:
            df = df.merge(flattend_dfs['SERVICES_LQ'],on=['header_id','functional_group_id','statement_id','claim_id','service_id'],how='left')
        if 'SERVICES_DTM' in flattend_dfs:
            df = df.merge(flattend_dfs['SERVICES_DTM'],on=['header_id','functional_group_id','statement_id','claim_id','service_id'],how='left')
        return df
