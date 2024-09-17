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
    'ISA': {},
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
        seg_data = segment.seg_data 
        seg_node = segment.x12_map_node

        amend_list = ['NM1','N101','N1','N2','N3','N4','DTM','PER']

        result = {
            'segment': segment.id + seg_data.get_value(segment.id+'01') if segment.id in amend_list else segment.id
        }
        for child in seg_node.children:
            if segment.id in amend_list:
                field = child.id + seg_data.get_value(segment.id+'01')
            else:
                field = child.id
            name = child.name
            value = seg_data.get_value(child.id)
            if type(value) == str:
                value = value.strip()
            result[field] = {
                'name':name, 
                'value': value
            }
        return result

    def load(self):
        reader = self.load_context()

        # All available tables start empty.
        # Some of these will always be empty, but we include them for consistency.
        result = {
            'ISA': [],
            'GS': [],
            'ST': [],
            'CLP': [],
            'SVC': [],
            'CAS': [],
            'REF': [],
            'LQ': [],
            'DTM': [],
            'AMT': [],
            'PLB': [],
            'NM1': [],
            'MOA': [],
            'FOOTER': []
        }
        ISA_ID = None 
        GS_ID = None
        ST_ID = None
        CLP_ID = None
        SVC_ID = None
        CAS_ID = None
        REF_ID = None
        LQ_ID = None
        DTM_ID = None
        AMT_ID = None
        NM1_ID = None
        MOA_ID = None
        PLB_ID = None
        def get_current_level():
            if SVC_ID is not None:
                return 'SVC'
            elif CLP_ID is not None:
                return 'CLP'
            elif ST_ID is not None:
                return 'ST'
            elif GS_ID is not None:
                return 'GS'
            else:
                return 'ISA'
        for segment in reader.iter_segments():
            seg_id = segment.id
            seg_data = segment.seg_data 
            seg_node = segment.x12_map_node
            ######################################
            # Header
            ######################################
            if seg_id == 'ISA':
                ISA_ID = generate_id()
                current_state['ISA'] = {
                    'ISA_ID': ISA_ID,
                    'segments':[]
                }
                current_state['ISA']['segments'].append(self.unpack(segment))
                result['ISA'].append(current_state['ISA'])
            ######################################
            # Functional Groups
            ######################################
            elif seg_id == 'GS':
                # Add the current GS segment to the ISA
                if current_state['GS'] is not None:
                    result['GS'].append(current_state['GS'])
                    # reset ids
                    GS_ID = None
                    ST_ID = None
                    CLP_ID = None
                    SVC_ID = None
                
                # Reset cascading states 
                current_state['GS'] = None
                current_state['ST'] = None
                current_state['CLP'] = None
                current_state['SVC'] = None

                # Start a new current state
                GS_ID = generate_id()
                current_state['GS'] = {
                    'ISA_ID':ISA_ID,
                    'GS_ID': GS_ID,
                    'segments':[]
                }
                current_state['GS']['segments'].append(self.unpack(segment))
            ######################################
            # Statements
            ######################################
            elif seg_id =='ST':
                # Add the current ST segment to the functional group
                if current_state['ST'] is not None:
                    result['ST'].append(current_state['ST'])
                    # reset ids
                    ST_ID = None
                    CLP_ID = None
                    SVC_ID = None

                # Reset cascading states 
                current_state['ST'] = None
                current_state['CLP'] = None
                current_state['SVC'] = None
                
                # Start a new current state
                ST_ID = generate_id()
                current_state['ST'] = {
                    'ISA_ID': ISA_ID,
                    'GS_ID':GS_ID,
                    'ST_ID': ST_ID,
                    'segments':[]
                }
                current_state['ST']['segments'].append(self.unpack(segment))
            ######################################
            # Claims
            ######################################
            elif seg_id == 'CLP':
                # Add the current CLP segment to the statement
                if current_state['CLP'] is not None:
                    result['CLP'].append(current_state['CLP'])
                    # reset ids
                    CLP_ID = None
                    SVC_ID = None
                    CAS_ID = None
                    REF_ID = None
                    LQ_ID = None
                    DTM_ID = None

                # Reset cascading states
                current_state['CLP'] = None
                current_state['SVC'] = None
                
                # Start a new current state
                CLP_ID = generate_id()
                current_state['CLP'] = {
                    'ISA_ID': ISA_ID,
                    'GS_ID': GS_ID,
                    'ST_ID': ST_ID,
                    'CLP_ID': CLP_ID,
                    'segments':[]
                }
                current_state['CLP']['segments'].append(self.unpack(segment))
            ######################################
            # Services
            ######################################
            elif seg_id == 'SVC':
                # Add the current SVC segment to the claim
                if current_state['SVC'] is not None:
                    result['SVC'].append(current_state['SVC'])
                    # reset ids
                    SVC_ID = None

                # Reset cascading states
                current_state['SVC'] = None

                # Start a new current state
                SVC_ID = generate_id()
                current_state['SVC'] = {
                    'ISA_ID': ISA_ID,
                    'GS_ID': GS_ID,
                    'ST_ID': ST_ID,
                    'CLP_ID': CLP_ID,
                    'SVC_ID': SVC_ID,
                    'segments':[]
                }
                current_state['SVC']['segments'].append(self.unpack(segment))
            ######################################
            # CAS
            ######################################
            elif seg_id == 'CAS':
                current_state['CAS'] = None
                # Start a new current state
                CAS_ID = generate_id()
                current_state['CAS'] = {
                    'ISA_ID': ISA_ID,
                    'GS_ID': GS_ID,
                    'ST_ID': ST_ID,
                    'CLP_ID': CLP_ID,
                    'SVC_ID': SVC_ID,
                    'level': get_current_level(),
                    'CAS_ID': CAS_ID,
                    'segments':[]
                }
                current_state['CAS']['segments'].append(self.unpack(segment))
                result['CAS'].append(current_state['CAS'])
            ######################################
            # REF
            ######################################
            elif seg_id == 'REF':
                # Start a new current state
                REF_ID = generate_id()
                current_state['REF'] = {
                    'ISA_ID': ISA_ID,
                    'GS_ID': GS_ID,
                    'ST_ID': ST_ID,
                    'CLP_ID': CLP_ID,
                    'SVC_ID': SVC_ID,
                    'level': get_current_level(),
                    'REF_ID': REF_ID,
                    'segments':[]
                }
                current_state['REF']['segments'].append(self.unpack(segment))
                result['REF'].append(current_state['REF'])
            ######################################
            # LQs
            ######################################
            elif seg_id == 'LQ':
                # Reset cascading states
                current_state['LQ'] = None
                
                # Start a new current state
                LQ_ID = generate_id()
                current_state['LQ'] = {
                    'ISA_ID': ISA_ID,
                    'GS_ID': GS_ID,
                    'ST_ID': ST_ID,
                    'CLP_ID': CLP_ID,
                    'SVC_ID': SVC_ID,
                    'level': get_current_level(),
                    'LQ_ID': LQ_ID,
                    'segments':[]
                }
                current_state['LQ']['segments'].append(self.unpack(segment))
                result['LQ'].append(current_state['LQ'])
            ######################################
            # DTMs
            ######################################
            elif seg_id == 'DTM':
                # Reset cascading states
                current_state['DTM'] = None
                
                # Start a new current state
                DTM_ID = generate_id()
                current_state['DTM'] = {
                    'ISA_ID': ISA_ID,
                    'GS_ID': GS_ID,
                    'ST_ID': ST_ID,
                    'CLP_ID': CLP_ID,
                    'SVC_ID': SVC_ID,
                    'level': get_current_level(),
                    'DTM_ID': DTM_ID,
                    'segments':[]
                }
                current_state['DTM']['segments'].append(self.unpack(segment))
                result['DTM'].append(current_state['DTM'])
            ######################################
            # AMT
            ######################################
            elif seg_id == 'AMT':
                # Reset cascading states
                current_state['AMT'] = None
                
                # Start a new current state
                AMT_ID = generate_id()
                current_state['AMT'] = {
                    'ISA_ID': ISA_ID,
                    'GS_ID': GS_ID,
                    'ST_ID': ST_ID,
                    'CLP_ID': CLP_ID,
                    'SVC_ID': SVC_ID,
                    'level': get_current_level(),
                    'AMT_ID': AMT_ID,
                    'segments':[]
                }
                current_state['AMT']['segments'].append(self.unpack(segment))
                result['AMT'].append(current_state['AMT'])
            ######################################
            # PLB
            ######################################
            elif seg_id == 'PLB':
                # Reset cascading states
                current_state['PLB'] = None
                
                # Start a new current state
                PLB_ID = generate_id()
                current_state['PLB'] = {
                    'ISA_ID': ISA_ID,
                    'GS_ID': GS_ID,
                    'ST_ID': ST_ID,
                    'CLP_ID': CLP_ID,
                    'SVC_ID': SVC_ID,
                    'level': get_current_level(),
                    'PLB_ID': PLB_ID,
                    'segments':[]
                }
                current_state['PLB']['segments'].append(self.unpack(segment))
                result['PLB'].append(current_state['PLB'])
            ######################################
            # End of a Statement
            ######################################
            elif seg_id == 'SE':
                # Add the last CLP segment if exists
                if current_state['CLP'] is not None:
                    result['CLP'].append(current_state['CLP'])
                    current_state['CLP'] = None
                # Add the last service if exists 
                if current_state['SVC'] is not None:
                    result['SVC'].append(current_state['SVC'])
                    current_state['SVC'] = None
                # Add the last statement if exists
                if current_state['ST'] is not None:
                    result['ST'].append(current_state['ST'])
                    current_state['ST'] = None
            ######################################
            # End of a functional group
            ######################################
            elif seg_id == 'GE':
                # Add the last CLP segment if exists
                if current_state['CLP'] is not None:
                    result['CLP'].append(current_state['CLP'])
                    current_state['CLP'] = None
                # Add the last service if exists 
                if current_state['SVC'] is not None:
                    result['SVC'].append(current_state['SVC'])
                    current_state['SVC'] = None
                # Add the last statement if exists
                if current_state['ST'] is not None:
                    result['SVC'].append(current_state['ST'])
                    current_state['ST'] = None
                # Add the last functional group if exists
                if current_state['GS'] is not None:
                    result['GS'].append(current_state['GS'])
                    current_state['GS'] = None
                
            ######################################
            # End of file
            ######################################
            elif seg_id == 'IEA':
                # Add the last CLP segment if exists
                if current_state['CLP'] is not None:
                    result['CLP'].append(current_state['CLP'])
                    current_state['CLP'] = None
                # Add the last service if exists 
                if current_state['SVC'] is not None:
                    result['SVC'].append(current_state['SVC'])
                    current_state['SVC'] = None
                # Add the last statement if exists
                if current_state['ST'] is not None:
                    result['SVC'].append(current_state['ST'])
                    current_state['ST'] = None
                # Add the last functional group if exists
                if current_state['GS'] is not None:
                    result['GS'].append(current_state['GS'])
                    current_state['GS'] = None
                current_state['FOOTER'] = {
                    'ISA_ID': ISA_ID
                }
                current_state['FOOTER'].update(self.unpack(segment))
                result['FOOTER'].append(current_state['FOOTER'])
            else:
                if current_state['SVC'] is not None:
                    current_state['SVC']['segments'].append(self.unpack(segment))
                elif current_state['CLP'] is not None:
                    current_state['CLP']['segments'].append(self.unpack(segment))
                elif current_state['ST'] is not None:
                    current_state['ST']['segments'].append(self.unpack(segment))
                elif current_state['GS'] is not None:
                    current_state['GS']['segments'].append(self.unpack(segment))
                else:
                    result['ISA'].append(self.unpack(segment))

        self.dict = {x:result[x] for x in result if result[x]}
        self.TABLES = pandify(self.dict)

    # def flatten(self,prefix = None,table_names = True, descriptions=False):
    #     flattend_dfs = {}
    #     for key, table in self.TABLES.items():
    #         prefix_string = ''
    #         if prefix:
    #             prefix_string = f"{prefix}"
    #         if table_names:
    #             prefix_string = f"{prefix_string}{key} "
    #         flattend_dfs[key] = table.flatten(prefix=prefix_string,descriptions=descriptions)


    #     df = flattend_dfs['ISA']
    #     if 'CAS' in flattend_dfs:
    #         df = df.merge(flattend_dfs['CAS'],on=['ISA_ID'],how='left')
    #     if 'ISA_REF' in flattend_dfs:
    #         df = df.merge(flattend_dfs['ISA_REF'],on=['ISA_ID'],how='left')
    #     if 'ISA_LQ' in flattend_dfs:
    #         df = df.merge(flattend_dfs['ISA_LQ'],on=['ISA_ID'],how='left')
    #     if 'ISA_DTM' in flattend_dfs:
    #         df = df.merge(flattend_dfs['ISA_DTM'],on=['ISA_ID'],how='left')
    #     if 'GS' in flattend_dfs.keys():
    #         df = df.merge(flattend_dfs['GS'],on=['ISA_ID'],how='left')
    #     if 'GS_REF' in flattend_dfs:
    #         df = df.merge(flattend_dfs['GS_REF'],on=['ISA_ID','GS_ID'],how='left')
    #     if 'GS_LQ' in flattend_dfs:
    #         df = df.merge(flattend_dfs['GS_LQ'],on=['ISA_ID','GS_ID'],how='left')
    #     if 'GS_DTM' in flattend_dfs:
    #         df = df.merge(flattend_dfs['GS_DTM'],on=['ISA_ID','GS_ID'],how='left')
    #     if 'SVC' in flattend_dfs:
    #         df = df.merge(flattend_dfs['SVC'],on=['ISA_ID','GS_ID'],how='left')
    #     if 'SVC_REF' in flattend_dfs:
    #         df = df.merge(flattend_dfs['SVC_REF'],on=['ISA_ID','GS_ID','ST_ID'],how='left')
    #     if 'SVC_LQ' in flattend_dfs:
    #         df = df.merge(flattend_dfs['SVC_LQ'],on=['ISA_ID','GS_ID','ST_ID'],how='left')
    #     if 'SVC_DTM' in flattend_dfs:
    #         df = df.merge(flattend_dfs['SVC_DTM'],on=['ISA_ID','GS_ID','ST_ID'],how='left')
    #     if 'CLP' in flattend_dfs:
    #         df = df.merge(flattend_dfs['CLP'],on=['ISA_ID','GS_ID','ST_ID'],how='left')
    #     if 'CLP_REF' in flattend_dfs:
    #         df = df.merge(flattend_dfs['CLP_REF'],on=['ISA_ID','GS_ID','ST_ID','CLP_ID'],how='left')
    #     if 'CLP_LQ' in flattend_dfs:
    #         df = df.merge(flattend_dfs['CLP_LQ'],on=['ISA_ID','GS_ID','ST_ID','CLP_ID'],how='left')
    #     if 'CLP_DTM' in flattend_dfs:
    #         df = df.merge(flattend_dfs['CLP_DTM'],on=['ISA_ID','GS_ID','ST_ID','CLP_ID'],how='left')
    #     if 'SVC' in flattend_dfs:
    #         df = df.merge(flattend_dfs['SVC'],on=['ISA_ID','GS_ID','ST_ID','CLP_ID'],how='left')
    #     if 'SVC_CAS' in flattend_dfs:
    #         df = df.merge(flattend_dfs['SVC_CAS'],on=['ISA_ID','GS_ID','ST_ID','CLP_ID','SVC_ID'],how='left')
    #     if 'SVC_REF' in flattend_dfs:
    #         df = df.merge(flattend_dfs['SVC_REF'],on=['ISA_ID','GS_ID','ST_ID','CLP_ID','SVC_ID'],how='left')
    #     if 'SVC_LQ' in flattend_dfs:
    #         df = df.merge(flattend_dfs['SVC_LQ'],on=['ISA_ID','GS_ID','ST_ID','CLP_ID','SVC_ID'],how='left')
    #     if 'SVC_DTM' in flattend_dfs:
    #         df = df.merge(flattend_dfs['SVC_DTM'],on=['ISA_ID','GS_ID','ST_ID','CLP_ID','SVC_ID'],how='left')
    #     return df
