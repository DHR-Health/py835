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
        LEAF_ID = None
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
                current_state['CAS'] = {
                    'ISA_ID': ISA_ID,
                    'GS_ID': GS_ID,
                    'ST_ID': ST_ID,
                    'CLP_ID': CLP_ID,
                    'SVC_ID': SVC_ID,
                    'level': get_current_level(),
                    'LEAF_ID': generate_id(),
                    'segments':[]
                }
                current_state['CAS']['segments'].append(self.unpack(segment))
                result['CAS'].append(current_state['CAS'])
            ######################################
            # REF
            ######################################
            elif seg_id == 'REF':
                # Start a new current state
                current_state['REF'] = {
                    'ISA_ID': ISA_ID,
                    'GS_ID': GS_ID,
                    'ST_ID': ST_ID,
                    'CLP_ID': CLP_ID,
                    'SVC_ID': SVC_ID,
                    'level': get_current_level(),
                    'LEAF_ID': generate_id(),
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
                current_state['LQ'] = {
                    'ISA_ID': ISA_ID,
                    'GS_ID': GS_ID,
                    'ST_ID': ST_ID,
                    'CLP_ID': CLP_ID,
                    'SVC_ID': SVC_ID,
                    'level': get_current_level(),
                    'LEAF_ID': generate_id(),
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
                current_state['DTM'] = {
                    'ISA_ID': ISA_ID,
                    'GS_ID': GS_ID,
                    'ST_ID': ST_ID,
                    'CLP_ID': CLP_ID,
                    'SVC_ID': SVC_ID,
                    'level': get_current_level(),
                    'LEAF_ID': generate_id(),
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
                current_state['AMT'] = {
                    'ISA_ID': ISA_ID,
                    'GS_ID': GS_ID,
                    'ST_ID': ST_ID,
                    'CLP_ID': CLP_ID,
                    'SVC_ID': SVC_ID,
                    'level': get_current_level(),
                    'LEAF_ID': generate_id(),
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
                current_state['PLB'] = {
                    'ISA_ID': ISA_ID,
                    'GS_ID': GS_ID,
                    'ST_ID': ST_ID,
                    'CLP_ID': CLP_ID,
                    'SVC_ID': SVC_ID,
                    'level': get_current_level(),
                    'LEAF_ID': generate_id(),
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

    def aggregate_leaf(self,group,name):
        grouped = group.groupby('LEAF_ID').apply(
            lambda x: {f"{row['field']}": row['value'] for _, row in x.iterrows()},
            include_groups = False
        ).reset_index(name=name)
        return grouped[name]
    def aggregate_leaves(self,df,name):
        ids = ['ISA_ID','GS_ID','ST_ID','CLP_ID','SVC_ID','level']
        test = df[ids+['LEAF_ID','field','name','value']].groupby(ids,dropna=False).apply(
            lambda x: list(self.aggregate_leaf(x, name)), 
            include_groups=False
        ).reset_index(name=name)
        return test
    def combine_CAS(self):
        return self.aggregate_leaves(self.TABLES['CAS'],'CAS')
    def combine_REF(self):
        return self.aggregate_leaves(self.TABLES['REF'],'REF')
    def combine_LQ(self):
        return self.aggregate_leaves(self.TABLES['LQ'],'LQ')
    def combine_DTM(self):
        return self.aggregate_leaves(self.TABLES['DTM'],'DTM')
    def combine_AMT(self):
        return self.aggregate_leaves(self.TABLES['AMT'],'AMT')
    def combine_PLB(self):
        return self.aggregate_leaves(self.TABLES['PLB'],'PLB')
    
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
    #         ISA_CAS = flattend_dfs['CAS'][flattend_dfs['CAS']['level'] == 'ISA']
    #         # Concatenate to create a single line per ISA
    #         ISA_CAS = ISA_CAS.groupby('ISA_ID').agg(lambda x: x.tolist()).reset_index()
    #         if ISA_CAS.shape[0] > 0:
    #             df = df.merge(ISA_CAS,on=['ISA_ID'],how='left')
            
    #     if 'REF' in flattend_dfs:
    #         ISA_REF = flattend_dfs['REF'][flattend_dfs['REF']['level'] == 'ISA']
    #         if ISA_REF.shape[0] > 0:
    #             df = df.merge(ISA_REF,on=['ISA_ID'],how='left')
    #     if 'DTM' in flattend_dfs:
    #         ISA_DTM = flattend_dfs['DTM'][flattend_dfs['DTM']['level'] == 'ISA']
    #         if ISA_DTM.shape[0] > 0:
    #             df = df.merge(ISA_DTM,on=['ISA_ID'],how='left')
    #     if 'LQ' in flattend_dfs:
    #         ISA_LQ = flattend_dfs['LQ'][flattend_dfs['LQ']['level'] == 'ISA']
    #         if ISA_LQ.shape[0] > 0:
    #             df = df.merge(ISA_LQ,on=['ISA_ID'],how='left')
    #     if 'AMT' in flattend_dfs:
    #         ISA_AMT = flattend_dfs['AMT'][flattend_dfs['AMT']['level'] == 'ISA']
    #         if ISA_AMT.shape[0] > 0:
    #             df = df.merge(ISA_AMT,on=['ISA_ID'],how='left')
    #     if 'PLB' in flattend_dfs:
    #         ISA_PLB = flattend_dfs['PLB'][flattend_dfs['PLB']['level'] == 'ISA']
    #         if ISA_PLB.shape[0] > 0:
    #             df = df.merge(ISA_PLB,on=['ISA_ID'],how='left')

    #     if 'GS' in flattend_dfs:
    #         df = df.merge(flattend_dfs['GS'],on=['ISA_ID'],how='left')
    #     if 'CAS' in flattend_dfs:
    #         GS_CASE = flattend_dfs['CAS'][flattend_dfs['CAS']['level'] == 'GS']
    #         if GS_CASE.shape[0] > 0:
    #             df = df.merge(GS_CASE,on=['ISA_ID','GS_ID'],how='left')
        

    #     if 'ST' in flattend_dfs:
    #         df = df.merge(flattend_dfs['ST'],on=['ISA_ID','GS_ID'],how='left')
    #     if 'CLP' in flattend_dfs:
    #         df = df.merge(flattend_dfs['CLP'],on=['ISA_ID','GS_ID','ST_ID'],how='left')
    #     if 'SVC' in flattend_dfs:
    #         df = df.merge(flattend_dfs['SVC'],on=['ISA_ID','GS_ID','ST_ID','CLP_ID'],how='left')
    #     if 'CAS' in flattend_dfs:
    #         ISA_CAS = flattend_dfs['CAS'][flattend_dfs['CAS']['level'] == 'ISA']
    #         df = df.merge(ISA_CAS,on=['ISA_ID'],how='left')
