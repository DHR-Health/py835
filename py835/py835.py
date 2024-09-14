import pyx12
import pyx12.error_handler
import pyx12.x12context
import pyx12.params
import random
import string
from io import StringIO
import pandas as pd 
from .PandasClass import PandasClass, flatten_data

def generate_id():
    # Define the character set: digits and uppercase letters
    characters = string.ascii_letters + string.digits
    
    # Generate four segments, each with six random characters
    segments = [''.join(random.choice(characters) for _ in range(6)) for _ in range(4)]
    
    # Join the segments with dashes
    api_key = '-'.join(segments)
    
    return api_key

current_state = {
    'header': None,
    'functional_group':None,
    'statement':None,
    'claim':None,
    'service':None
}

class Element:
    def __init__(self,child,value,segment):
        if segment.id in ['REF','CAS','DTM','NM1','N1','N2','N3','N4','N5','AMT','BPR','PER','ST']:
            self.id = child.id + '-'+ segment.seg_data.get_value(segment.id+'01')
        else:
            self.id = child.id
        self.name = child.name
        self.value = value

class Segment:
    def __init__(self,segment):
        self.segment_id = generate_id()
        self.elements = []
        
        for child in segment.x12_map_node.children:
            self.elements.append(
                Element(child,segment.seg_data.get_value(child.id),segment)
            )

class LQ:
    def __init__(self,segment):
        self.elements = []
        self.service_id = None 
        self.claim_id = None 
        self.statement_id = None 
        self.functional_group_id = None 
        self.header_id = current_state['header'].header_id 
        if current_state['service']:
            self.service_id = current_state['service'].service_id 
        if current_state['claim']:
            self.claim_id = current_state['claim'].claim_id
        if current_state['statement']:
            self.statement_id = current_state['statement'].statement_id
        if current_state['functional_group']:
            self.functional_group_id = current_state['functional_group'].functional_group_id
        
        for child in segment.x12_map_node.children:
            self.elements.append(
                Element(child,segment.seg_data.get_value(child.id),segment)
            )

class CAS:
    def __init__(self,segment):
        self.elements = []
        self.service_id = None 
        self.claim_id = None 
        self.statement_id = None 
        self.functional_group_id = None 
        self.header_id = current_state['header'].header_id 
        if current_state['service']:
            self.service_id = current_state['service'].service_id 
        if current_state['claim']:
            self.claim_id = current_state['claim'].claim_id
        if current_state['statement']:
            self.statement_id = current_state['statement'].statement_id
        if current_state['functional_group']:
            self.functional_group_id = current_state['functional_group'].functional_group_id
        
        for child in segment.x12_map_node.children:
            self.elements.append(
                Element(child,segment.seg_data.get_value(child.id),segment)
            )

class REF:
    def __init__(self,segment):
        self.elements = []
        self.service_id = None 
        self.claim_id = None 
        self.statement_id = None 
        self.functional_group_id = None 
        self.header_id = current_state['header'].header_id 
        if current_state['service']:
            self.service_id = current_state['service'].service_id 
        if current_state['claim']:
            self.claim_id = current_state['claim'].claim_id
        if current_state['statement']:
            self.statement_id = current_state['statement'].statement_id
        if current_state['functional_group']:
            self.functional_group_id = current_state['functional_group'].functional_group_id

        for child in segment.x12_map_node.children:
            self.elements.append(
                Element(child,segment.seg_data.get_value(child.id),segment)
            )

class DTM:
    def __init__(self,segment):
        self.elements = []
        self.service_id = None 
        self.claim_id = None 
        self.statement_id = None 
        self.functional_group_id = None 
        self.header_id = current_state['header'].header_id 
        if current_state['service']:
            self.service_id = current_state['service'].service_id 
        if current_state['claim']:
            self.claim_id = current_state['claim'].claim_id
        if current_state['statement']:
            self.statement_id = current_state['statement'].statement_id
        if current_state['functional_group']:
            self.functional_group_id = current_state['functional_group'].functional_group_id

        for child in segment.x12_map_node.children:
            self.elements.append(
                Element(child,segment.seg_data.get_value(child.id),segment)
            )

class Service:
    def __init__(self):
        self.service_id = generate_id()
        self.segments = []
        self.lq = []
        self.cas = []
        self.ref = []
        self.dtm = []
        
    def update(self,segment):
        self.segments.append(Segment(segment))

    
class Claim:
    def __init__(self):
        self.claim_id = generate_id()
        self.segments = []
        self.lq = []
        self.cas = []
        self.ref = []
        self.dtm = []
        self.SERVICES = []
        

    def update(self,segment):
        self.segments.append(Segment(segment))

    def append(self,service):
        self.SERVICES.append(service)
    
class Statement:
    def __init__(self):
        self.statement_id = generate_id()
        self.segments = []
        self.lq = []
        self.cas = []
        self.ref = []
        self.dtm = []
        self.CLAIMS = []
        

    def update(self,segment):
        self.segments.append(Segment(segment))

    def append(self,claim):
        self.CLAIMS.append(claim)
    
class Functional_Group:
    def __init__(self):
        self.functional_group_id = generate_id()
        self.segments = []
        self.lq = []
        self.cas = []
        self.ref = []
        self.dtm = []
        self.STATEMENTS = []
        
    def update(self,segment):
        self.segments.append(Segment(segment))

    def append(self,statement):
        self.STATEMENTS.append(statement)
    
class Header:
    def __init__(self):
        self.header_id = generate_id()
        self.segments = []
        self.lq = []
        self.cas = []
        self.ref = []
        self.dtm = []
        self.FUNCTIONAL_GROUPS = []

    def update(self,segment):
        self.segments.append(Segment(segment))

    def append(self,functional_group):
        self.FUNCTIONAL_GROUPS.append(functional_group)
    
class Parser:
    def __init__(self,filepath):
        self.filepath = filepath
        self.unpack()
        
    def load_file_content(self):
        with open(self.filepath, 'r') as edi_file:
            return edi_file.read()

    def load_context(self):
        params = pyx12.params.params()
        errh = pyx12.error_handler.errh_null()
        edi_file_stream = StringIO(self.load_file_content())
        return pyx12.x12context.X12ContextReader(params, errh, edi_file_stream)

    def unpack(self):
        current_state['header'] = Header()
        reader = self.load_context()
        for segment in reader.iter_segments():
            if segment.id == 'GS':  # Begin functional group
                if current_state['functional_group'] is not None:
                    current_state['header'].append(current_state['functional_group'])
                current_state['service'] = None
                current_state['claim'] = None 
                current_state['statement'] = None 
                current_state['functional_group'] = Functional_Group()
                current_state['functional_group'].update(segment)
                
            elif segment.id == 'ST':  # Begin statement
                # Ensure that the functional group does not append to statements
                if current_state['statement'] is not None:
                    current_state['functional_group'].append(current_state['statement'])
                current_state['service'] = None
                current_state['claim'] = None 
                current_state['statement'] = Statement()
                current_state['statement'].update(segment)
            elif segment.id == 'CLP':
                if current_state['claim'] is not None:
                    current_state['statement'].append(current_state['claim'])
                current_state['service'] = None 
                current_state['claim'] = Claim()
                current_state['claim'].update(segment)
            elif segment.id == 'SVC':
                if current_state['service'] is not None:
                    current_state['claim'].append(current_state['service'])
                current_state['service'] = Service()
                current_state['service'].update(segment)
            elif segment.id == 'CAS':
                cas = CAS(segment)
                if cas.service_id:
                    current_state['service'].cas.append(cas)
                elif cas.claim_id:
                    current_state['claim'].cas.append(cas)
                elif cas.statement_id:
                    current_state['statement'].cas.append(cas)
                elif cas.functional_group_id:
                    current_state['functional_group'].cas.append(cas)
                else:
                    current_state['header'].cas.append(cas)
            elif segment.id == 'REF':
                ref = REF(segment)
                
                if ref.service_id:
                    current_state['service'].ref.append(ref)
                elif ref.claim_id:
                    current_state['claim'].ref.append(ref)
                elif ref.statement_id:
                    current_state['statement'].ref.append(ref)
                elif ref.functional_group_id:
                    current_state['functional_group'].ref.append(ref)
                else:
                    current_state['header'].ref.append(ref)
            elif segment.id == 'DTM':
                dtm = DTM(segment)
                if dtm.service_id:
                    current_state['service'].dtm.append(dtm)
                elif dtm.claim_id:
                    current_state['claim'].dtm.append(dtm)
                elif dtm.statement_id:
                    current_state['statement'].dtm.append(dtm)
                elif dtm.functional_group_id:
                    current_state['functional_group'].dtm.append(dtm)
                else:
                    current_state['header'].dtm.append(dtm)
            elif segment.id == 'LQ':
                lq = LQ(segment)
                if lq.service_id:
                    current_state['service'].lq.append(lq)
                elif lq.claim_id:
                    current_state['claim'].lq.append(lq)
                elif lq.statement_id:
                    current_state['statement'].lq.append(lq)
                elif lq.functional_group_id:
                    current_state['functional_group'].lq.append(lq)
                else:
                    current_state['header'].lq.append(lq)
                
            elif segment.id == 'SE': # End of a statement
                
                current_state['statement'].update(segment)
                current_state['functional_group'].append(current_state['statement'])
                current_state['statement'] = None
                current_state['claim'] = None 
                current_state['service'] = None  
            elif segment.id == 'GE': # End of a functional group
                
                current_state['functional_group'].update(segment)
                current_state['header'].append(current_state['functional_group'])
                current_state['functional_group'] = None
                current_state['statement'] = None
                current_state['claim'] = None 
                current_state['service'] = None 

            elif segment.id == 'IEA':
                current_state['header'].update(segment)

            else:
                if current_state['service'] is not None:
                    current_state['service'].update(segment)
                elif current_state['claim'] is not None:
                    current_state['claim'].update(segment)
                elif current_state['statement'] is not None:
                    current_state['statement'].update(segment)
                elif current_state['functional_group'] is not None:
                    current_state['functional_group'].update(segment)
                else: current_state['header'].update(segment)

        self.HEADER = current_state['header']

        # Generate pandas dataframes

        self.pandas = PandasClass(self.HEADER)

    def flatten(self,translate_columns=True):
        return flatten_data(self,translate_columns)