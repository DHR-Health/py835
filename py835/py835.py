import pyx12
import pyx12.error_handler
import pyx12.x12context
import pyx12.params
import random
import string
from io import StringIO
import pandas as pd 

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
        self.header_id = None 
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
        self.header_id = None 
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
        self.header_id = None 
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
        self.header_id = None 
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
        class PandasClass:
            def __init__(self,header):
                self.HEADER = [] 
                self.FUNCTIONAL_GROUPS = [] 
                self.STATEMENTS = [] 
                self.CLAIMS = [] 
                self.CLAIMS_CAS = []
                self.CLAIMS_DTM = [] 
                self.CLAIMS_REF = []
                self.CLAIMS_LQ = []
                self.SERVICES = [] 
                self.SERVICES_CAS = []
                self.SERVICES_DTM = []
                self.SERVICES_REF = []
                self.SERVICES_LQ = []
                self.generate_dfs(header)

            def generate_dfs(self,header):
                header_id = header.header_id
                for seg_head in header.segments:
                    head_data = []
                    for seg_el in seg_head.elements:
                        el = {'header_id': header_id,'id':seg_el.id,'name':seg_el.name,'value':seg_el.value}
                        head_data.append(el)
                    head_df = pd.DataFrame(head_data)
                    if not head_df.empty:
                        self.HEADER.append(head_df)
                for group in header.FUNCTIONAL_GROUPS:
                    functional_group_id = group.functional_group_id

                    for seg_group in group.segments:
                        group_data =[]
                        for seg_el in seg_group.elements:
                            el = {'header_id': header_id, 'functional_group_id': functional_group_id,'id':seg_el.id,'name':seg_el.name,'value':seg_el.value}
                            group_data.append(el)
                        group_df = pd.DataFrame(group_data)
                        if not group_df.empty:
                            self.FUNCTIONAL_GROUPS.append(group_df)

                    for statement in group.STATEMENTS:
                        statement_id = statement.statement_id 
                        for seg_statement in statement.segments:
                            statement_data = []
                            for seg_el in seg_statement.elements:
                                el = {'header_id': header_id, 'functional_group_id': functional_group_id,'statement_id':statement_id,'id':seg_el.id,'name':seg_el.name,'value':seg_el.value}
                                statement_data.append(el)
                            statement_df = pd.DataFrame(statement_data)
                            if not statement_df.empty:
                                self.STATEMENTS.append(statement_df)

                        for claim in statement.CLAIMS:
                            claim_id = claim.claim_id
                            for seg_claim in claim.segments:
                                claim_data = []
                                for seg_el in seg_claim.elements:
                                    el = {'header_id': header_id, 'functional_group_id': functional_group_id,'statement_id':statement_id,'claim_id':claim_id,'id':seg_el.id,'name':seg_el.name,'value':seg_el.value}
                                    claim_data.append(el)
                                claim_df = pd.DataFrame(claim_data)
                                if not claim_df.empty:
                                    self.CLAIMS.append(claim_df)
                            
                            for cas_claim in claim.cas:
                                data = []
                                for seg_el in cas_claim.elements:
                                    el = {'header_id': header_id, 'functional_group_id': functional_group_id,'statement_id':statement_id,'claim_id':claim_id,'id':seg_el.id,'name':seg_el.name,'value':seg_el.value}
                                    data.append(el)
                                df = pd.DataFrame(data)
                                if not df.empty:
                                    self.CLAIMS_CAS.append(df)
                                data = []
                                df = pd.DataFrame()
                            
                            for ref_claim in claim.ref:
                                data = [] 
                                for seg_el in ref_claim.elements:
                                    el = {'header_id': header_id, 'functional_group_id': functional_group_id,'statement_id':statement_id,'claim_id':claim_id,'id':seg_el.id,'name':seg_el.name,'value':seg_el.value}
                                    data.append(el)
                                df = pd.DataFrame(data)
                                if not df.empty:
                                    self.CLAIMS_REF.append(df)
                                data = []
                                df = pd.DataFrame()

                            for lq_claim in claim.lq:
                                data = [] 
                                for seg_el in lq_claim.elements:
                                    el = {'header_id': header_id, 'functional_group_id': functional_group_id,'statement_id':statement_id,'claim_id':claim_id,'id':seg_el.id,'name':seg_el.name,'value':seg_el.value}
                                    data.append(el)
                                df = pd.DataFrame(data)
                                if not df.empty:
                                    self.CLAIMS_LQ.append(df)
                                data = []
                                df = pd.DataFrame()

                            for dtm_claim in claim.dtm:
                                data = [] 
                                for seg_el in dtm_claim.elements:
                                    el = {'header_id': header_id, 'functional_group_id': functional_group_id,'statement_id':statement_id,'claim_id':claim_id,'id':seg_el.id,'name':seg_el.name,'value':seg_el.value}
                                    data.append(el)
                                df = pd.DataFrame(data)
                                if not df.empty:
                                    self.CLAIMS_DTM.append(df)
                                data = []
                                df = pd.DataFrame()

                            for service in claim.SERVICES:
                                service_id = service.service_id
                                for seg_service in service.segments:
                                    service_data = []
                                    for seg_el in seg_service.elements:
                                        el = {'header_id': header_id, 'functional_group_id': functional_group_id,'statement_id':statement_id,'claim_id':claim_id,'service_id':service_id,'id':seg_el.id,'name':seg_el.name,'value':seg_el.value}
                                        service_data.append(el)
                                    service_df = pd.DataFrame(service_data)
                                    if not service_df.empty:
                                        self.SERVICES.append(service_df)

                                for cas_service in service.cas:
                                    data = []
                                    for seg_el in cas_service.elements:
                                        el = {'header_id': header_id, 'functional_group_id': functional_group_id,'statement_id':statement_id,'claim_id':claim_id,'service_id':service_id,'id':seg_el.id,'name':seg_el.name,'value':seg_el.value}
                                        data.append(el)
                                    df = pd.DataFrame(data)
                                    if not df.empty:
                                        self.SERVICES_CAS.append(df)
                                    data = []
                                    df = pd.DataFrame()

                                for ref_service in service.ref:
                                    data = []
                                    for seg_el in ref_service.elements:
                                        el = {'header_id': header_id, 'functional_group_id': functional_group_id,'statement_id':statement_id,'claim_id':claim_id,'service_id':service_id,'id':seg_el.id,'name':seg_el.name,'value':seg_el.value}
                                        data.append(el)
                                    df = pd.DataFrame(data)
                                    if not df.empty:
                                        self.SERVICES_REF.append(df)
                                    data = []
                                    df = pd.DataFrame()

                                for lq_service in service.lq:
                                    data = []
                                    for seg_el in lq_service.elements:
                                        el = {'header_id': header_id, 'functional_group_id': functional_group_id,'statement_id':statement_id,'claim_id':claim_id,'service_id':service_id,'id':seg_el.id,'name':seg_el.name,'value':seg_el.value}
                                        data.append(el)
                                    df = pd.DataFrame(data)
                                    if not df.empty:
                                        self.SERVICES_LQ.append(df)
                                    data = []
                                    df = pd.DataFrame()

                                for dtm_service in service.dtm:
                                    data = []
                                    for seg_el in dtm_service.elements:
                                        el = {'header_id': header_id, 'functional_group_id': functional_group_id,'statement_id':statement_id,'claim_id':claim_id,'service_id':service_id,'id':seg_el.id,'name':seg_el.name,'value':seg_el.value}
                                        data.append(el)
                                    df = pd.DataFrame(data)
                                    if not df.empty:
                                        self.SERVICES_DTM.append(df)
                                    data = []
                                    df = pd.DataFrame()

                self.HEADER = pd.concat(self.HEADER,ignore_index=True)
                if len(self.FUNCTIONAL_GROUPS) > 0:
                    self.FUNCTIONAL_GROUPS = pd.concat(self.FUNCTIONAL_GROUPS,ignore_index=True)
                else:
                    self.FUNCTIONAL_GROUPS = pd.DataFrame()
                if len(self.STATEMENTS) > 0:
                    self.STATEMENTS = pd.concat(self.STATEMENTS,ignore_index=True)
                else:
                    self.STATEMENTS= pd.DataFrame()

                if len(self.CLAIMS) > 0:
                    self.CLAIMS = pd.concat(self.CLAIMS,ignore_index=True)
                else:
                    self.CLAIMS = pd.DataFrame()
                if len(self.CLAIMS_CAS) > 0:
                    self.CLAIMS_CAS = pd.concat(self.CLAIMS_CAS,ignore_index=True)
                else:
                    self.CLAIMS_CAS = pd.DataFrame()
                if len(self.CLAIMS_REF) > 0:
                    self.CLAIMS_REF = pd.concat(self.CLAIMS_REF,ignore_index=True)
                else:
                    self.CLAIMS_REF = pd.DataFrame()
                if len(self.CLAIMS_LQ) > 0:
                    self.CLAIMS_LQ = pd.concat(self.CLAIMS_LQ,ignore_index=True)
                else:
                    self.CLAIMS_LQ = pd.DataFrame()
                if len(self.CLAIMS_DTM) > 0:
                    self.CLAIMS_DTM = pd.concat(self.CLAIMS_DTM,ignore_index=True)
                else:
                    self.CLAIMS_DTM = pd.DataFrame()
                if len(self.SERVICES) > 0:
                    self.SERVICES = pd.concat(self.SERVICES,ignore_index=True)
                else:
                    self.SERVICES= pd.DataFrame()
                if len(self.SERVICES_CAS) > 0:
                    self.SERVICES_CAS = pd.concat(self.SERVICES_CAS,ignore_index=True)
                else:
                    self.SERVICES_REF = pd.DataFrame()
                if len(self.SERVICES_REF) > 0:
                    self.SERVICES_REF = pd.concat(self.SERVICES_REF,ignore_index=True)
                else:
                    self.SERVICES_LQ = pd.DataFrame()
                if len(self.SERVICES_LQ) > 0:
                    self.SERVICES_LQ = pd.concat(self.SERVICES_LQ,ignore_index=True)
                else:
                    self.SERVICES_DTM = pd.DataFrame()
                if len(self.SERVICES_DTM) > 0:
                    self.SERVICES_DTM = pd.concat(self.SERVICES_DTM,ignore_index=True)
                else:
                    self.SERVICES_DTM = pd.DataFrame()

        self.pandas = PandasClass(self.HEADER)

    def financial_report(self):
        # Prepare header 
        header = self.pandas.HEADER
        # Create the 'id-name' column
        header['id-name'] = 'HEADER ' + header['id'] + ' ' + header['name']
        # Select relevant columns and set index
        header = header[['header_id', 'id-name', 'value']]
        # Set index and unstack to pivot the data, using first value if duplicates exist
        header = header.set_index(['header_id', 'id-name'])['value'].unstack()
        # Reset index for final output
        header = header.reset_index()

        df = header

        functional_groups = self.pandas.FUNCTIONAL_GROUPS
        functional_groups['id-name'] = 'TRANSACTION ' + functional_groups['id']+' '+ functional_groups["name"]
        functional_groups = functional_groups[['header_id','functional_group_id','id-name','value']]
        functional_groups = functional_groups.set_index(['header_id','functional_group_id','id-name'])['value'].unstack()
        functional_groups = functional_groups.reset_index()

        df = df.merge(
            functional_groups,
            on = ['header_id'],
            how = 'left'
        ).reset_index(drop=True)

        statements = self.pandas.STATEMENTS
        if not statements.empty:
            statements['id-name'] = 'STATEMENT ' + statements['id'] + ' ' + statements['name']
            statements = statements[['header_id','functional_group_id','statement_id','id-name','value']]
            statements = statements.set_index(['header_id','functional_group_id','statement_id','id-name'])['value'].unstack()
            statements = statements.reset_index()

            df = df.merge(
                statements,
                on = ['header_id','functional_group_id'],
                how = 'left'
            ).reset_index(drop=True)

        claims = self.pandas.CLAIMS
        if not claims.empty:
            claims['id-name'] = 'CLAIM ' + claims['id']+' '+claims['name']
            claims = claims[['header_id','functional_group_id','statement_id','claim_id','id-name','value']]
            claims = claims.set_index(['header_id','functional_group_id','statement_id','claim_id','id-name'])['value'].unstack()
            claims = claims.reset_index()
            df = df.merge(
                claims,
                on = ['header_id','functional_group_id','statement_id'],
                how = 'left'
            ).reset_index(drop=True)

        claims_cas = self.pandas.CLAIMS_CAS
        if not claims_cas.empty:
            claims_cas['id-name'] = 'SERVICE ' + claims_cas['id'] + ' ' + claims_cas['name']
            claims_cas = claims_cas[['header_id','functional_group_id','statement_id','claim_id','id-name','value']]
            # Group by relevant columns and concatenate values in case of duplicates
            claims_cas = claims_cas.groupby(['header_id', 'functional_group_id', 'statement_id', 'claim_id', 'id-name'])['value'] \
                .apply(lambda x: ', '.join(x.dropna().astype(str))) \
                .unstack()
            claims_cas = claims_cas.reset_index()
            df = df.merge(
                claims_cas,
                on = ['header_id','functional_group_id','statement_id','claim_id'],
                how = 'left'
            )

        claims_ref = self.pandas.CLAIMS_REF
        if not claims_ref.empty:
            claims_ref['id-name'] = 'SERVICE ' + claims_ref['id'] + ' ' + claims_ref['name']
            claims_ref = claims_ref[['header_id','functional_group_id','statement_id','claim_id','id-name','value']]
            # Group by relevant columns and concatenate values in case of duplicates
            claims_ref = claims_ref.groupby(['header_id', 'functional_group_id', 'statement_id', 'claim_id', 'id-name'])['value'] \
                .apply(lambda x: ', '.join(x.dropna().astype(str))) \
                .unstack()
            claims_ref = claims_ref.reset_index()
            df = df.merge(
                claims_ref,
                on = ['header_id','functional_group_id','statement_id','claim_id'],
                how = 'left'
            )

        claims_lq = self.pandas.CLAIMS_LQ
        if not claims_lq.empty:
            claims_lq['id-name'] = 'SERVICE ' + claims_lq['id'] + ' ' + claims_lq['name']
            claims_lq = claims_lq[['header_id','functional_group_id','statement_id','claim_id','id-name','value']]
            # Group by relevant columns and concatenate values in case of duplicates
            claims_lq = claims_lq.groupby(['header_id', 'functional_group_id', 'statement_id', 'claim_id', 'id-name'])['value'] \
                .apply(lambda x: ', '.join(x.dropna().astype(str))) \
                .unstack()
            claims_lq = claims_lq.reset_index()
            df = df.merge(
                claims_lq,
                on = ['header_id','functional_group_id','statement_id','claim_id'],
                how = 'left'
            )

        claims_dtm = self.pandas.CLAIMS_DTM
        if not claims_dtm.empty:
            claims_dtm['id-name'] = 'SERVICE ' + claims_dtm['id'] + ' ' + claims_dtm['name']
            claims_dtm = claims_dtm[['header_id','functional_group_id','statement_id','claim_id','id-name','value']]
            # Group by relevant columns and concatenate values in case of duplicates
            claims_dtm = claims_dtm.groupby(['header_id', 'functional_group_id', 'statement_id', 'claim_id', 'id-name'])['value'] \
                .apply(lambda x: ', '.join(x.dropna().astype(str))) \
                .unstack()
            claims_dtm = claims_dtm.reset_index()
            df = df.merge(
                claims_dtm,
                on = ['header_id','functional_group_id','statement_id','claim_id'],
                how = 'left'
            )

        services = self.pandas.SERVICES
        if not services.empty:
            services['id-name'] = 'SERVICE ' + services['id'] + ' ' + services['name']
            services = services[['header_id','functional_group_id','statement_id','claim_id','service_id','id-name','value']]
            services = services.set_index(['header_id','functional_group_id','statement_id','claim_id','service_id','id-name'])['value'].unstack()
            services = services.reset_index()
            df = df.merge(
                services,
                on = ['header_id','functional_group_id','statement_id','claim_id'],
                how = 'left'
            ).reset_index()

        services_cas = self.pandas.SERVICES_CAS
        if not services_cas.empty:
            services_cas['id-name'] = 'SERVICE ' + services_cas['id'] + ' ' + services_cas['name']
            services_cas = services_cas[['header_id','functional_group_id','statement_id','claim_id','service_id','id-name','value']]
            # Group by relevant columns and concatenate values in case of duplicates
            services_cas = services_cas.groupby(['header_id', 'functional_group_id', 'statement_id', 'claim_id','service_id', 'id-name'])['value'] \
                .apply(lambda x: ', '.join(x.dropna().astype(str))) \
                .unstack()
            services_cas = services_cas.reset_index()
            df = df.merge(
                services_cas,
                on = ['header_id','functional_group_id','statement_id','claim_id','service_id'],
                how = 'left'
            )

        services_ref = self.pandas.SERVICES_REF
        if not services_ref.empty:
            services_ref['id-name'] = 'SERVICE ' + services_ref['id'] + ' ' + services_ref['name']
            services_ref = services_ref[['header_id','functional_group_id','statement_id','claim_id','service_id','id-name','value']]
            # Group by relevant columns and concatenate values in case of duplicates
            services_ref = services_ref.groupby(['header_id', 'functional_group_id', 'statement_id', 'claim_id','service_id', 'id-name'])['value'] \
                .apply(lambda x: ', '.join(x.dropna().astype(str))) \
                .unstack()
            services_ref = services_ref.reset_index()
            df = df.merge(
                services_ref,
                on = ['header_id','functional_group_id','statement_id','claim_id','service_id'],
                how = 'left'
            )

        services_lq = self.pandas.SERVICES_LQ
        if not services_lq.empty:
            services_lq['id-name'] = 'SERVICE ' + services_lq['id'] + ' ' + services_lq['name']
            services_lq = services_lq[['header_id','functional_group_id','statement_id','claim_id','service_id','id-name','value']]
            # Group by relevant columns and concatenate values in case of duplicates
            services_lq = services_lq.groupby(['header_id', 'functional_group_id', 'statement_id', 'claim_id','service_id', 'id-name'])['value'] \
                .apply(lambda x: ', '.join(x.dropna().astype(str))) \
                .unstack()
            services_lq = services_lq.reset_index()
            df = df.merge(
                services_lq,
                on = ['header_id','functional_group_id','statement_id','claim_id','service_id'],
                how = 'left'
            )
        
        services_dtm = self.pandas.SERVICES_DTM
        if not services_dtm.empty:
            services_dtm['id-name'] = 'SERVICE ' + services_dtm['id'] + ' ' + services_dtm['name']
            services_dtm = services_dtm[['header_id','functional_group_id','statement_id','claim_id','service_id','id-name','value']]
            # Group by relevant columns and concatenate values in case of duplicates
            services_dtm = services_dtm.groupby(['header_id', 'functional_group_id', 'statement_id', 'claim_id','service_id', 'id-name'])['value'] \
                .apply(lambda x: ', '.join(x.dropna().astype(str))) \
                .unstack()
            services_dtm = services_dtm.reset_index()
            df = df.merge(
                services_dtm,
                on = ['header_id','functional_group_id','statement_id','claim_id','service_id'],
                how = 'left'
            )

        return df

