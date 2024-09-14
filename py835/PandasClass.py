import pandas as pd


class CustomDataFrame(pd.DataFrame):
    @property
    def _constructor(self):
        return CustomDataFrame

    def flatten(self, prefix=None, translate_columns=False):
        try:
            df = process_df(
                self,
                [col for col in self.columns if col not in ['id','name','value']],
                prefix,
                translate_columns
            )
            return df
        except Exception as e:
            raise ValueError("There was an error pivoting the table, possibly due to repeated IDs. Try again with `translate_columns = True`. \n Original error: " + str(e))


class PandasClass:
    def __init__(self,header):
        self.HEADER = [] 
        self.FUNCTIONAL_GROUPS = [] 
        self.FUNCTIONAL_GROUPS_CAS = []
        self.FUNCTIONAL_GROUPS_REF = []
        self.FUNCTIONAL_GROUPS_LQ = []
        self.FUNCTIONAL_GROUPS_DTM = []
        self.STATEMENTS = [] 
        self.STATEMENTS_REF = []
        self.STATEMENTS_CAS = []
        self.STATEMENTS_LQ = []
        self.STATEMENTS_DTM = []
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
            
            for ref_group in group.ref:
                data = []
                for seg_el in ref_group.elements:
                    el = {'header_id': header_id, 'functional_group_id': functional_group_id,'id':seg_el.id,'name':seg_el.name,'value':seg_el.value}
                    data.append(el)
                df = pd.DataFrame(data)
                if not df.empty:
                    self.FUNCTIONAL_GROUPS_REF.append(df)
                data = []
                df = pd.DataFrame()
            for cas_group in group.cas:
                data = []
                for seg_el in cas_group.elements:
                    el = {'header_id': header_id, 'functional_group_id': functional_group_id,'id':seg_el.id,'name':seg_el.name,'value':seg_el.value}
                    data.append(el)
                df = pd.DataFrame(data)
                if not df.empty:
                    self.FUNCTIONAL_GROUPS_CAS.append(df)
                data = []
                df = pd.DataFrame()
            for lq_group in group.lq:
                data = []
                for seg_el in lq_group.elements:
                    el = {'header_id': header_id, 'functional_group_id': functional_group_id,'id':seg_el.id,'name':seg_el.name,'value':seg_el.value}
                    data.append(el)
                df = pd.DataFrame(data)
                if not df.empty:
                    self.FUNCTIONAL_GROUPS_LQ.append(df)
                data = []
                df = pd.DataFrame()
            for dtm_group in group.dtm:
                data = []
                for seg_el in dtm_group.elements:
                    el = {'header_id': header_id, 'functional_group_id': functional_group_id,'id':seg_el.id,'name':seg_el.name,'value':seg_el.value}
                    data.append(el)
                df = pd.DataFrame(data)
                if not df.empty:
                    self.FUNCTIONAL_GROUPS_DTM.append(df)
                data = []
                df = pd.DataFrame()

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
                for ref_statement in statement.ref:
                    data = []
                    for seg_el in ref_statement.elements:
                        el = {'header_id': header_id, 'functional_group_id': functional_group_id,'statement_id':statement_id,'id':seg_el.id,'name':seg_el.name,'value':seg_el.value}
                        data.append(el)
                    df = pd.DataFrame(data)
                    if not df.empty:
                        self.STATEMENTS_REF.append(df)
                    data = []
                    df = pd.DataFrame()
                for cas_statement in statement.cas:
                    data = []
                    for seg_el in cas_statement.elements:
                        el = {'header_id': header_id, 'functional_group_id': functional_group_id,'statement_id':statement_id,'id':seg_el.id,'name':seg_el.name,'value':seg_el.value}
                        data.append(el)
                    df = pd.DataFrame(data)
                    if not df.empty:
                        self.STATEMENTS_CAS.append(df)
                    data = []
                    df = pd.DataFrame()
                for dtm_statement in statement.dtm:
                    data = []
                    for seg_el in dtm_statement.elements:
                        el = {'header_id': header_id, 'functional_group_id': functional_group_id,'statement_id':statement_id,'id':seg_el.id,'name':seg_el.name,'value':seg_el.value}
                        data.append(el)
                    df = pd.DataFrame(data)
                    if not df.empty:
                        self.STATEMENTS_DTM.append(df)
                    data = []
                    df = pd.DataFrame()
                for lq_statement in statement.lq:
                    data = []
                    for seg_el in lq_statement.elements:
                        el = {'header_id': header_id, 'functional_group_id': functional_group_id,'statement_id':statement_id,'id':seg_el.id,'name':seg_el.name,'value':seg_el.value}
                        data.append(el)
                    df = pd.DataFrame(data)
                    if not df.empty:
                        self.STATEMENTS_LQ.append(df)
                    data = []
                    df = pd.DataFrame()
                for dtm_statement in statement.dtm:
                    data = []
                    for seg_el in dtm_statement.elements:
                        el = {'header_id': header_id, 'functional_group_id': functional_group_id,'statement_id':statement_id,'id':seg_el.id,'name':seg_el.name,'value':seg_el.value}
                        data.append(el)
                    df = pd.DataFrame(data)
                    if not df.empty:
                        self.STATEMENTS_DTM.append(df)
                    data = []
                    df = pd.DataFrame()

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

        #print(f"length: {len(self.SERVICES_DTM)}")
        
        self.HEADER = CustomDataFrame(pd.concat(self.HEADER,ignore_index=True))

        if len(self.FUNCTIONAL_GROUPS) > 0:
            self.FUNCTIONAL_GROUPS = CustomDataFrame(pd.concat(self.FUNCTIONAL_GROUPS,ignore_index=True))
        else:
            self.FUNCTIONAL_GROUPS = pd.DataFrame()

        if len(self.STATEMENTS) > 0:
            self.STATEMENTS = CustomDataFrame(pd.concat(self.STATEMENTS,ignore_index=True))
        else:
            self.STATEMENTS= pd.DataFrame()

        if len(self.STATEMENTS_REF) > 0:
            self.STATEMENTS_REF = CustomDataFrame(pd.concat(self.STATEMENTS_REF,ignore_index=True))
        else:
            self.STATEMENTS_REF = pd.DataFrame()

        if len(self.STATEMENTS_CAS) > 0:
            self.STATEMENTS_CAS = CustomDataFrame(pd.concat(self.STATEMENTS_CAS,ignore_index=True))
        else:
            self.STATEMENTS_CAS = pd.DataFrame()

        if len(self.STATEMENTS_LQ) > 0:
            self.STATEMENTS_LQ = CustomDataFrame(pd.concat(self.STATEMENTS_LQ,ignore_index=True))
        else:
            self.STATEMENTS_LQ = pd.DataFrame()
            
        if len(self.STATEMENTS_DTM) > 0:
            self.STATEMENTS_DTM = CustomDataFrame(pd.concat(self.STATEMENTS_DTM,ignore_index=True))
        else:
            self.STATEMENTS_DTM = pd.DataFrame()

        if len(self.CLAIMS) > 0:
            self.CLAIMS = CustomDataFrame(pd.concat(self.CLAIMS,ignore_index=True))
        else:
            self.CLAIMS = pd.DataFrame()

        if len(self.CLAIMS_CAS) > 0:
            self.CLAIMS_CAS = CustomDataFrame(pd.concat(self.CLAIMS_CAS,ignore_index=True))
        else:
            self.CLAIMS_CAS = pd.DataFrame()

        if len(self.CLAIMS_REF) > 0:
            self.CLAIMS_REF = CustomDataFrame(pd.concat(self.CLAIMS_REF,ignore_index=True))
        else:
            self.CLAIMS_REF = pd.DataFrame()

        if len(self.CLAIMS_LQ) > 0:
            self.CLAIMS_LQ = CustomDataFrame(pd.concat(self.CLAIMS_LQ,ignore_index=True))
        else:
            self.CLAIMS_LQ = pd.DataFrame()

        if len(self.CLAIMS_DTM) > 0:
            self.CLAIMS_DTM = CustomDataFrame(pd.concat(self.CLAIMS_DTM,ignore_index=True))
        else:
            self.CLAIMS_DTM = pd.DataFrame()

        if len(self.SERVICES) > 0:
            self.SERVICES = CustomDataFrame(pd.concat(self.SERVICES,ignore_index=True))
        else:
            self.SERVICES= pd.DataFrame()

        if len(self.SERVICES_CAS) > 0:
            self.SERVICES_CAS = CustomDataFrame(pd.concat(self.SERVICES_CAS,ignore_index=True))
        else:
            self.SERVICES_CAS = pd.DataFrame()

        if len(self.SERVICES_REF) > 0:
            self.SERVICES_REF = CustomDataFrame(pd.concat(self.SERVICES_REF,ignore_index=True))
        else:
            self.SERVICES_REF = pd.DataFrame()

        if len(self.SERVICES_LQ) > 0:
            self.SERVICES_LQ = CustomDataFrame(pd.concat(self.SERVICES_LQ,ignore_index=True))
        else:
            self.SERVICES_LQ = pd.DataFrame()

        if len(self.SERVICES_DTM) > 0:
            self.SERVICES_DTM = CustomDataFrame(pd.concat(self.SERVICES_DTM,ignore_index=True))
        else:
            self.SERVICES_DTM = pd.DataFrame()


# This is it in action. We use the get_var_variable to see what the variable starts with
def process_df(df,col_idx,prefix = None,translate_columns = False):
    df = df.copy()
    if translate_columns:
        df['id'] = df['id'] + ' ' + df['name']
    if prefix:
        df['id'] = prefix + df['id']
    df = df[col_idx + ['id','value']]
    df = df.set_index(col_idx+['id'])['value'].unstack()
    df = df.reset_index()
    df.columns.name = None
    return df
    
def flatten_df(df,translate_columns):
    df = df.copy()
    col_idx = ['header_id','functional_group_id','statement_id','claim_id','service_id']
    if 'service_id' in df.columns:
        df = process_df(
            df,
            col_idx,
            'SERVICE ', translate_columns)
        df
    elif 'claim_id' in df.columns:
        df = process_df(
            df,
            [col for col in col_idx if col not in ['service_id']],
            'CLAIM ', translate_columns)
        df 
    elif 'statement_id' in df.columns:
        df = process_df(
            df,
            [col for col in col_idx if col not in ['service_id','claim_id']],
            'STATEMENT ', translate_columns)
    elif 'functional_group_id' in df.columns:
        df = process_df(
            df,
            [col for col in col_idx if col not in ['service_id','claim_id','statement_id']],
            'FUNCTIONAL GROUP ', translate_columns)
    elif not df.empty:
        df = process_df(
            df,
            [col for col in col_idx if col not in ['service_id','claim_id','statement_id','functional_group_id']],
            'HEADER ', translate_columns)
    else:
        df = pd.DataFrame()

    return df


### For huge report export. Must use translate_columns to avoid duplicate column names
def flatten_data(self,translate_columns):
    # Prepare header 
    header = flatten_df(self.pandas.HEADER,translate_columns)

    df = header

    functional_groups = flatten_df(self.pandas.FUNCTIONAL_GROUPS,translate_columns)

    df = df.merge(
        functional_groups,
        on = ['header_id'],
        how = 'left'
    ).reset_index(drop=True)

    statements = self.pandas.STATEMENTS
    if not statements.empty:
        statements = flatten_df(self.pandas.STATEMENTS,translate_columns)
        df = df.merge(
            statements,
            on = ['header_id','functional_group_id'],
            how = 'left'
        ).reset_index(drop=True)

    statements_ref = self.pandas.STATEMENTS_REF
    if not statements_ref.empty:
        statements_ref = flatten_df(self.pandas.STATEMENTS_REF,translate_columns)
        df = df.merge(
            statements_ref,
            on = ['header_id','functional_group_id','statement_id'],
            how = 'left'
        ).reset_index(drop=True)

    statements_dtm = self.pandas.STATEMENTS_DTM
    if not statements_dtm.empty:
        statements_dtm = flatten_df(self.pandas.STATEMENTS_DTM,translate_columns)
        df = df.merge(
            statements_dtm,
            on = ['header_id','functional_group_id','statement_id'],
            how = 'left'
        ).reset_index(drop=True)

    claims = self.pandas.CLAIMS
    if not claims.empty:
        claims = flatten_df(self.pandas.CLAIMS,translate_columns)
        df = df.merge(
            claims,
            on = ['header_id','functional_group_id','statement_id'],
            how = 'left'
        ).reset_index(drop=True)

    claims_cas = self.pandas.CLAIMS_CAS
    if not claims_cas.empty:
        claims_cas = flatten_df(self.pandas.CLAIMS_CAS,translate_columns)

    claims_ref = self.pandas.CLAIMS_REF
    if not claims_ref.empty:
        claims_ref = flatten_df(self.pandas.CLAIMS_REF,translate_columns)
        df = df.merge(
            claims_ref,
            on = ['header_id','functional_group_id','statement_id','claim_id'],
            how = 'left'
        )

    claims_lq = self.pandas.CLAIMS_LQ
    if not claims_lq.empty:
        claims_lq = flatten_df(self.pandas.CLAIMS_LQ,translate_columns)
        df = df.merge(
            claims_lq,
            on = ['header_id','functional_group_id','statement_id','claim_id'],
            how = 'left'
        )

    claims_dtm = self.pandas.CLAIMS_DTM
    if not claims_dtm.empty:
        claims_dtm = flatten_df(self.pandas.CLAIMS_DTM,translate_columns)
        df = df.merge(
            claims_dtm,
            on = ['header_id','functional_group_id','statement_id','claim_id'],
            how = 'left'
        )

    services = self.pandas.SERVICES
    if not services.empty:
        services = flatten_df(self.pandas.SERVICES,translate_columns)
        df = df.merge(
            services,
            on = ['header_id','functional_group_id','statement_id','claim_id'],
            how = 'left'
        ).reset_index()

    services_cas = self.pandas.SERVICES_CAS
    if not services_cas.empty:
        services_cas = flatten_df(self.pandas.SERVICES_CAS,translate_columns)
        df = df.merge(
            services_cas,
            on = ['header_id','functional_group_id','statement_id','claim_id','service_id'],
            how = 'left'
        )

    services_ref = self.pandas.SERVICES_REF
    if not services_ref.empty:
        services_ref = flatten_df(self.pandas.SERVICES_REF,translate_columns)
        df = df.merge(
            services_ref,
            on = ['header_id','functional_group_id','statement_id','claim_id','service_id'],
            how = 'left'
        )

    services_lq = self.pandas.SERVICES_LQ
    if not services_lq.empty:
        services_lq = flatten_df(self.pandas.SERVICES_LQ,translate_columns)
        df = df.merge(
            services_lq,
            on = ['header_id','functional_group_id','statement_id','claim_id','service_id'],
            how = 'left'
        )
    
    services_dtm = self.pandas.SERVICES_DTM
    if not services_dtm.empty:
        services_dtm = flatten_df(self.pandas.SERVICES_DTM,translate_columns)
        df = df.merge(
            services_dtm,
            on = ['header_id','functional_group_id','statement_id','claim_id','service_id'],
            how = 'left'
        )

    return df


