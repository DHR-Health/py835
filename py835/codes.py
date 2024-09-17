import sqlite3
import importlib.resources
import csv
import os 
import pandas as pd

def connect():
    """Connect to the SQLite database packaged with the module."""
    try:
        # Use importlib.resources to locate the 'codes.db' file in the 'codes' subpackage
        with importlib.resources.path('py835.codes', 'codes.db') as db_path:
            return sqlite3.connect(str(db_path))
    except Exception as e:
        print(f"Error opening database: {e}")
        raise

def import_csv_to_dict(file_path):
    """Import a CSV file and return a dictionary."""
    data_dict = {}

    with open(file_path, mode='r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data_dict[row['Code']] = row['Description']

    return data_dict

def get(segment = None, field = None, value=None):
    conn = connect()
    query = f"SELECT * FROM codes WHERE  1=1"
    if segment:
        query += f" AND segment = '{segment}'"
    if field:
        query += f" AND field = '{field}'"
    if value:
        query += f" AND value = '{value}'"
    return pd.read_sql_query(query, conn)

##########################################################################################
# DTM Codes
##########################################################################################
def update_dtm01_codes():
    conn = connect()
    DTM01 = {
        '036': 'Coverage Expiration',
        '050': 'Received',
        '150': 'Service Period Start',
        '151': 'Service Period End',
        '232': 'Claim Statement Period Start',
        '233': 'Claim Statement Period End',
        '405': 'Production',
        '472': 'Service Period'
    }

    dtm01_df = pd.DataFrame([
        {"segment": "DTM","field": "DTM01", "value": value, "description": description} for value, description in DTM01.items()
    ])

    dtm01_df.to_sql('codes', conn, index=False, if_exists='append')
    conn.close()

##########################################################################################
# REF01 Codes
##########################################################################################
# Pulled some of these from here: 
# https://aging.ohio.gov/wps/wcm/connect/gov/bee80467-6343-48cc-8d2a-fedfbe4fc185/ODA835-5010.pdf?MOD=AJPERES&CONVERT_TO=url&CACHEID=ROOTWORKSPACE.Z18_K9I401S01H7F40QBNJU3SO1F56-bee80467-6343-48cc-8d2a-fedfbe4fc185-newl.nS
def update_ref01_codes():
    conn = connect()
    REF01 = {
        '0B': 'State License Number',
        '0K': 'Policy Form Identifying Number',
        '1A': 'Blue Cross Provider Number',
        '1B': 'Blue Shield Provider Number',
        '1C': 'Medicare Provider Number',
        '1D': 'Medicaid Provider Number',
        '1G': 'Provider UPIN Number',
        '1H': 'CHAMPUS Identification Number',
        '1J': 'Facility ID Number',
        '1L': 'Group or Policy Number',
        '1S': 'Ambulatory Patient Group Number',
        '1W': 'Member Identification Number',
        '28': 'Employee Identification Number',
        '2U': 'Payer Identification Number',
        '6P': 'Group Number',
        '6R': 'Provider Control Number',
        '9A': 'Repriced Claim Reference Number',
        '9C': 'Adjusted Repriced Claim Reference Number',
        'APC': 'Ambulatory Payment Classification',
        'BB': 'Authorization Number',
        'CE': 'Class of Contract Code',
        'D3': 'National Council for Prescription Drug Programs Pharmacy Number',
        'E9': 'Attachment Code',
        'EA': 'Medical Record Identification Number',
        'EO': 'Submitter Identification Number',
        'EV': 'Production',
        'F2': 'Version Code',
        'F8': 'Original Reference Number',
        'G1': 'Prior Authorization Number',
        'G2': 'Provider Commercial Number',
        'G3': 'Predetermination of Benefits Identification Number',
        'HPI': 'Centers for Medicare and Medicaid Services National Provider Identifier',
        'IG': 'Insurance Policy Number',
        'LU': 'Location Number',
        'NF': 'National Association of Insurance Commissioners Code',
        'PQ': 'Payee Identification',
        'RB': 'Rate code number',
        'SY': 'Social Security Number',
        'TJ': 'Federal Taxpayer\'s Identification Number'
    }
    ref01_df = pd.DataFrame([
        {"segment":"REF","field": "REF01", "value": value, "description": description} for value, description in REF01.items()
    ])
    ref01_df.to_sql('codes', conn, index=False, if_exists='append')
    conn.close()

##########################################################################################
# CAS01 Codes
##########################################################################################
def update_cas01_codes():
    conn = connect()
    CAS01 = {
            "CO": "Contractual Obligations",
            "CR": "Correction and Reversals",
            "OA": "Other Adjustments",
            "PI": "Payer Initiated Reductions",
            "PR": "Patient Responsibility"
        }
    cas01_df = pd.DataFrame([
        {"segment":"CAS","field": "CAS01", "value": value, "description": description} for value, description in CAS01.items()
    ])
    cas01_df.to_sql('codes', conn, index=False, if_exists='append')
    conn.close()

##########################################################################################
# Claim Adjustment Reason Codes
##########################################################################################
def update_claim_adjustment_reason_codes():
    conn = connect()
    claim_adjustment_reason_codes = pd.read_csv('py835/codes/claim_adjustment_reason_codes.csv').rename({'Code':'value', 'Description':'description'}, axis=1)
    fields = ['CAS02','CAS05','CAS08','CAS11','CAS14','CAS17']
    dfs = []
    for field in fields:
        df = pd.DataFrame([{'field':field} for _,row in claim_adjustment_reason_codes.iterrows()])
        df = pd.concat([df, claim_adjustment_reason_codes], axis=1)
        dfs.append(df)
    claim_adjustment_reason_codes = pd.concat(dfs, axis=0)
    claim_adjustment_reason_codes.to_sql('codes', conn, index=False, if_exists='append')
##########################################################################################
# Claim Status Category Codes
##########################################################################################
def update_claim_status_category_codes():
    conn = connect()
    claim_status_category_codes = pd.read_csv('py835/codes/claim_status_category_codes.csv').rename({'Code':'value', 'Description':'description'}, axis=1)
    fields = []
    dfs = []
    for field in fields:
        df = pd.DataFrame([{'field':field} for _,row in claim_status_category_codes.iterrows()])
        df = pd.concat([df, claim_status_category_codes], axis=1)
        dfs.append(df)
    if dfs:
        claim_status_category_codes = pd.concat(dfs, axis=0)
        claim_status_category_codes.to_sql('codes', conn, index=False, if_exists='append')

##########################################################################################
# Claim Status Codes
##########################################################################################
def update_claim_status_codes():
    conn = connect()
    claim_status_codes = pd.read_csv('py835/codes/claim_status_codes.csv').rename({'Code':'value', 'Description':'description'}, axis=1)
    fields = ['CLP02']
    dfs = []
    for field in fields:
        df = pd.DataFrame([{"segment":"CLP",'field':field} for _,row in claim_status_codes.iterrows()])
        df = pd.concat([df, claim_status_codes], axis=1)
        dfs.append(df)
    claim_status_codes = pd.concat(dfs, axis=0)
    claim_status_codes.to_sql('codes', conn, index=False, if_exists='append')
##########################################################################################
# Provider Adjustment Reason Codes
##########################################################################################
def update_provider_adjustment_reason_codes():
    conn = connect()
    provider_adjustment_reason_codes = pd.read_csv('py835/codes/provider_adjustment_reason_codes.csv').rename({'Code':'value', 'Description':'description'}, axis=1)
    fields = ['PLB03']
    dfs = []
    for field in fields:
        df = pd.DataFrame([{"segment":"PLB",'field':field} for _,row in provider_adjustment_reason_codes.iterrows()])
        df = pd.concat([df, provider_adjustment_reason_codes], axis=1)
        dfs.append(df)
    provider_adjustment_reason_codes = pd.concat(dfs, axis=0)
    provider_adjustment_reason_codes.to_sql('codes', conn, index=False, if_exists='append')
##########################################################################################
# Update all codes
##########################################################################################

def update_codes():
    # delete the codes table from the sqlite3 database if it exists
    db = connect()
    db.execute("DROP TABLE IF EXISTS codes")
    db.close()

    update_dtm01_codes()
    update_ref01_codes()
    update_cas01_codes()
    update_claim_adjustment_reason_codes()
    update_claim_status_category_codes()
    update_claim_status_codes()
    update_provider_adjustment_reason_codes()

update_codes()
