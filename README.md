<img src="misc/DHR-Health-Logo.png" width="50%">

### **Business Intelligence**

# py835

The **py835** Python package provides a robust toolset for parsing EDI 835 files using the `pyx12` library. It processes healthcare claim information from EDI 835 files into structured formats like Pandas DataFrames and JSON for seamless data manipulation, reporting, and analysis.

Note that this project is still very much in the early stages. If you require a stable version, please fork this Github repository.

#### **Table of Contents**
- [Features](#features)
- [Installation](#installation)
- [Structure of an 835 file](#structure-of-an-835-file)
- [Usage](#usage)
  - [Flattening DataFrames (long-to-wide)](#flattening-dataframes)
  - [Accessing 835 Components](#accessing-835-components)
- [Data Tree](#data-tree)
- [Pandas DataFrames](#pandas-dataframes)
- [Quick Export](#quick-export)
- [Contributing](#contributing)
- [License](#license)

## Features

- **Parse EDI 835 Files:** Load and process `.835` EDI files for healthcare claims and payment information.
- **Extract Data:** Extracts detailed information, including functional groups, transaction sets, claims, services, adjustments, and references.
- **DataFrame Output:** Organizes parsed data into Pandas DataFrames for more convenient analysis.
- **Column Renaming:** Automatically renames columns based on EDI segment codes and descriptions for better readability.
- **Pivot Tables:** Supports pivoting data (e.g., CAS and REF segments) for deeper analysis.
- **JSON Export:** Supports exporting parsed data to JSON format (via pandas) for further use in other systems.

## Installation

To install this package, run the following command:

```bash
pip install git+https://github.com/DHR-Health/py835.git
```

### Dependencies

- `pyx12`: Python library for EDI file parsing.
- `pandas`: Used for organizing parsed data into DataFrames.
- `io`: Standard Python module for handling input/output operations.
- `json`: Used for exporting data to JSON format.

<figcaption><h3>Structure of an 835 file</h3></figcaption>
<img src="misc\835 Structure.png">

# Usage
Parse an 835 file using `py835` to access the data as Pandas dataframes. The parser systematically breaks down the 835 data into hierarchical layers, reflecting the structure of the EDI 835 file:

```python
import py835

# Initialize the parser with the path to your EDI file
parser = py835.Parser(r'misc\example.835')
parser.pandas.STATEMENTS.head(5)
```

<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>header_id</th>
      <th>functional_group_id</th>
      <th>statement_id</th>
      <th>id</th>
      <th>name</th>
      <th>value</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>otjWzh-ZjqtV4-3Pztpt-bYJ2ce</td>
      <td>dc1CcU-A9Crpm-RCOmi9-5tqs5K</td>
      <td>qCz1ZE-NpShQQ-9mhd6n-Ij521x</td>
      <td>ST01-835</td>
      <td>Transaction Set Identifier Code</td>
      <td>835</td>
    </tr>
    <tr>
      <th>1</th>
      <td>otjWzh-ZjqtV4-3Pztpt-bYJ2ce</td>
      <td>dc1CcU-A9Crpm-RCOmi9-5tqs5K</td>
      <td>qCz1ZE-NpShQQ-9mhd6n-Ij521x</td>
      <td>ST02-835</td>
      <td>Transaction Set Control Number</td>
      <td>35681</td>
    </tr>
    <tr>
      <th>2</th>
      <td>otjWzh-ZjqtV4-3Pztpt-bYJ2ce</td>
      <td>dc1CcU-A9Crpm-RCOmi9-5tqs5K</td>
      <td>qCz1ZE-NpShQQ-9mhd6n-Ij521x</td>
      <td>ST03-835</td>
      <td>Implementation Convention Reference</td>
      <td>None</td>
    </tr>
    <tr>
      <th>3</th>
      <td>otjWzh-ZjqtV4-3Pztpt-bYJ2ce</td>
      <td>dc1CcU-A9Crpm-RCOmi9-5tqs5K</td>
      <td>qCz1ZE-NpShQQ-9mhd6n-Ij521x</td>
      <td>BPR01-I</td>
      <td>Transaction Handling Code</td>
      <td>I</td>
    </tr>
    <tr>
      <th>4</th>
      <td>otjWzh-ZjqtV4-3Pztpt-bYJ2ce</td>
      <td>dc1CcU-A9Crpm-RCOmi9-5tqs5K</td>
      <td>qCz1ZE-NpShQQ-9mhd6n-Ij521x</td>
      <td>BPR02-I</td>
      <td>Total Actual Provider Payment Amount</td>
      <td>810.8</td>
    </tr>
  </tbody>
</table>
</div>



By default, **py835** dataframes are in long-format. The dataframes generated by the package include a custom method for converting these tables to wide format. Use `.flatten` to do so:


```python
parser.pandas.STATEMENTS.head(5).flatten()
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>header_id</th>
      <th>functional_group_id</th>
      <th>statement_id</th>
      <th>BPR01-I</th>
      <th>BPR02-I</th>
      <th>ST01-835</th>
      <th>ST02-835</th>
      <th>ST03-835</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>otjWzh-ZjqtV4-3Pztpt-bYJ2ce</td>
      <td>dc1CcU-A9Crpm-RCOmi9-5tqs5K</td>
      <td>qCz1ZE-NpShQQ-9mhd6n-Ij521x</td>
      <td>I</td>
      <td>810.8</td>
      <td>835</td>
      <td>35681</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
</div>



You can translate the codes while flattening the data as well, and add a prefix:


```python
parser.pandas.STATEMENTS.head(5).flatten(prefix='STATEMENTS ',translate_columns = True)
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>header_id</th>
      <th>functional_group_id</th>
      <th>statement_id</th>
      <th>STATEMENTS BPR01-I Transaction Handling Code</th>
      <th>STATEMENTS BPR02-I Total Actual Provider Payment Amount</th>
      <th>STATEMENTS ST01-835 Transaction Set Identifier Code</th>
      <th>STATEMENTS ST02-835 Transaction Set Control Number</th>
      <th>STATEMENTS ST03-835 Implementation Convention Reference</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>otjWzh-ZjqtV4-3Pztpt-bYJ2ce</td>
      <td>dc1CcU-A9Crpm-RCOmi9-5tqs5K</td>
      <td>qCz1ZE-NpShQQ-9mhd6n-Ij521x</td>
      <td>I</td>
      <td>810.8</td>
      <td>835</td>
      <td>35681</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
</div>



You can access individual components of the 835 file using the class tree.


```python
print(f"""
    # First element of the first segment of the first statement in the first functional group of the header
    ID: {parser.HEADER.FUNCTIONAL_GROUPS[0].STATEMENTS[0].segments[0].elements[0].id}
    # Name of the first element of the first segment of the first statement in the first functional group of the header
    Name: {parser.HEADER.FUNCTIONAL_GROUPS[0].STATEMENTS[0].segments[0].elements[0].name}
    # Value of the first element of the first segment of the first statement in the first functional group of the header
    Name: {parser.HEADER.FUNCTIONAL_GROUPS[0].STATEMENTS[0].segments[0].elements[0].value}
""")
```

    
        # First element of the first segment of the first statement in the first functional group of the header
        ID: ST01-835
        # Name of the first element of the first segment of the first statement in the first functional group of the header
        Name: Transaction Set Identifier Code
        # Value of the first element of the first segment of the first statement in the first functional group of the header
        Name: 835
    
    


# Data Tree 
The hierarchy of the tables generated by **py835** is as follows:

<img src="misc\tree_structure.png">

# Pandas Dataframes
The **py835** parser generates pandas dataframes from your 835 file so that you can quickly import the data into your data warehouse. These are available using `parser.pandas`. The parser generates IDs as it moves through each component of the 835. The `header_id`, `functional_group_id`, `statement_id`, `claim_id`, and `service_id` are all generated by the parser as it moves through the file. You can use these when joining the tables together. 

1. **ISA (Interchange Control Header):**  `parser.pandas.HEADER`
   The top-level layer is the `ISA` segment, which contains metadata about the interchange, such as sender/receiver information, control numbers, and transaction timestamps. This segment serves as a unique identifier for the file. You can retrieve the ISA header as a Pandas DataFrame using `parser.pandas.HEADER`. This allows for easy analysis of interchange metadata, including file-level information.



```python
# Example Header
parser.pandas.HEADER.head(5)
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>header_id</th>
      <th>id</th>
      <th>name</th>
      <th>value</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>otjWzh-ZjqtV4-3Pztpt-bYJ2ce</td>
      <td>ISA01</td>
      <td>Authorization Information Qualifier</td>
      <td>00</td>
    </tr>
    <tr>
      <th>1</th>
      <td>otjWzh-ZjqtV4-3Pztpt-bYJ2ce</td>
      <td>ISA02</td>
      <td>Authorization Information</td>
      <td></td>
    </tr>
    <tr>
      <th>2</th>
      <td>otjWzh-ZjqtV4-3Pztpt-bYJ2ce</td>
      <td>ISA03</td>
      <td>Security Information Qualifier</td>
      <td>00</td>
    </tr>
    <tr>
      <th>3</th>
      <td>otjWzh-ZjqtV4-3Pztpt-bYJ2ce</td>
      <td>ISA04</td>
      <td>Security Information</td>
      <td></td>
    </tr>
    <tr>
      <th>4</th>
      <td>otjWzh-ZjqtV4-3Pztpt-bYJ2ce</td>
      <td>ISA05</td>
      <td>Interchange Sender ID Qualifier</td>
      <td>ZZ</td>
    </tr>
  </tbody>
</table>
</div>




2. **Functional Groups (GS):**  `parser.pandas.FUNCTIONAL_GROUPS`
   Within each `ISA` segment, there are one or more `GS` (Functional Group Header) segments. Functional groups organize related transaction sets under a specific purpose or business function, such as claims, remittance advice, or payment acknowledgments. You can retrieve information about the functional groups as a Pandas DataFrame using `parser.pandas.FUNCTIONAL_GROUPS`. This table can be joined with the ISA table on the `'header_id'` column for comprehensive data analysis across files.



```python
# Example Functional Group
parser.pandas.FUNCTIONAL_GROUPS.head(5)
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>header_id</th>
      <th>functional_group_id</th>
      <th>id</th>
      <th>name</th>
      <th>value</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>otjWzh-ZjqtV4-3Pztpt-bYJ2ce</td>
      <td>dc1CcU-A9Crpm-RCOmi9-5tqs5K</td>
      <td>GS01</td>
      <td>Functional Identifier Code</td>
      <td>HP</td>
    </tr>
    <tr>
      <th>1</th>
      <td>otjWzh-ZjqtV4-3Pztpt-bYJ2ce</td>
      <td>dc1CcU-A9Crpm-RCOmi9-5tqs5K</td>
      <td>GS02</td>
      <td>Application Sender's Code</td>
      <td>ABCD</td>
    </tr>
    <tr>
      <th>2</th>
      <td>otjWzh-ZjqtV4-3Pztpt-bYJ2ce</td>
      <td>dc1CcU-A9Crpm-RCOmi9-5tqs5K</td>
      <td>GS03</td>
      <td>Application Receiver's Code</td>
      <td>ABCD</td>
    </tr>
    <tr>
      <th>3</th>
      <td>otjWzh-ZjqtV4-3Pztpt-bYJ2ce</td>
      <td>dc1CcU-A9Crpm-RCOmi9-5tqs5K</td>
      <td>GS04</td>
      <td>Date</td>
      <td>20190827</td>
    </tr>
    <tr>
      <th>4</th>
      <td>otjWzh-ZjqtV4-3Pztpt-bYJ2ce</td>
      <td>dc1CcU-A9Crpm-RCOmi9-5tqs5K</td>
      <td>GS05</td>
      <td>Time</td>
      <td>12345678</td>
    </tr>
  </tbody>
</table>
</div>




3. **Statements (ST):**  `parser.pandas.STATEMENTS`
   Inside each functional group, `ST` segments define statements, also known as transaction sets. Each transaction set corresponds to a statement, bundling related claims, payments, or service details. One 835 file can have multiple transaction sets, which serve as logical groups for payment and claim details. You can extract statement data as a Pandas DataFrame using `parser.pandas.STATEMENTS`. This table can be joined with the functional group data using the composite key `['header_id', 'functional_group_id']`.



4. **Claims (CLP):**  `parser.pandas.CLAIMS`
   Each transaction set breaks down further into individual claims (`CLP` segments). Claims represent billing information for healthcare services rendered, including important details such as claim IDs, patient identifiers, the total amount billed, adjustments, payments made, and any denials or rejections. You can retrieve claim information as a Pandas DataFrame using `parser.pandas.CLAIMS`. Claims can be joined to statement data using the composite key `['header_id', 'functional_group_id', 'statement_id']`.

    4a. **Claim Adjustments (CAS):**  `parser.pandas.CLAIMS_CAS`
       Claims often have adjustments (`CAS` segments), which represent reductions or additions to the claim amount based on specific reasons like contractual obligations, patient responsibility, or denials. The parser extracts all adjustments, grouping them by claim, and allows you to retrieve this data in a Pandas DataFrame via `parser.pandas.CLAIMS_CAS`.

    4b. **Claim References (REF):**  `parser.pandas.CLAIMS_REF`
       The parser captures `REF` (Reference Identification) segments, which contain additional reference information related to claims. These may include provider identification numbers, patient account numbers, or other important reference codes. You can access reference data as a Pandas DataFrame via `parser.pandas.CLAIMS_REF`.

    4c. **Claim Service Identification (LQ):**  `parser.pandas.CLAIMS_LQ`
       The `LQ` segments provide additional information related to the services or claims, such as service qualifiers and codes. These segments are extracted into a Pandas DataFrame for claims via `parser.pandas.CLAIMS_LQ`, which can be joined to the claims table.

    4d. **Claim Date/Time (DTM):**  `parser.pandas.CLAIMS_DTM`
       The `DTM` segments represent various date and time-related information for claims (e.g., service dates, adjudication dates). You can retrieve claim-related date/time information as a Pandas DataFrame via `parser.pandas.CLAIMS_DTM`.


```python
# Example Claim Data
parser.pandas.CLAIMS.head(5)
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>header_id</th>
      <th>functional_group_id</th>
      <th>statement_id</th>
      <th>claim_id</th>
      <th>id</th>
      <th>name</th>
      <th>value</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>otjWzh-ZjqtV4-3Pztpt-bYJ2ce</td>
      <td>dc1CcU-A9Crpm-RCOmi9-5tqs5K</td>
      <td>qCz1ZE-NpShQQ-9mhd6n-Ij521x</td>
      <td>JAGubJ-pegsvc-ac3d2U-JVm6D7</td>
      <td>CLP01</td>
      <td>Patient Control Number</td>
      <td>7722337</td>
    </tr>
    <tr>
      <th>1</th>
      <td>otjWzh-ZjqtV4-3Pztpt-bYJ2ce</td>
      <td>dc1CcU-A9Crpm-RCOmi9-5tqs5K</td>
      <td>qCz1ZE-NpShQQ-9mhd6n-Ij521x</td>
      <td>JAGubJ-pegsvc-ac3d2U-JVm6D7</td>
      <td>CLP02</td>
      <td>Claim Status Code</td>
      <td>1</td>
    </tr>
    <tr>
      <th>2</th>
      <td>otjWzh-ZjqtV4-3Pztpt-bYJ2ce</td>
      <td>dc1CcU-A9Crpm-RCOmi9-5tqs5K</td>
      <td>qCz1ZE-NpShQQ-9mhd6n-Ij521x</td>
      <td>JAGubJ-pegsvc-ac3d2U-JVm6D7</td>
      <td>CLP03</td>
      <td>Total Claim Charge Amount</td>
      <td>226</td>
    </tr>
    <tr>
      <th>3</th>
      <td>otjWzh-ZjqtV4-3Pztpt-bYJ2ce</td>
      <td>dc1CcU-A9Crpm-RCOmi9-5tqs5K</td>
      <td>qCz1ZE-NpShQQ-9mhd6n-Ij521x</td>
      <td>JAGubJ-pegsvc-ac3d2U-JVm6D7</td>
      <td>CLP04</td>
      <td>Claim Payment Amount</td>
      <td>132</td>
    </tr>
    <tr>
      <th>4</th>
      <td>otjWzh-ZjqtV4-3Pztpt-bYJ2ce</td>
      <td>dc1CcU-A9Crpm-RCOmi9-5tqs5K</td>
      <td>qCz1ZE-NpShQQ-9mhd6n-Ij521x</td>
      <td>JAGubJ-pegsvc-ac3d2U-JVm6D7</td>
      <td>CLP05</td>
      <td>Patient Responsibility Amount</td>
      <td></td>
    </tr>
  </tbody>
</table>
</div>




5. **Service Line Items (SVC):**  `parser.pandas.SERVICES`
   Within each claim, service line items (`SVC` segments) detail individual healthcare services or procedures performed during the treatment. The line item data includes service codes, charges, allowed amounts, and any related adjustments. You can extract service line data as a Pandas DataFrame using `parser.pandas.SERVICES`. These can be linked to the claims table using the composite key `['header_id', 'functional_group_id', 'statement_id', 'claim_id']`.

    5a. **Service Adjustments (CAS):**  `parser.pandas.SERVICES_CAS`
       Services often have adjustments (`CAS` segments), which represent reductions or additions to the service amount based on specific reasons like contractual obligations, patient responsibility, or denials. The parser extracts all adjustments, grouping them by service level, and allows you to retrieve this data as a Pandas DataFrame via `parser.pandas.SERVICES_CAS`.

    5b. **Service References (REF):**  `parser.pandas.SERVICES_REF`
       The parser captures `REF` (Reference Identification) segments for service-level items, which may contain reference information like procedure codes or authorization numbers. These references are extracted into a Pandas DataFrame using `parser.pandas.SERVICES_REF`.

    5c. **Service Identification (LQ):**  `parser.pandas.SERVICES_LQ`
       Service-level `LQ` segments contain service-specific qualifiers and codes. You can retrieve this information as a Pandas DataFrame via `parser.pandas.SERVICES_LQ`.

    5d. **Service Date/Time (DTM):**  `parser.pandas.SERVICES_DTM`
       The `DTM` segments for services capture date and time-related information (e.g., service dates, procedure dates). This data is available as a Pandas DataFrame using `parser.pandas.SERVICES_DTM`.



```python
# Example Service CAS Data
parser.pandas.SERVICES_CAS
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>header_id</th>
      <th>functional_group_id</th>
      <th>statement_id</th>
      <th>claim_id</th>
      <th>service_id</th>
      <th>id</th>
      <th>name</th>
      <th>value</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>otjWzh-ZjqtV4-3Pztpt-bYJ2ce</td>
      <td>dc1CcU-A9Crpm-RCOmi9-5tqs5K</td>
      <td>qCz1ZE-NpShQQ-9mhd6n-Ij521x</td>
      <td>JAGubJ-pegsvc-ac3d2U-JVm6D7</td>
      <td>nl7fRY-7FVkUV-2mJawy-t2i3gI</td>
      <td>CAS01-CO</td>
      <td>Claim Adjustment Group Code</td>
      <td>CO</td>
    </tr>
    <tr>
      <th>1</th>
      <td>otjWzh-ZjqtV4-3Pztpt-bYJ2ce</td>
      <td>dc1CcU-A9Crpm-RCOmi9-5tqs5K</td>
      <td>qCz1ZE-NpShQQ-9mhd6n-Ij521x</td>
      <td>JAGubJ-pegsvc-ac3d2U-JVm6D7</td>
      <td>nl7fRY-7FVkUV-2mJawy-t2i3gI</td>
      <td>CAS02-CO</td>
      <td>Adjustment Reason Code</td>
      <td>45</td>
    </tr>
    <tr>
      <th>2</th>
      <td>otjWzh-ZjqtV4-3Pztpt-bYJ2ce</td>
      <td>dc1CcU-A9Crpm-RCOmi9-5tqs5K</td>
      <td>qCz1ZE-NpShQQ-9mhd6n-Ij521x</td>
      <td>JAGubJ-pegsvc-ac3d2U-JVm6D7</td>
      <td>nl7fRY-7FVkUV-2mJawy-t2i3gI</td>
      <td>CAS03-CO</td>
      <td>Adjustment Amount</td>
      <td>21</td>
    </tr>
    <tr>
      <th>3</th>
      <td>otjWzh-ZjqtV4-3Pztpt-bYJ2ce</td>
      <td>dc1CcU-A9Crpm-RCOmi9-5tqs5K</td>
      <td>qCz1ZE-NpShQQ-9mhd6n-Ij521x</td>
      <td>JAGubJ-pegsvc-ac3d2U-JVm6D7</td>
      <td>nl7fRY-7FVkUV-2mJawy-t2i3gI</td>
      <td>CAS04-CO</td>
      <td>Adjustment Quantity</td>
      <td>None</td>
    </tr>
    <tr>
      <th>4</th>
      <td>otjWzh-ZjqtV4-3Pztpt-bYJ2ce</td>
      <td>dc1CcU-A9Crpm-RCOmi9-5tqs5K</td>
      <td>qCz1ZE-NpShQQ-9mhd6n-Ij521x</td>
      <td>JAGubJ-pegsvc-ac3d2U-JVm6D7</td>
      <td>nl7fRY-7FVkUV-2mJawy-t2i3gI</td>
      <td>CAS05-CO</td>
      <td>Adjustment Reason Code</td>
      <td>None</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>470</th>
      <td>otjWzh-ZjqtV4-3Pztpt-bYJ2ce</td>
      <td>dc1CcU-A9Crpm-RCOmi9-5tqs5K</td>
      <td>qCz1ZE-NpShQQ-9mhd6n-Ij521x</td>
      <td>fPfsSV-rsxaRs-AsCg4p-gLJRDn</td>
      <td>BKKvyX-vdIxUP-icHxRg-O9y92d</td>
      <td>CAS15-CO</td>
      <td>Adjustment Amount</td>
      <td>None</td>
    </tr>
    <tr>
      <th>471</th>
      <td>otjWzh-ZjqtV4-3Pztpt-bYJ2ce</td>
      <td>dc1CcU-A9Crpm-RCOmi9-5tqs5K</td>
      <td>qCz1ZE-NpShQQ-9mhd6n-Ij521x</td>
      <td>fPfsSV-rsxaRs-AsCg4p-gLJRDn</td>
      <td>BKKvyX-vdIxUP-icHxRg-O9y92d</td>
      <td>CAS16-CO</td>
      <td>Adjustment Quantity</td>
      <td>None</td>
    </tr>
    <tr>
      <th>472</th>
      <td>otjWzh-ZjqtV4-3Pztpt-bYJ2ce</td>
      <td>dc1CcU-A9Crpm-RCOmi9-5tqs5K</td>
      <td>qCz1ZE-NpShQQ-9mhd6n-Ij521x</td>
      <td>fPfsSV-rsxaRs-AsCg4p-gLJRDn</td>
      <td>BKKvyX-vdIxUP-icHxRg-O9y92d</td>
      <td>CAS17-CO</td>
      <td>Adjustment Reason Code</td>
      <td>None</td>
    </tr>
    <tr>
      <th>473</th>
      <td>otjWzh-ZjqtV4-3Pztpt-bYJ2ce</td>
      <td>dc1CcU-A9Crpm-RCOmi9-5tqs5K</td>
      <td>qCz1ZE-NpShQQ-9mhd6n-Ij521x</td>
      <td>fPfsSV-rsxaRs-AsCg4p-gLJRDn</td>
      <td>BKKvyX-vdIxUP-icHxRg-O9y92d</td>
      <td>CAS18-CO</td>
      <td>Adjustment Amount</td>
      <td>None</td>
    </tr>
    <tr>
      <th>474</th>
      <td>otjWzh-ZjqtV4-3Pztpt-bYJ2ce</td>
      <td>dc1CcU-A9Crpm-RCOmi9-5tqs5K</td>
      <td>qCz1ZE-NpShQQ-9mhd6n-Ij521x</td>
      <td>fPfsSV-rsxaRs-AsCg4p-gLJRDn</td>
      <td>BKKvyX-vdIxUP-icHxRg-O9y92d</td>
      <td>CAS19-CO</td>
      <td>Adjustment Quantity</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
<p>475 rows × 8 columns</p>
</div>




The parser ensures that all segments (ISA, GS, ST, CLP, SVC) are organized in a structured, hierarchical format for easy access and analysis. It also captures important references and adjustments at various levels using `REF` and `CAS` segments, further enhancing the breakdown of claims and services.

# Quick Export
You can use the tables in `parser.pandas` to import the data into your data warehouse for long-term storage, though their long-format structure isn't well-suited for analytics. We've included a way of quickly exporting your data. 



```python
from py835 import Parser

# Initialize the parser with the path to your EDI 835 file
parser = Parser(r'misc\example.835')

# Generate a financial report as a Pandas DataFrame
financial_report_df = parser.flatten()

# Display the first few rows of the DataFrame
financial_report_df.head(3)
```




<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>index</th>
      <th>header_id</th>
      <th>HEADER IEA01 Number of Included Functional Groups</th>
      <th>HEADER IEA02 Interchange Control Number</th>
      <th>HEADER ISA01 Authorization Information Qualifier</th>
      <th>HEADER ISA02 Authorization Information</th>
      <th>HEADER ISA03 Security Information Qualifier</th>
      <th>HEADER ISA04 Security Information</th>
      <th>HEADER ISA05 Interchange Sender ID Qualifier</th>
      <th>HEADER ISA06 Interchange Sender ID</th>
      <th>...</th>
      <th>SERVICE CAS18-CO Adjustment Amount</th>
      <th>SERVICE CAS18-PR Adjustment Amount</th>
      <th>SERVICE CAS19-CO Adjustment Quantity</th>
      <th>SERVICE CAS19-PR Adjustment Quantity</th>
      <th>SERVICE DTM01-472 Date Time Qualifier</th>
      <th>SERVICE DTM02-472 Service Date</th>
      <th>SERVICE DTM03-472 Time</th>
      <th>SERVICE DTM04-472 Time Code</th>
      <th>SERVICE DTM05-472 Date Time Period Format Qualifier</th>
      <th>SERVICE DTM06-472 Date Time Period</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>yOgTWr-noWjkj-jgqvx1-zoG12Z</td>
      <td>1</td>
      <td>191511902</td>
      <td>00</td>
      <td></td>
      <td>00</td>
      <td></td>
      <td>ZZ</td>
      <td>ABCPAYER</td>
      <td>...</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>472</td>
      <td>20190324</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>yOgTWr-noWjkj-jgqvx1-zoG12Z</td>
      <td>1</td>
      <td>191511902</td>
      <td>00</td>
      <td></td>
      <td>00</td>
      <td></td>
      <td>ZZ</td>
      <td>ABCPAYER</td>
      <td>...</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>472</td>
      <td>20190324</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>yOgTWr-noWjkj-jgqvx1-zoG12Z</td>
      <td>1</td>
      <td>191511902</td>
      <td>00</td>
      <td></td>
      <td>00</td>
      <td></td>
      <td>ZZ</td>
      <td>ABCPAYER</td>
      <td>...</td>
      <td>None</td>
      <td>NaN</td>
      <td>None</td>
      <td>NaN</td>
      <td>472</td>
      <td>20190324</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
      <td>None</td>
    </tr>
  </tbody>
</table>
<p>3 rows × 210 columns</p>
</div>




Note that the resulting dataframe can have over 200 columns, depending on the amount of data in your 835 file.

This method converts each table to long-format and left joins the data, starting from `pandas.parser.HEADER` to `pandas.parser.SERVICES_DTM`. Note that the relationship from the CAS tables to Claims and Services is many-to-one, so we group the CAS for each claim or service together in this export.

## Contributing

Contributions are welcome! Feel free to submit pull requests or open issues.

1. Fork the repo.
2. Create your feature branch (`git checkout -b feature/my-feature`).
3. Commit your changes (`git commit -am 'Add some feature'`).
4. Push to the branch (`git push origin feature/my-feature`).
5. Open a pull request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

