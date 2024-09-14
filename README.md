<img src="DHR-Health-Logo.png" width="50%">

# py835

The **py835** Python package provides a robust toolset for parsing EDI 835 files using the `pyx12` library. It processes healthcare claim information from EDI 835 files into structured formats like Pandas DataFrames and JSON for seamless data manipulation, reporting, and analysis.

Note that this project is still very much in the early stages. If you require a stable version, please fork this Github repository.

## Features

- **Parse EDI 835 Files:** Load and process `.835` EDI files for healthcare claims and payment information.
- **Extract Data:** Extracts detailed information, including functional groups, transaction sets, claims, services, adjustments, and references.
- **DataFrame Output:** Organizes parsed data into Pandas DataFrames for more convenient analysis.
- **Column Renaming:** Automatically renames columns based on EDI segment codes and descriptions for better readability.
- **Pivot Tables:** Supports pivoting data (e.g., CAS and REF segments) for deeper analysis.
- **JSON Export:** Supports exporting parsed data to JSON format for further use in other systems.

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

## Usage

### Parsing an EDI 835 File

```python
import py835

# Initialize the parser with the path to your EDI file
parser = py835.Parser(file_path='path/to/your/file.835')
```

The parser systematically breaks down the 835 data into hierarchical layers, reflecting the structure of the EDI 835 file:

<img src="https://github.com/DHR-Health/py835/blob/main/835%20Structure.png">

# Pandas Dataframes
The **pyx12** parser generates pandas dataframes from your 835 file so that you can quickly import the data into your data warehouse. These are available using `parser.pandas`. 

1. **ISA (Interchange Control Header):**  `parser.pandas.HEADER`
   The top-level layer is the `ISA` segment, which contains metadata about the interchange, such as sender/receiver information, control numbers, and transaction timestamps. This segment serves as a unique identifier for the file. You can retrieve the ISA header as a Pandas DataFrame using `parser.pandas.HEADER`. This allows for easy analysis of interchange metadata, including file-level information.

2. **Functional Groups (GS):**  `parser.pandas.FUNCTIONAL_GROUPS`
   Within each `ISA` segment, there are one or more `GS` (Functional Group Header) segments. Functional groups organize related transaction sets under a specific purpose or business function, such as claims, remittance advice, or payment acknowledgments. You can retrieve information about the functional groups as a Pandas DataFrame using `parser.pandas.FUNCTIONAL_GROUPS`. This table can be joined with the ISA table on the `'header_id'` column for comprehensive data analysis across files.

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

The parser ensures that all segments (ISA, GS, ST, CLP, SVC) are organized in a structured, hierarchical format for easy access and analysis. It also captures important references and adjustments at various levels using `REF` and `CAS` segments, further enhancing the breakdown of claims and services.

# Quick Export
You can use the tables in `parser.pandas` to impordt into your data warehouse for long-term storage, though their long-format structure isn't well-suited for analytics. We've included a way of quickly exporting your data:

```
from py835 import Parser

# Initialize the parser with the path to your EDI 835 file
parser = Parser(file_path='path/to/your/file.835')

# Generate a financial report as a Pandas DataFrame
financial_report_df = parser.financial_report()

# Display the first few rows of the DataFrame
print(financial_report_df.head())

```

This converts each table to long-format and left joins the data, starting from `pandas.parser.HEADER` to `pandas.parser.SERVICES_DTM`. Note that the relationship from the CAS tables to Claims and Services is many-to-one, so we group the CAS for each claim or service together in this export.

## Contributing

Contributions are welcome! Feel free to submit pull requests or open issues.

1. Fork the repo.
2. Create your feature branch (`git checkout -b feature/my-feature`).
3. Commit your changes (`git commit -am 'Add some feature'`).
4. Push to the branch (`git push origin feature/my-feature`).
5. Open a pull request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
