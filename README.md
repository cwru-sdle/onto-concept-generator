# MDS Concept Extractor

**Script Name**: `extractor.py`

## Description
This script processes a hierarchical folder structure containing ontology files (in ttl) to identify common MDS concepts—terms that start with `https://cwrusdle.bitbucket.io`. It extracts both data and object properties associated with these concepts and generates:

- A new ontology file with standardized namespaces.
- A summary report with detailed information on:
  - The common concepts found
  - Their properties
  - The ontology files from which they originated

This script is designed for efficient data extraction using advanced techniques like multi-processing, SPARQL querying, and a global index to minimize memory usage and runtime. It facilitates modular ontology development by abstracting shared concepts into higher-level concept files, preventing duplication, and promoting reuse.

## Installation
Ensure that Python 3.x is installed on your system, then install the following dependencies:

```bash
pip install rdflib tqdm
```

## Usage

To use the script, specify the root directory of your ontology folder structure. The script will recursively process subdirectories to identify common MDS concepts and generate concept files at each level.

```bash
python extractor.py [--verbose] /path/to/root_directory
```

## Step-by-Step Workflow

### 1. Input Handling
- **Accept Root Directory**: The script accepts a single root directory as input, which contains the ontology files organized in a hierarchical folder structure. The folder name is treated as the domain name.
- **Enable Verbose Logging (Optional)**: Use the `--verbose` flag to enable detailed logging output for debugging purposes.

### 2. Recursive Directory Processing
- **Bottom-Up Approach**: The script processes directories recursively, starting from the bottom of the hierarchy (leaf directories).
- **Process Subdirectories First**: For each subdirectory, it processes and generates a concept file before processing the parent directory.
- **Update Output Directory Path**: Determines the relative path for the output directory structure, mirroring the input directory hierarchy.

### 3. Ontology File Collection
- **Collect Ontology Files**: In each directory, the script collects all ontology files with `.ttl` extensions.
- **Include Subdirectory Concept Files**: Concept files generated from subdirectories (e.g., `mds-<subdomain>-concept.ttl`) are included in the comparison at the current level.

### 4. URI Sanitization
- **Parse Ontologies**: Each ontology file is parsed using `rdflib`.
- **Identify Invalid URIs**: URIs containing invalid characters (e.g., spaces, special characters) are identified.
- **Sanitize URIs**: Invalid characters in URIs are replaced with underscores (`_`).
- **Save Sanitized Ontologies**: The sanitized ontology graphs are saved to temporary files.
- **Log Issues**: Any URIs that required sanitization are logged to a separate log file (`mds-<domain_name>-uri-issues-log.txt`) for review.

### 5. Metadata Extraction
- **Extract Ontology Metadata**: From each ontology, extract metadata such as authors (`dcterms:creator`), titles (`dcterms:title`), version information (`owl:versionInfo`), and descriptions (`dcterms:description`).
- **Collect Namespaces**: Gather namespaces used in the ontology for proper binding in the new ontology.
- **Collect Titles**: Store the titles of the analyzed ontologies for inclusion in the new ontology's description.

### 6. MDS Concepts Identification
- **Execute SPARQL Query**: A SPARQL query is executed to identify all subjects and objects starting with the MDS namespace (`https://cwrusdle.bitbucket.io`).
- **Aggregate MDS Concepts**: Compile a global index of MDS concepts across all ontologies, noting the files they appear in.
- **Collect MDS Triples**: Store the triples involving MDS concepts for further processing.

### 7. Common Concepts Determination
- **Identify Common Concepts**: Determine which MDS concepts appear in two or more ontology files at the current level, indicating commonality.
- **Extract Properties**:
  - **Data Properties**: Include data properties associated with common concepts that have the same value across all ontology files at the current level.
  - **Object Properties**: Include object properties associated with common concepts.

### 8. Ontology Metadata Merging
- **Combine Unique Authors**: Merge all unique authors from the analyzed ontologies.
- **Set Ontology Title**: Assign the title of the new ontology as `mds-<domain_name>-concept.ttl`.
- **Create Combined Description**: Formulate a description that lists all titles of the analyzed ontologies.
- **Determine Version Information**: Use the smallest `owl:versionInfo` from the ontologies as the version for the new ontology.

### 9. New Ontology Creation
- **Initialize New Graph**: Create a new `rdflib.Graph` for the merged ontology.
- **Namespace Binding**: Bind the `mds` namespace (`https://cwrusdle.bitbucket.io/mds#`) and other standard namespaces (e.g., `rdf`, `rdfs`, `owl`, `xsd`, `dc`, `dcterms`).
- **Add Merged Metadata**: Incorporate the merged metadata into the new ontology.
- **Add Triples**:
  - **Standardize URIs**: Convert MDS concept URIs to the standardized `mds` namespace.
  - **Add Data Properties**: Include data properties for common concepts that are consistent across all files.
  - **Add Object Properties**: Include object properties associated with common concepts.
  - **Collect Superclass Hierarchies**: Recursively collect and include superclass hierarchies for the common MDS concepts.

### 10. Output Generation
- **Serialize Ontology**: Save the new ontology in Turtle format (`mds-<domain_name>-concept.ttl`) in the output directory.
- **Generate Summary Report**:
  - **Create CSV Report**: Generate `mds-<domain_name>-concept-summary.csv` detailing common concepts, their properties, and the ontology files in which they appear.
  - **Include the Following Columns**:
    - **Concept**: The name of the MDS concept.
    - **Appears In**: The ontology files where the concept appears.
    - **Common Data Properties**: Data properties with the same value across all files.
    - **Object Properties**: Object properties associated with the concept.
- **Log URI Issues**: If any URIs were sanitized, output them to a log file (`mds-<domain_name>-uri-issues-log.txt`) for further inspection.

### 11. Cleanup
- **Remove Temporary Files**: Delete any temporary files created during the sanitization process to conserve space.

### 12. Error Handling
- **Graceful Exits**: Ensure that any errors encountered during processing are handled gracefully, providing meaningful messages to the user.
- **Exception Handling**: Catch exceptions during parsing, serialization, and file operations.
- **Logging**: Use Python's logging module to record informational messages, warnings, and errors for debugging purposes.