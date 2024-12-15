import argparse
import os
import rdflib
from rdflib.namespace import OWL, RDF, RDFS, XSD, DC, DCTERMS
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
import sys
from tqdm import tqdm
from urllib.parse import unquote
import tempfile
import logging
import csv

# Global variable to collect URI issues
URI_ISSUES = []

def parse_arguments():
    """
    Parses command-line arguments.

    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(description='Process ontology files to extract common MDS concepts.')
    parser.add_argument('root_directory', help='Root directory of the ontology folder structure.')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose logging.')
    args = parser.parse_args()
    return args

def setup_logging(verbose=False):
    """
    Configures the logging settings.

    Args:
        verbose (bool): If True, set logging level to INFO. Otherwise, WARNING.
    """
    level = logging.INFO if verbose else logging.WARNING
    logging.basicConfig(level=level, format='%(levelname)s: %(message)s')

def has_invalid_chars(uri_str):
    """
    Checks if a URI string contains invalid characters.

    Args:
        uri_str (str): The URI string to check.

    Returns:
        bool: True if invalid characters are found, False otherwise.
    """
    invalid_chars = set(' <>"\'\\^`{}|%')
    return any(c in uri_str for c in invalid_chars)

def sanitize_uri(uri, ontology_file, triple_position, triple):
    """
    Sanitizes a URI by replacing invalid characters with underscores and logs any issues.

    Args:
        uri (rdflib.term.URIRef or rdflib.term.Node): The URI to sanitize.
        ontology_file (str): The ontology file where the URI is found.
        triple_position (str): Position in the triple ('subject', 'predicate', 'object').
        triple (tuple): The triple where the URI is found.

    Returns:
        rdflib.term.URIRef or original object: The sanitized URI, or the original object if it's not a URIRef.
    """
    if isinstance(uri, rdflib.URIRef):
        uri_str = str(uri)
        if has_invalid_chars(uri_str):
            sanitized_uri_str = ''.join(c if c not in ' <>"\'\\^`{}|%' else '_' for c in uri_str)
            sanitized_uri = rdflib.URIRef(sanitized_uri_str)
            URI_ISSUES.append({
                'ontology_file': ontology_file,
                'triple': triple,
                'position': triple_position,
                'original_uri': uri_str,
                'sanitized_uri': sanitized_uri_str
            })
            logging.info(f"Sanitized URI in {ontology_file}: {uri_str} -> {sanitized_uri_str}")
            return sanitized_uri
    return uri

def sanitize_ontology_file(ontology_file):
    """
    Sanitizes all URIs in an ontology file by replacing invalid characters and saves it to a temporary file.

    Args:
        ontology_file (str): Path to the original ontology file.

    Returns:
        tuple: (original_ontology_file, sanitized_temp_file_path) or None if an error occurs.
    """
    graph = rdflib.Graph()
    try:
        graph.parse(ontology_file, format=rdflib.util.guess_format(ontology_file))
        logging.info(f"Parsed ontology file: {ontology_file}")
    except Exception as e:
        logging.error(f"Error parsing {ontology_file}: {e}")
        return None

    sanitized_graph = rdflib.Graph()
    for prefix, namespace in graph.namespaces():
        sanitized_graph.bind(prefix, namespace)
    for s, p, o in graph:
        s_sanitized = sanitize_uri(s, ontology_file, 'subject', (s, p, o))
        p_sanitized = sanitize_uri(p, ontology_file, 'predicate', (s, p, o))
        o_sanitized = sanitize_uri(o, ontology_file, 'object', (s, p, o)) if isinstance(o, rdflib.URIRef) else o
        sanitized_graph.add((s_sanitized, p_sanitized, o_sanitized))

    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.ttl')
    try:
        sanitized_graph.serialize(destination=temp_file.name, format='turtle')
        logging.info(f"Sanitized ontology saved to temporary file: {temp_file.name}")
    except Exception as e:
        logging.error(f"Error serializing sanitized ontology for {ontology_file}: {e}")
        temp_file.close()
        return None
    temp_file.close()
    return (ontology_file, temp_file.name)

def extract_ontology_metadata(graph):
    """
    Extracts ontology metadata from the graph.

    Args:
        graph (rdflib.Graph): The ontology graph.

    Returns:
        dict: A dictionary containing ontology metadata.
    """
    metadata = {}
    for s in graph.subjects(RDF.type, OWL.Ontology):
        for p, o in graph.predicate_objects(s):
            metadata.setdefault(p, set()).add(o)
    return metadata

def process_ontology_file(ontology_file):
    """
    Processes an ontology file to extract MDS concepts and related triples.

    Args:
        ontology_file (str): Path to the ontology file.

    Returns:
        tuple: (ontology_file, mds_concepts, mds_triples, namespaces, ontology_metadata)
    """
    graph = rdflib.Graph()
    try:
        graph.parse(ontology_file, format=rdflib.util.guess_format(ontology_file))
        logging.info(f"Parsed sanitized ontology file: {ontology_file}")
    except Exception as e:
        logging.error(f"Error parsing {ontology_file}: {e}")
        return ontology_file, set(), set(), {}, {}

    namespaces = {str(prefix): str(ns) for prefix, ns in graph.namespace_manager.namespaces()}
    ontology_metadata = extract_ontology_metadata(graph)

    query = """
    SELECT DISTINCT ?s ?p ?o WHERE {
        {
            ?s ?p ?o .
            FILTER(STRSTARTS(STR(?s), "https://cwrusdle.bitbucket.io"))
        }
        UNION
        {
            ?s ?p ?o .
            FILTER(isIRI(?o) && STRSTARTS(STR(?o), "https://cwrusdle.bitbucket.io"))
        }
    }
    """
    mds_concepts = set()
    mds_triples = set()
    for row in graph.query(query):
        s, p, o = row
        if isinstance(s, rdflib.URIRef) and str(s).startswith('https://cwrusdle.bitbucket.io'):
            mds_concepts.add(s)
        if isinstance(o, rdflib.URIRef) and str(o).startswith('https://cwrusdle.bitbucket.io'):
            mds_concepts.add(o)
        mds_triples.add((s, p, o))
    logging.info(f"Extracted {len(mds_concepts)} MDS concepts from {ontology_file}")
    return ontology_file, mds_concepts, mds_triples, namespaces, ontology_metadata

def standardize_mds_uri(uri, mds_namespace):
    """
    Standardizes MDS URIs to the 'mds' namespace.

    Args:
        uri (rdflib.term.URIRef): Original URI.
        mds_namespace (rdflib.Namespace): The 'mds' namespace.

    Returns:
        rdflib.term.URIRef: Standardized URI.
    """
    if not isinstance(uri, rdflib.URIRef):
        return uri
    uri_str = str(uri)
    if uri_str.startswith('https://cwrusdle.bitbucket.io'):
        local_part = uri_str.split('/')[-1].split('#')[-1]
        local_part = unquote(local_part).replace(' ', '_').replace('-', '_')
        return mds_namespace[local_part]
    return uri

def write_uri_issues_log(domain_name, output_dir):
    """
    Writes the URI issues collected during sanitization to a log file.

    Args:
        domain_name (str): The domain name for the ontology.
        output_dir (str): The directory where the log file will be saved.
    """
    if URI_ISSUES:
        log_file = os.path.join(output_dir, f'mds-{domain_name}-uri-issues-log.txt')
        with open(log_file, 'w') as f:
            f.write("URI Issues Log\n")
            f.write("====================\n\n")
            for issue in URI_ISSUES:
                f.write(f"Ontology File: {issue['ontology_file']}\n")
                f.write(f"Triple: {issue['triple']}\n")
                f.write(f"Position: {issue['position']}\n")
                f.write(f"Original URI: {issue['original_uri']}\n")
                f.write(f"Sanitized URI: {issue['sanitized_uri']}\n")
                f.write("----------\n")
        logging.info(f"URI issues log generated: {log_file}")
    else:
        logging.info("No URI issues found during sanitization.")

def add_ontology_metadata(graph, metadata_list, domain_name, analyzed_titles):
    """
    Adds merged ontology metadata to the graph.

    Args:
        graph (rdflib.Graph): The ontology graph.
        metadata_list (list): List of metadata dictionaries from each ontology.
        domain_name (str): The domain name for the ontology.
        analyzed_titles (list): List of titles of the analyzed ontologies.
    """
    mds_namespace = rdflib.Namespace('https://cwrusdle.bitbucket.io/mds#')
    ontology_node = mds_namespace[f'{domain_name}_concept']
    graph.bind('mds', mds_namespace)
    graph.add((ontology_node, RDF.type, OWL.Ontology))

    merged_metadata = defaultdict(set)
    for metadata in metadata_list:
        for predicate, objects in metadata.items():
            if predicate == DCTERMS.title:
                continue
            merged_metadata[predicate].update(objects)

    unique_authors = set()
    if DCTERMS.creator in merged_metadata:
        for author_str in merged_metadata[DCTERMS.creator]:
            authors = [author.strip() for author in author_str.split(',')]
            unique_authors.update(authors)
    unique_authors = sorted(unique_authors)
    for author in unique_authors:
        graph.add((ontology_node, DCTERMS.creator, rdflib.Literal(author)))

    graph.add((ontology_node, DCTERMS.title, rdflib.Literal(f'mds-{domain_name}-concept.owl')))
    graph.add((ontology_node, RDFS.label, rdflib.Literal(f'mds-{domain_name}-concept')))
    description_str = f"Common concepts extracted from {', '.join(analyzed_titles)}"
    graph.add((ontology_node, DCTERMS.description, rdflib.Literal(description_str)))

    version_numbers = []
    for metadata in metadata_list:
        if OWL.versionInfo in metadata:
            for version in metadata[OWL.versionInfo]:
                try:
                    version_num = float(version)
                    version_numbers.append(version_num)
                except ValueError:
                    logging.warning(f"Non-numeric versionInfo '{version}' encountered and skipped.")
    new_version = min(version_numbers) if version_numbers else 1.0
    graph.add((ontology_node, OWL.versionInfo, rdflib.Literal(str(new_version))))

def process_directory(input_directory_path, output_directory_root):
    """
    Processes a directory to generate concept files by identifying common MDS concepts.

    Args:
        input_directory_path (str): The path of the directory to process.
        output_directory_root (str): The root output directory where output files will be saved.
    """
    # Determine relative path for output directory structure
    root_dir_abs = os.path.abspath(args.root_directory)
    input_dir_abs = os.path.abspath(input_directory_path)
    relative_path = os.path.relpath(input_dir_abs, root_dir_abs)
    output_directory_path = os.path.join(output_directory_root, relative_path)

    domain_name = os.path.basename(os.path.abspath(input_directory_path))
    logging.info(f"Processing directory: {input_directory_path} (Domain: {domain_name})")

    # Ensure the output directory exists
    if not os.path.exists(output_directory_path):
        os.makedirs(output_directory_path, exist_ok=True)

    # Collect ontology files in this directory
    ttl_files = [os.path.join(input_directory_path, f) for f in os.listdir(input_directory_path)
                 if f.endswith('.ttl') or f.endswith('.owl')]

    # Include concept files from subdirectories
    subdirs = [d for d in os.listdir(input_directory_path) if os.path.isdir(os.path.join(input_directory_path, d))]
    for subdir in subdirs:
        input_subdir_path = os.path.join(input_directory_path, subdir)
        process_directory(input_subdir_path, output_directory_root)

        # Include concept files from subdirectories
        subdir_output_path = os.path.join(output_directory_path, subdir)
        subdir_concept_file = os.path.join(subdir_output_path, f'mds-{subdir}-concept.ttl')
        if os.path.exists(subdir_concept_file):
            ttl_files.append(subdir_concept_file)

    if not ttl_files:
        logging.warning(f"No .ttl or .owl files found in directory: {input_directory_path}")
        return

    # Sanitize and process ontology files
    sanitized_files = []
    for ontology_file in ttl_files:
        sanitized_result = sanitize_ontology_file(ontology_file)
        if sanitized_result:
            sanitized_files.append(sanitized_result)
        else:
            logging.warning(f"Skipping {ontology_file} due to errors during sanitization.")
    if not sanitized_files:
        logging.error(f"No valid ontology files after sanitization in {input_directory_path}.")
        return

    # Write the URI issues log
    write_uri_issues_log(domain_name, output_directory_path)

    # Process sanitized ontology files
    process_sanitized_files(sanitized_files, domain_name, output_directory_path)

def process_sanitized_files(sanitized_files, domain_name, output_dir):
    """
    Processes sanitized ontology files to generate concept files.

    Args:
        sanitized_files (list): List of tuples (original_file, sanitized_file).
        domain_name (str): The domain name for the ontology.
        output_dir (str): Directory where outputs will be saved.
    """
    mds_concept_global = defaultdict(set)  # Concept URI -> set of files
    mds_data_properties = defaultdict(lambda: defaultdict(set))  # Concept URI -> predicate -> set of (value, file)
    mds_object_properties = defaultdict(lambda: defaultdict(set))  # Concept URI -> predicate -> set of (object, file)
    all_namespaces = {}
    ontology_metadata_list = []
    analyzed_titles = []
    mds_full_graph = rdflib.Graph()

    logging.info("Processing sanitized ontology files to extract MDS concepts...")
    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(process_ontology_file, sanitized_file): original_file
                   for original_file, sanitized_file in sanitized_files}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing Ontologies"):
            original_file = futures[future]
            try:
                ontology_file, mds_concepts, mds_triples, namespaces, ontology_metadata = future.result()
            except Exception as e:
                logging.error(f"Error processing {original_file}: {e}")
                continue
            if mds_concepts:
                for concept in mds_concepts:
                    mds_concept_global[concept].add(original_file)
                for triple in mds_triples:
                    s, p, o = triple
                    if s in mds_concepts:
                        if isinstance(o, rdflib.Literal):
                            mds_data_properties[s][p].add((o, original_file))
                        elif isinstance(o, rdflib.URIRef):
                            mds_object_properties[s][p].add((o, original_file))
                    # Add the triple to the full graph
                    mds_full_graph.add(triple)
                all_namespaces.update(namespaces)
                ontology_metadata_list.append(ontology_metadata)
                if DCTERMS.title in ontology_metadata:
                    for title in ontology_metadata[DCTERMS.title]:
                        analyzed_titles.append(str(title))
                else:
                    analyzed_titles.append(os.path.basename(original_file))
            else:
                logging.info(f"No MDS concepts found in {original_file}.")

    if not mds_concept_global:
        logging.warning("No MDS concepts found in any ontology files.")
        return

    logging.info("Identifying common MDS concepts present in any two or more ontology files...")
    # Aggregate all files in which each concept appears
    common_concepts = {}
    for concept, files in mds_concept_global.items():
        if len(files) >= 2:
            common_concepts[concept] = files

    if not common_concepts:
        logging.warning("No common MDS concepts found between ontology files.")
        return

    logging.info(f"Found {len(common_concepts)} common MDS concepts.")

    logging.info("Creating new ontology with standardized namespaces and merged metadata...")
    new_graph = rdflib.Graph()
    mds_namespace = rdflib.Namespace('https://cwrusdle.bitbucket.io/mds#')
    new_graph.bind('mds', mds_namespace)
    # Bind standard namespaces
    for ns_prefix, ns in [('owl', OWL), ('rdf', RDF), ('rdfs', RDFS), ('xsd', XSD), ('dc', DC), ('dcterms', DCTERMS)]:
        new_graph.bind(ns_prefix, ns)
    # Bind additional namespaces
    for prefix, namespace in all_namespaces.items():
        if prefix not in ['mds', 'owl', 'rdf', 'rdfs', 'xsd', 'dc', 'dcterms']:
            new_graph.bind(prefix, rdflib.Namespace(namespace))
    # Add ontology metadata
    add_ontology_metadata(new_graph, ontology_metadata_list, domain_name, analyzed_titles)

    logging.info("Adding triples of common MDS concepts to the new ontology...")
    # Add data properties
    for concept in common_concepts:
        s_std = standardize_mds_uri(concept, mds_namespace)
        predicates = mds_data_properties.get(concept, {})
        for p, values_files in predicates.items():
            values = set(value for value, _ in values_files)
            files_with_prop = set(file for _, file in values_files)
            if len(files_with_prop) == len(common_concepts[concept]) and len(values) == 1:
                # Property is common across all files
                value = next(iter(values))
                new_graph.add((s_std, p, value))
            else:
                # Varying data properties are not included
                pass  # Skipping varying properties

    # Add object properties
    for concept in common_concepts:
        s_std = standardize_mds_uri(concept, mds_namespace)
        predicates = mds_object_properties.get(concept, {})
        for p, objects_files in predicates.items():
            objects = set(obj for obj, _ in objects_files)
            for o in objects:
                o_std = standardize_mds_uri(o, mds_namespace)
                new_graph.add((s_std, p, o_std))

    # Collect superclass hierarchies
    logging.info("Collecting superclass hierarchies for common MDS concepts...")
    collected_concepts = set()
    parent_concepts = defaultdict(set)
    for concept in common_concepts:
        collect_superclass_hierarchy(concept, mds_full_graph, new_graph, mds_namespace, collected_concepts, parent_concepts, common_concepts)

    output_file = os.path.join(output_dir, f'mds-{domain_name}-concept.ttl')
    try:
        new_graph.serialize(destination=output_file, format='turtle')
        logging.info(f"New ontology file created: {output_file}")
    except Exception as e:
        logging.error(f"Error serializing the ontology: {e}")

    logging.info("Generating summary report...")
    generate_summary_report(common_concepts, mds_data_properties, mds_object_properties, output_dir, domain_name)

    # Cleanup temporary files
    logging.info("Cleaning up temporary files...")
    for _, sanitized_file in sanitized_files:
        try:
            os.remove(sanitized_file)
            logging.info(f"Removed temporary file: {sanitized_file}")
        except OSError as e:
            logging.warning(f"Could not remove temporary file {sanitized_file}: {e}")

def collect_superclass_hierarchy(concept, full_graph, new_graph, mds_namespace, collected_concepts, parent_concepts, common_concepts, child_concept=None):
    """
    Recursively collects superclass hierarchy for a concept.

    Args:
        concept (rdflib.URIRef): The concept URI.
        full_graph (rdflib.Graph): The full graph containing all MDS triples.
        new_graph (rdflib.Graph): The new graph where triples are being added.
        mds_namespace (rdflib.Namespace): The mds namespace.
        collected_concepts (set): A set of concepts already collected, to avoid cycles.
        parent_concepts (dict): A dictionary to track parent concepts and their children.
        common_concepts (dict): The dictionary of common concepts.
        child_concept (rdflib.URIRef): The child concept for which this concept is a parent.
    """
    if concept in collected_concepts:
        # Update parent_concepts if necessary
        if child_concept:
            parent_concepts[concept].add(child_concept)
        return
    collected_concepts.add(concept)
    concept_std = standardize_mds_uri(concept, mds_namespace)
    # Add (concept_std, RDF.type, OWL.Class)
    new_graph.add((concept_std, RDF.type, OWL.Class))
    # Add other triples about the concept (e.g., labels, definitions)
    for p, o in full_graph.predicate_objects(concept):
        if p != RDF.type and p != RDFS.subClassOf:
            p_std = p
            o_std = o
            if isinstance(o, rdflib.URIRef):
                o_std = standardize_mds_uri(o, mds_namespace)
            new_graph.add((concept_std, p_std, o_std))
    # Get any rdfs:subClassOf triples
    for superclass in full_graph.objects(concept, RDFS.subClassOf):
        superclass_std = standardize_mds_uri(superclass, mds_namespace)
        new_graph.add((concept_std, RDFS.subClassOf, superclass_std))
        # Track parent concepts
        if superclass not in common_concepts:
            parent_concepts[superclass].add(concept)
        # Continue recursion if superclass is in mds namespace
        if str(superclass).startswith('https://cwrusdle.bitbucket.io'):
            collect_superclass_hierarchy(superclass, full_graph, new_graph, mds_namespace, collected_concepts, parent_concepts, common_concepts, concept)
        else:
            # For non-mds terms, include their definitions if available
            if superclass not in collected_concepts:
                collected_concepts.add(superclass)
                # Add any available information about the superclass
                for p, o in full_graph.predicate_objects(superclass):
                    p_std = p
                    o_std = o
                    if isinstance(o, rdflib.URIRef):
                        o_std = standardize_mds_uri(o, mds_namespace)
                    new_graph.add((superclass_std, p_std, o_std))

def generate_summary_report(common_concepts, mds_data_properties, mds_object_properties, output_dir, domain_name):
    """
    Generates a summary report in CSV format.

    Args:
        common_concepts (dict): Concepts common across files.
        mds_data_properties (dict): Data properties for concepts.
        mds_object_properties (dict): Object properties for concepts.
        output_dir (str): Directory where the summary report will be saved.
        domain_name (str): Domain name for the ontology.
    """
    summary_file = os.path.join(output_dir, f'mds-{domain_name}-concept-summary.csv')
    fieldnames = ['Concept', 'Appears In', 'Common Data Properties', 'Object Properties']

    with open(summary_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for concept, files in common_concepts.items():
            concept_name = unquote(str(concept).split('#')[-1].split('/')[-1])
            if not concept_name.strip():
                logging.warning(f"Skipping concept with empty name: {concept}")
                continue

            files_list = ', '.join(sorted({os.path.basename(f) for f in files}))

            # Data Properties
            data_props = mds_data_properties.get(concept, {})
            common_data_props = []

            for p in data_props:
                p_name = unquote(str(p).split('#')[-1].split('/')[-1])
                value_files_map = defaultdict(set)
                for value, file in data_props[p]:
                    value_files_map[value].add(os.path.basename(file))

                # Check if the property has the same value across all files
                if len(value_files_map) == 1 and len(next(iter(value_files_map.values()))) == len(files):
                    # Property is common across all files
                    value = next(iter(value_files_map))
                    common_data_props.append(f"{p_name}: {value}")

            # Object Properties
            obj_props = mds_object_properties.get(concept, {})
            obj_properties = []

            for p in obj_props:
                p_name = unquote(str(p).split('#')[-1].split('/')[-1])
                obj_files_map = defaultdict(set)
                for obj, file in obj_props[p]:
                    o_name = unquote(str(obj).split('#')[-1].split('/')[-1])
                    obj_files_map[o_name].add(os.path.basename(file))

                for o_name, file_set in obj_files_map.items():
                    files_str = ', '.join(sorted(file_set))
                    obj_properties.append(f"{p_name}: {o_name} (Files: {files_str})")

            if not common_data_props and not obj_properties:
                logging.info(f"Skipping concept '{concept_name}' with no properties.")
                continue

            writer.writerow({
                'Concept': concept_name,
                'Appears In': files_list,
                'Common Data Properties': '; '.join(common_data_props) or 'None',
                'Object Properties': '; '.join(obj_properties) or 'None'
            })

    logging.info(f"Summary report generated: {summary_file}")

def main():
    global args
    args = parse_arguments()
    setup_logging(verbose=args.verbose)
    root_dir = args.root_directory

    if not os.path.isdir(root_dir):
        logging.error(f"The provided root directory does not exist or is not a directory: {root_dir}")
        sys.exit(1)

    # Create the output directory with '_Output' appended to root directory name
    output_dir = f"{os.path.basename(os.path.normpath(root_dir))}_Output"
    output_dir = os.path.join(os.path.dirname(os.path.abspath(root_dir)), output_dir)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    # Process directories recursively
    process_directory(root_dir, output_dir)

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        sys.exit(1)
