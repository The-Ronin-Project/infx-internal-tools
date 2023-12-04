import app.coding_companion.github
import openai
import re
from decouple import config

from app.coding_companion.coco import Ticket

openai.api_type = "azure"
openai.api_base = "https://oai-ds-test2.openai.azure.com/"
openai.api_version = "2023-03-15-preview"
openai.api_key = config("OPENAI_API_KEY")


def extract_file_or_schema_names(text, file_or_schema='file'):
    if file_or_schema == 'file':
        regex = r"file: (\S+(?:\.\w+)?)"
    else:
        regex = r"schema: (\S+(?:\.\w+)?)"
    matches = re.finditer(regex, text)
    names = [match.group(1) for match in matches]
    return names


def read_schema(schema_name):
    with open(f'app/coding_companion/schemas/{schema_name}.sql', 'r') as file:
        content = file.read()
    return content


def generate_response(ticket: Ticket, files=None, schemas=None, previous_responses=None) -> str:
    query = f"{ticket.title}\n\n{ticket.description}"

    messages = [
            {
                "role": "system",
                "content": f"""
                You are an AI assistant and expert python and SQL programmer. 
                If you want to read the contents of a file before finishing a request, you must ask.
                If you want to read the contents of a database schema, you must ask.

                For exaple, to read the value_sets.py file, respond with:
                read file: value_sets.py
                
                To read the value_sets schema, respond with:
                read schema: value_sets
                
                You must respond in this format if requesting to read a schema or file.

                The following schemas are available:
                - concept_maps: contains all tables for concept maps, concept map versions, source concepts, and mappings
                - value_sets: contains all tables for value sets, value set versions, rules, and expansions

                The following files are available:

                file name: concept_maps.py
                This file contains several classes and functions for working with Concept Maps, which are used to provide mappings between concepts in different code systems. The main classes in this file are:
                    - DeprecatedConceptMap: A class representing an older version of the concept map system. This class is marked for removal once the new mapping system is fully implemented.
                    - ConceptMap: A class representing a FHIR ConceptMap resource, providing mappings between concepts in different code systems. It contains methods for loading data, creating new concept maps, and serializing the concept map data.
                    - ConceptMapVersion: A class representing a specific version of a concept map. It contains methods for loading data, generating self-mappings, and serializing the concept map version data.
                    - MappingRelationship: A data class representing a mapping relationship between two codes. It contains methods for loading mapping relationships from the database and serializing the relationship data.
                    - SourceConcept: A data class representing a source concept in a concept map. It contains methods for updating the concept data and loading related mapping relationships.
                    - Mapping: A data class representing a mapping between two codes. It contains methods for loading, saving, and serializing mapping data.
                    - MappingSuggestion: A data class representing a mapping suggestion. It contains methods for saving and serializing mapping suggestion data.
                The file also includes several helper functions for working with concept maps, such as update_comments_source_concept for updating the comments of a source concept in the database.

                file name: value_sets.py
                The overall purpose of this code is to provide a way to manage and manipulate value sets and their versions, allowing users to create, load, update, and delete value sets, as well as generate expansions of the value sets based on the rules defined for each version.
                This code defines a Python module that provides functionality to manage and manipulate ValueSets and their versions. ValueSets are used to represent a set of codes from different medical terminologies. The module contains two main classes: 
                    - ValueSet: represents a value set and provides methods to create, load, and manipulate value sets.
                    - ValueSetVersion: represents a specific version of a value set and provides methods to manage and manipulate these versions.
                    - RuleGroup: which represents a group of rules within a value set version
                    
                file name: app.py
                This file contains the API endpoint definitions for all data types.

                When asked to perform a task, you first ask for any necessary files or schemas to read. You must ask in the specified format. In most cases, you should ask for at least one file and at least one schema.
                Then, you can check for clarification on any parts of directions that should be clearer before performing the task. 
                Once directions are clarified, you write the requested code. You will be provided with a ticket containing a description of a task to do.
                If the task is sufficiently clear, create a detailed implementation plan. If the task requires code, write the code.
                """
            },
            {
                "role": "user", "content": query
            }
        ]

    if previous_responses:
        messages.extend(previous_responses)

    if files or schemas:
        if files:
            for file_name in files:
                if file_name in ['value_sets.py', 'concept_maps.py', 'surveys.py', 'terminologies.py', 'data_ingestion_registry.py', 'concept_map_versioning.py', 'codes.py']:
                    file_contents = app.coding_companion.github.get_file_from_github('projectronin', 'infx-internal-tools', 'main', f'app/models/{file_name}')
                elif file_name in ['app.py', 'database.py']:
                    file_contents = app.coding_companion.github.get_file_from_github('projectronin',
                                                                                     'infx-internal-tools', 'main',
                                                                                     f'app/{file_name}')
                messages.append({
                    "role": "user",
                    "content": f"file {file_name}: \n{file_contents}"
                })

        if schemas:
            for schema_name in schemas:
                schema_contents = read_schema(schema_name)
                messages.append(
                    {
                        "role": "user",
                        "content": f"schema {schema_name}: \n {schema_contents}"
                    }
                )

    response = openai.ChatCompletion.create(
        engine="gpt_4_32k_test",
        messages=messages,
        temperature=0.7,
        max_tokens=800,
        top_p=0.95,
        frequency_penalty=0,
        presence_penalty=0,
        stop=None
    )
    text_response = response.get('choices')[0].get('message').get('content')

    file_names = extract_file_or_schema_names(text_response, 'file')
    schema_names = extract_file_or_schema_names(text_response, 'schema')

    if file_names or schema_names:
        print(text_response)
        return generate_response(ticket, files=file_names, schemas=schema_names,
                                 previous_responses=
                                 [{
                                     "role": "assistant",
                                     "content": text_response
                                 }])
    else:
        return text_response
