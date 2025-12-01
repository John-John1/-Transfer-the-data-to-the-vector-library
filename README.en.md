# Transfer the data to the vector library

#### introduce
This project utilizes functions or APIs from the Unstructured library.
Divide PDF, PPTX, and HTML files into chunks based on the metadata automatically generated from the parsed elements.
If there is custom metadata, it will also be inserted subsequently by a custom function.
After being transformed into vectors by the vector model
The data is stored in the Supabase (Chroma DB) database because it contains metadata.
Therefore, it supports hybrid search.

#### Software Architecture
1. Preliminary preparations
2. Parse the data into Element format and temporarily convert it into JSON for display.
3. Define your own custom metadata framework.
4. Retrieve the data from the element, and then create a Python dictionary to store the mapping relationships.
5. Swap the positions of the key and value (to make it easier to quickly find which chapter the element belongs to based on the parent_id later).
6. Automatically gather all content from the same chapter in a PDF together, easily restoring the structure of your document.
7. Divide the data into blocks based on the metadata generated during the parsing process.
8.2.8 Write to Supabase or an in-memory vector database (ChromaDB)


#### Instructions for Use
pip requiresments.txt
python main.py
