# RI Documentation

# Reader Step

## Read Documents

Our program allows the user to set the amount of memory available for the reader (in MBs - default is 64 MB) by using the argument `-reader.memory_threshold`. The reader will read and load the documents to memory until it reaches up to 80% of the available memory. 

Once this happens, the documents loaded are sent to the tokenizer and the read documents are deleted from memory, allowing the reader to read the next chunk of documents to memory. This process is repeated over and over again until there are no documents left to read.

With this approach, our reader loads the documents to memory in small chunks allowing it to work with any amount of memory available - the only thing that changes is the size of the chunks.

**Algorithm**:

- Read the documents from the provided collection
- [Memory Threshold] Verify if the occupied memory is greater than 80% of the available memory
    - If the occupied memory is greater, return the read documents
- Repeat until all documents have been loaded and returned to the tokenizer/indexer

# Tokenizer Step

The tokenizer is the section of our system responsible for parsing the text in each one of the documents, allowing the indexer to index the terms and documents where they occur. We implemented the following tokenizing rules in our tokenizer.

## Tokenizing rules

- A minimum length filter that removes tokens with less than the specified number of characters;
- Normalization to lowercase;
- A stop word filter, allowing the user to specify a file listing the stop words to use;
- A special characters filter that removes any characters that aren’t alphanumeric or apostrophe;
- Porter stemmer (from NLTK).

# Indexer Step

For the indexer, we also allow the user to set the amount of memory (in MBs) that can be used by the indexer, with the argument `-indexer.memory_threshold`. This value should be higher than the value set by the reader - once the read documents are stored in memory by the indexer.

The indexer will index documents using in-memory data structures until the memory usage reaches 80% of the memory threshold defined by the user. When this happens, the data in memory is written to disk (creating a new **partial index**) and purged/deleted from memory, allowing the indexer to keep indexing more documents without exceeding the amount of memory configurated by the user. 

After all the documents from the collection have been read, the SPIMI Merge takes place - merging the partial indexes into a set of **final indexes**. Each one of those **final indexes** has a set of terms sorted alphabetically. 

To ease the future search process, in the same directory where our **final indexes** are stored, there is also an **index** file (an example - `index.json` - can be seen below). When we search for a term, that index file contains a pointer to the **final index** with the postings for that term, allowing us to speed up the search (since we don’t have to read each one of the final indexes to find the term).

## Obtain partial indexes

**Algorithm**:

- Iterate through the read documents (Note: might be a subset of all documents)
- Add the terms - postings lists of each document to the index
- [Memory Threshold] Verify if the occupied memory is greater than 80% of the available memory
    - If the occupied memory is greater, save the current partial index to disk and refresh the index (remove terms - postings lists)
- Repeat everything until we’ve read all documents

**Result**:

- Multiple files named `index[index_id].json` with a partial index and the corresponding terms - postings list

**Example** (index0.json):

```json
{"term": "00", "postings": {"17436024": 1, "33506432": 1, "9422018": 1, "25840644": 1, "34272127": 1, "28328759": 3, "27216271": 1, "15119194": 1, "24095935": 1, "33951526": 1, "15204747": 1, "30807879": 1, "23404491": 1, "17181586": 1, "17182662": 1, "2240732": 1, "20004336": 2, "18294049": 1, "34763544": 1, "25501640": 1, "30243233": 1, "33928487": 3, "32144803": 1, "22330114": 1}}
```

## Merge partial indexes into final inverted indexes

**Definition**: Smallest term - Term that comes first in alphanumeric order

**Algorithm**:

- [First time] Obtain the smallest term of each partial index, in other words, the first line, and record them temporarily in `temp_index`
- [First time] Store the partial indexes (files) in the dictionary `files`
- Obtain the smallest term in `temp_index`
- Obtain the postings in each partial index that contains the smallest term and add them to create the new postings list `new_postings`
- Add the smallest term - postings list to the final index
- [Memory Threshold] Verify if the occupied memory is greater than 30% of the available memory
    - If the occupied memory is greater, save the current merged index in disk and refresh the index (remove terms - postings lists)
- Record the *Id* of each partial index that contains the current smallest term in `smallest_partial_ids`
- Iterate through all partial indexes in `smallest_partial_ids` in order to obtain the next smallest term for each partial index (read from `files`)
- Update the smallest term of each partial index in `temp_index`
- Repeat everything until we reach the EOF with all partial indexes (whenever we reach EOF with a partial index, close the file and delete the partial index)

**Result**:

- A file named `index.json` with the following structure: `{"start": term_1, "end": term_2, "index": index_id}`
- Multiple files named `merged_index[index_id].json` with the terms - postings list within a certain interval (`"start"` and `"end"`)

**Example** (index.json):

```json
{"start": "0'", "end": "alphaketobutyr", "index": 0}
{"start": "alphaketoepsilonacetylaminocapro", "end": "deficiencieschromat", "index": 2}
{"start": "deficiency'", "end": "hypodipsia", "index": 3}
{"start": "hypodna", "end": "nfkappabactiv", "index": 4}
{"start": "nfkappabbind", "end": "rootbeer", "index": 5}
{"start": "rootbound", "end": "womenrec", "index": 6}
{"start": "womenreduc", "end": "zztat101chomt1h", "index": 7}
```

**Example** (merged_index0.json):

```json
{"term": "00", "postings": {"17436024": 1, "33506432": 1, "9422018": 1, "25840644": 1, "34272127": 1, "28328759": 3, "27216271": 1, "15119194": 1, "24095935": 1, "33951526": 1, "15204747": 1, "30807879": 1, "23404491": 1, "17181586": 1, "17182662": 1, "2240732": 1, "20004336": 2, "18294049": 1, "34763544": 1, "25501640": 1, "30243233": 1, "33928487": 3, "32144803": 1, "22330114": 1, "19232907": 1, "21667130": 1, "22587585": 1, "24658655": 2, "31434844": 2, "31444407": 1, "27657915": 1, "24447308": 2, "23668119": 1, "32618554": 1, "31908745": 1, "22986195": 1, "21838569": 1, "26893382": 2, "30070928": 2, "30072297": 1, "17495086": 1, "22849682": 1, "1373559": 1, "31490251": 1, "32589879": 1, "12010108": 1, "29635325": 1, "27703956": 1, "21214283": 1, "33740095": 1, "32985150": 1, "26017648": 7, "14582571": 2, "2754511": 2, "27445131": 1, "27449562": 1, "26689283": 1, "16526143": 1, "24500792": 2, "20157022": 2, "19144028": 1, "30742838": 2, "22768115": 1, "21391781": 1, "32017862": 1, "30141840": 1, "31074595": 1, "28416870": 2, "33035214": 1, "33035760": 1, "32347762": 2, "32352165": 1, "2885254": 1, "29463420": 3, "25139641": 1, "16482279": 2, "33183927": 1, "12505902": 1, "12525094": 2, "6964660": 1, "18076749": 2, "30994758": 1, "10093029": 2, "11479938": 1, "20965465": 1, "20972949": 1, "34824001": 2, "23760917": 1, "18831369": 1, "11836030": 1, "31786141": 1, "26991142": 1, "8683413": 1, "27079801": 2, "27509214": 2, "8765017": 2, "19028978": 2, "26186734": 1, "2004860": 2, "29729113": 1, "29850134": 1, "25727853": 1, "23116224": 1, "28456490": 1, "26121788": 1, "29378972": 1, "29384840": 1, "17935910": 2, "27755058": 1, "21520936": 1, "24871384": 1, "20407343": 1, "22693997": 2, "28188952": 1, "11899460": 1, "31512813": 1, "33235535": 1, "33259124": 1, "12396868": 1, "18475166": 1, "30425474": 1, "22436855": 1, "17580573": 1, "11674602": 1, "28914495": 1, "24994823": 1, "16946163": 2, "33510567": 1, "33531767": 1, "34368628": 1, "11920512": 3, "24819972": 1, "21256685": 1}}
```

## Statistics

We’ve run our indexer for each one of the provided collections and collected some statistics. The statistics for each collection can be found in the file **indexer_statistics.txt** available at the root of our repository.

[Indexer Statistics](https://github.com/Danielar0w0/RI/blob/main/First%20Assignment/indexer_statistics.txt)

## Run Command Example

```python
python [main.py](http://main.py/) indexer collections/pubmed_2022_tiny.jsonl.gz pubmedSPIMIindex --tk.minL 2 --tk.stopwords stopwords.txt --tk.stemmer potterNLTK --indexer
.memory_threshold 16 --reader.memory_threshold 8
```