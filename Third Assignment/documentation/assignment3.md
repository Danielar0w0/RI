### Indexer

The first step to use the searcher is to index one of the collections available, so we can perform searches over it.
The indexer is similar to the already implemented in the previous assignment (we consider the term frequency for each document and or we can apply caching for either BM25 and TF-IDF).

For this assignment, we extended the indexer in order to store term positions (relevant to later perform searches with window size). 
By adding terms with their positions, along their postings lists, the index is now capable of holding information about the position of each term in each document.

Previously, the final merged index would contain the following entry:
```json
{"term": "pseudoblock", "postings": {"3375493": 6.9463587067160875}}
```

After the mentioned changes, the final merged index will contain:
```json
{"term": "pseudoblock", "postings": {"3375493": 6.9463587067160875}, "positions": {"3375493": [90, 130]}}
```

To run the indexer, the commands are the same as in the previous assignment. 
For example, to run the indexer for TF-IDF and BM25 without *cache in disk*:

```bash
python3 main.py indexer collections/pubmed_2022_tiny.jsonl.gz pubmedSPIMIindex --tk.minL 2 --tk.stopwords stopwords.txt --tk.stemmer potterNLTK --indexer.memory_threshold 16 --r
eader.memory_threshold 8
```

To run the indexer for TF-IDF with *cache in disk* enabled, we run the following command:

```bash
python3 main.py indexer collections/pubmed_2022_tiny.jsonl.gz pubmedSPIMIindex --tk.minL 2 --tk.stopwords stopwords.txt --tk.stemmer potterNLTK --indexer.memory_threshold 16 --r
eader.memory_threshold 8 --indexer.tfidf.cache_in_disk
```

And for BM25:

```bash
python3 main.py indexer collections/pubmed_2022_tiny.jsonl.gz pubmedSPIMIindex --tk.minL 2 --tk.stopwords stopwords.txt --tk.stemmer potterNLTK --indexer.memory_threshold 16 --r
eader.memory_threshold 8 --indexer.bm25.cache_in_disk
```

### Searcher

Just like in the previous assignment, the searcher can use the general index and run over it or use the cache in disk to speed up the process.
For this assignment, we've improved our searcher in order to obtain the positions of terms in the documents.

We've also extended our ranked retrieval method to boost scores of documents using the minimum window size, that is,
the smallest text span in the document that contains all search terms. 
For this, we've added the parameter `--boost` to the searcher, which is a boolean value that indicates if the boost should be applied or not.
By default, the searcher doesn't apply boost (i.e., `--boost` is set to `False`).

For this boost, we use a multiplicative boost factor that has a maximum value B when the window size corresponds to the number of distinct terms in the query (minimum window size possible).
We consider B = 2. To guarantee that larger windows have smaller boosts, we decrease the value of the window size with the following formula:
```python
boost = B / (window_size / min_window)
```
where `B` is the maximum boost value, `window_size` is the size of the current window, and `min_window` is the minimum window size (the number of distinct terms).

For larger values of the window size, in other words, when the calculated boost is too low (lower than 1), the boost factor is changed to 1 and the document isn't boosted. 
It's also worth noting that documents that don't contain all terms in the query are not boosted (boost = 1).

Since we are using natural language questions as queries, we consider only high IDF terms when finding the minimum window 
(this is already guaranteed by the removal of stopwords when reading the questions).

Finally, the function responsible for storing the ranked documents (`save_results()`) has been improved to store the query processing time,
enabling us to compare the efficiency of the searcher with and without the boost.

### Evaluation

For evaluating our retrieval engine using the queries and the relevant documents provided (‘questions_with_gs’), we've written
a script (`evaluation.py`) that runs the searcher over all queries with the different combinations of TF-IDF and BM25 ranking methods, 
with and without minimum window boost. 

At the same time, this script applies evaluation metrics, such as Precision, Recall, F-measure, Average Precision 
for each question (including the average values for each collection of questions and for each combination of ranking-boost), 
and efficiency metrics, such as Query throughput and Median query latency. This allows us to compare the different 
implementations of our retrieval engine in terms of efficiency and effectiveness.

For this evaluation, we consider the top 10, 50 and 100 retrieved documents for each question. 
In the file `utils.py`, we have implemented the functions to evaluate the results of all searches.



    

