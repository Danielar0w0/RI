import json
import os
from utils import precision, recall, f_measure, average_precision, median_query_latency, query_throughput


def main():
    results_file = open("evaluation/all_questions.md", "w", encoding="utf-8")

    # Keep track of evaluation statistics (ranking-boost)
    evaluation_table_rows = [
        ["Collection", "Top K", "Ranking", "Boost", "Precision", "Recall", "F-measure", "Average Precision"]]

    # Keep track of efficiency statistics (ranking-boost)
    efficiency_table_rows = [["Collection", "Ranking", "Boost", "Query Throughput", "Median Query Latency"]]

    questions_path = "questions_with_gs"
    for path in os.scandir(questions_path):
        if path.is_file():

            # Report the mean over all queries of a collection overall, considering the top 10, 50 and 100 retrieved documents
            mean_results_files = open(f"evaluation/{path.name.split('.')[0]}.md", "w", encoding="utf-8")

            mean_precision = {"10": 0, "50": 0, "100": 0}
            mean_recall = {"10": 0, "50": 0, "100": 0}
            mean_f_measure = {"10": 0, "50": 0, "100": 0}
            mean_average_precision = {"10": 0, "50": 0, "100": 0}

            n_queries = 0

            # Try all combinations of boost and ranking
            for boost in ["True", "False"]:
                for ranking in ["ranking.bm25", "ranking.tfidf"]:

                    # Report mean over all queries of a collection for specific ranking and boost
                    combination_mean_precision = {"10": 0, "50": 0, "100": 0}
                    combination_mean_recall = {"10": 0, "50": 0, "100": 0}
                    combination_mean_f_measure = {"10": 0, "50": 0, "100": 0}
                    combination_mean_average_precision = {"10": 0, "50": 0, "100": 0}

                    combination_n_queries = 0

                    os.system(f"python main.py searcher pubmedSPIMIindex {path.path} --boost {boost} ranked {ranking}")

                    # Keep track of metrics
                    precisions = {}
                    recalls = {}
                    f_measures = {}
                    average_precisions = {}

                    # Open file with queries
                    with open(path.path, "r") as file:

                        query_times = []

                        # Read queries
                        while True:

                            line = file.readline()

                            # Reached EOF
                            if line == "":
                                break

                            # Read query
                            query = json.loads(line)

                            query_id = query["query_id"]
                            query_text = query["query_text"]
                            documents = query["documents_pmid"]

                            # Read ranked documents
                            if os.path.exists(f"ranked/query{query_id}.json"):
                                with open(f"ranked/query{query_id}.json", "r") as ranked_file:

                                    relevant_docs = []
                                    retrieved_docs = []

                                    # First line with query_id, query_text and query_time
                                    details = json.loads(ranked_file.readline())

                                    query_text = details["query_text"]
                                    query_time = details["query_time"]
                                    query_times.append(query_time)

                                    # Read ranked documents
                                    while True:

                                        line = ranked_file.readline()

                                        # Reached EOF
                                        if line == "":
                                            break

                                        ranked_doc = json.loads(line)

                                        doc_id = ranked_doc["doc_id"]
                                        # score = ranked_doc["score"]

                                        if doc_id in documents:
                                            relevant_docs.append(doc_id)

                                        retrieved_docs.append(doc_id)

                                    # Store details about query, ranking and boost
                                    results_file.write(f"### {'Query ID:'} {query_id}  \n")
                                    results_file.write(f"{'Query Text:'} {query_text}  \n")
                                    results_file.write(f"{'Query Time:'} {query_time}  \n")
                                    results_file.write(f"{'Ranking:'} {ranking}  \n")
                                    results_file.write(f"{'Boost:'} {boost}  \n")
                                    results_file.write("\n")

                                    for k in [10, 50, 100]:
                                        k_relevant_docs = relevant_docs[:k]
                                        k_retrieved_docs = retrieved_docs[:k]

                                        # print_statistics(query_id, query_text, k_relevant_docs, k_retrieved_docs,
                                        #                  documents, k, ranking, boost)

                                        query_precision = precision(k_relevant_docs, k_retrieved_docs)
                                        query_recall = recall(k_relevant_docs, documents)
                                        query_f_measure = f_measure(query_precision, query_recall)
                                        query_average_precision = average_precision(k_relevant_docs, k_retrieved_docs)

                                        # Update results
                                        precisions[str(k)] = query_precision
                                        recalls[str(k)] = query_recall
                                        f_measures[str(k)] = query_f_measure
                                        average_precisions[str(k)] = query_average_precision

                                        # Update mean results
                                        mean_precision[str(k)] += query_precision
                                        mean_recall[str(k)] += query_recall
                                        mean_f_measure[str(k)] += query_f_measure
                                        mean_average_precision[str(k)] += query_average_precision

                                        # Update combination mean results
                                        combination_mean_precision[str(k)] += query_precision
                                        combination_mean_recall[str(k)] += query_recall
                                        combination_mean_f_measure[str(k)] += query_f_measure
                                        combination_mean_average_precision[str(k)] += query_average_precision

                                    # Keep track of evaluation statistics (query)
                                    table_rows = [
                                        ["Top K", 10, 50, 100],
                                        ["Precision", precisions["10"], precisions["50"], precisions["100"]],
                                        ["Recall", recalls["10"], recalls["50"], recalls["100"]],
                                        ["F-measure", f_measures["10"], f_measures["50"], f_measures["100"]],
                                        ["Average Precision", average_precisions["10"], average_precisions["50"],
                                         average_precisions["100"]]
                                    ]

                                    write_markdown_table(results_file, table_rows)

                            # Count query
                            n_queries += 1

                            # Count query for combination
                            combination_n_queries += 1

                    # print_efficiency_statistics(query_times, ranking, boost)

                    efficiency_table_rows.append([path.name, ranking, boost, query_throughput(query_times),
                                                  median_query_latency(query_times)])

                    if combination_n_queries > 0:
                        for k in [10, 50, 100]:
                            evaluation_table_rows.append(
                                [path.name, k, ranking, boost,
                                 combination_mean_precision[str(k)] / combination_n_queries,
                                 combination_mean_recall[str(k)] / combination_n_queries,
                                 combination_mean_f_measure[str(k)] / combination_n_queries,
                                 combination_mean_average_precision[str(k)] / combination_n_queries])

            # Calculate mean results
            if n_queries > 0:
                for k in [10, 50, 100]:
                    mean_precision[str(k)] /= n_queries
                    mean_recall[str(k)] /= n_queries
                    mean_f_measure[str(k)] /= n_queries
                    mean_average_precision[str(k)] /= n_queries

            # Write mean evaluation results (collection)
            table_rows = [
                ["Top K", 10, 50, 100],
                ["Precision", mean_precision["10"], mean_precision["50"], mean_precision["100"]],
                ["Recall", mean_recall["10"], mean_recall["50"], mean_recall["100"]],
                ["F-measure", mean_f_measure["10"], mean_f_measure["50"], mean_f_measure["100"]],
                ["Average Precision", mean_average_precision["10"], mean_average_precision["50"],
                 mean_average_precision["100"]]
            ]

            write_markdown_table(mean_results_files, table_rows)
            mean_results_files.close()

    # Write evaluation results (ranking-boost)
    with open("evaluation/evaluation.md", "w", encoding="utf-8") as results_evaluation_file:
        write_markdown_table(results_evaluation_file, evaluation_table_rows)

    # Write efficiency results (ranking-boost)
    with open("evaluation/efficiency.md", "w", encoding="utf-8") as results_efficiency_file:
        write_markdown_table(results_efficiency_file, efficiency_table_rows)

    results_file.close()


def write_markdown_table(results_file, table_rows):
    column_widths = []

    for i in range(len(table_rows[0])):
        column_widths.append(max(len(str(row[i])) for row in table_rows))

    # Write table header
    for col in range(len(table_rows[0])):
        results_file.write(f"| {table_rows[0][col]:<{column_widths[col]}} ")
    results_file.write("|\n")

    # Write the separator
    for col in range(len(table_rows[0])):
        results_file.write(f"|{'-' * (column_widths[col] + 2)}")
    results_file.write("|\n")

    # Write table data
    for row in table_rows[1:]:
        for col in range(len(row)):
            results_file.write(f"| {row[col]:<{column_widths[col]}} ")
        results_file.write("|\n")
    results_file.write("\n")


def print_statistics(query_id, query_text, relevant_docs, retrieved_docs, total_relevant_docs, k, ranking, boost):
    if k > len(retrieved_docs):
        retrieved_docs = retrieved_docs[:k]
    relevant_docs = [doc for doc in relevant_docs if doc in total_relevant_docs]

    query_precision = precision(relevant_docs, retrieved_docs)
    query_recall = recall(relevant_docs, total_relevant_docs)

    print("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
    print()
    print(f"    Evaluation Statistics - top {k} retrieved documents")
    print(f"    Ranking: {ranking} - Boost: {boost}")
    print()
    print(f"    Query ID: {query_id}")
    print(f"    Query Text: {query_text}")
    print()
    print(f"    Number of relevant ranked documents: {len(relevant_docs)}")
    print(f"    Number of retrieved ranked documents: {len(retrieved_docs)}")
    print(f"    Number of total relevant documents: {len(total_relevant_docs)}")
    print(f"    Precision: {query_precision}")
    print(f"    Recall: {query_recall}")
    print(f"    F-measure: {f_measure(query_precision, query_recall)}")
    print(f"    Average Precision: {average_precision(relevant_docs, retrieved_docs)}")
    print()
    print("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")


def print_efficiency_statistics(query_times, ranking, boost):
    print("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")
    print()
    print(f"    Efficiency Statistics")
    print(f"    Ranking: {ranking} - Boost: {boost}")
    print()
    print(f"    Query Throughput: {query_throughput(query_times)} queries/second")
    print(f"    Median Query Latency: {median_query_latency(query_times)} seconds")
    print()
    print("-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=")


if __name__ == '__main__':
    main()
