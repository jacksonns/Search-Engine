<h1 align="center"> Search Engine </h1>

A search engine is a software system or an online service that helps users find information on the internet by retrieving and displaying relevant results in response to their queries. Search engines play a crucial role in organizing and making vast amounts of web content accessible to users. This repository has basic implementations of some of its components:
* **Crawler**: also known as a spider or bot, is a program that browses the internet, visiting web pages and collecting information from them. It starts from a list of seed URLs and follows links to other pages, downloading and storing its contents in a database for further processing (in this case, they are stored on WARC files).

* **Indexer**: is responsible for processing the web pages collected by the crawler and creating an index of the content. It extracts text, metadata, and other relevant information from each page and stores it in a structured format that allows for efficient searching. The index contains information about the words on each page, their locations, and other data that helps with ranking and retrieving search results.

* **Query Processor**: the query processor handles user queries and interprets them to retrieve relevant results from the index. It may involve stemming (reducing words to their root form), stop words removal, synonym recognition, and other natural language processing techniques to understand the user's intent and match it with the indexed content.

## Setup

Creating a Python 3.8.10 virtual environment (Ubuntu):

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Crawler

The crawler will store data from webpages on WARC files. To start crawling, the following commands should be run:

```bash
cd crawler
python3 main.py -s seeds.txt -n N [-d]
```

```-s``` argument is the .TXT file containing the seed URLs

```-n``` argument is the number of pages to be crawled

```-d``` argument is optional. If provided, activates verbose mode


## Indexer

The indexer creates a file with the inverted index, storing tokens extracted from the corpus and its frequency on each document.

```bash
cd indexer
python3 main.py -m 1024 -c path/to/corpus -i path/to/index.txt
```

```-m``` argument is the Memory Limit 

```-c``` argument is the path to the directory containing the crawled WARC files

```-i``` argument is the path to file where the index will be stored.
