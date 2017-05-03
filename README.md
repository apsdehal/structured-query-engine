# Structured Query Engine

Structured Query Engine is an Elasticsearch like search engine which supports real time indexing of documents with some particular structure and searching over them with complex compound query through a RESTful API.

SQE supports most of the basic features of Elasticsearch which includes real time addition, updation and deletion of documents through a RESTful API and has support for building queries for searching using a Query DSL (which is again similar to Elasticsearch).

This project serves as a base for creating advanced structured search engines and serves as a concise implementation of Elasticsearch.

For a demo built with structured query engine, see [movie recommendations project](https://github.com/apsdehal/movie-recommendations)

## Installation

- First we need to install google's snappy which is used for compressing the inverted index.

**DEB-based**: `sudo apt-get install libsnappy-dev`

**RPM-based**: `sudo yum install libsnappy-devel`

**Brew**:  `brew install snappy`

- Recommended Version: `Python >= 3.5`
- Use `pip install -r requirements.txt` to install python dependencies


## Running

- Run using `python -m app.start`

## Indexing and Querying

- First create an index with proper mappings and settings. See this [example](https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html#mappings). Nested, primitive, keyword and text datatypes are supported for the fields.
- Add documents with POST for automatic id generation or PUT for specific id. See [this](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html#_automatic_id_generation) and [this](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html#docs-index_)
- For querying on data, see this [documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html). SQE supports leaf and compound queries at one nested level at the moment.

