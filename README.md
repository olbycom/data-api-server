# Data API Server

A server for querying data through the Nekt API.

## Dependencies

Install dependencies using `uv`:

```bash
uv sync
```

## Run the Application

Run the application using `uv`:

```bash
uv run main.py
```

## Using the Application

### Base URL

All requests should be made against:

```
localhost:5001
```

### Server Status

Check if the server is running:

```bash
curl --location 'localhost:5001/api/health'
```

### Authentication

All requests, except `Server status`, should include a header `x-api-key`. The value should be the API Key you created in your [Workspace Settings](https://app.nekt.ai/settings/api-keys).

### Create Queries

To create queries, use the endpoint:

```
POST localhost:5001/api/queries
```

You can query your data using two different approaches:
* Using layer and table names
* Using a SQL query

Both approaches will initialize the Explorer application on your cloud provider (configured at Nekt) and return a `query_slug`, which is required to execute the query and fetch the results.

#### Using Layer and Table Names:

```bash
curl --location 'localhost:5001/api/queries' \
--header 'Content-Type: application/json' \
--header 'x-api-key: YOUR_API_KEY' \
--data '{
    "layer": "layer_name",
    "table": "table_name"
}'
```

> [!TIP]
> This endpoint also supports the `limit` parameter, which can speed up query execution and save costs for large tables when you're exploring data:

```bash
curl --location 'localhost:5001/api/queries' \
--header 'Content-Type: application/json' \
--header 'x-api-key: YOUR_API_KEY' \
--data '{
    "layer": "layer_name",
    "table": "table_name",
    "limit": 100
}'
```

#### Using SQL Query:

```bash
curl --location 'localhost:5001/api/queries' \
--header 'Content-Type: application/json' \
--header 'x-api-key: YOUR_API_KEY' \
--data '{
    "sql_query": "SELECT\n\t*\nFROM\n\t\"layer_name\".\"table_name\""
}'
```

#### Response with Query Slug

Using this endpoint will return an object that has a `query_slug`. You'll need this to execute the query and get its results:

```json
{
    "message": "Query created successfully",
    "query_slug": "explorer-query-8OSP",
    "sql_query": "SELECT\n\t*\nFROM\n\t\"layer_name\".\"table_name\"",
    "status": "success"
}
```

### Execute Query and Fetch Results

To fetch results, use the endpoint:

```bash
curl --location 'localhost:5001/api/queries/explorer-query-8OSP/results?page_number=1' \
--header 'Content-Type: application/json' \
--header 'x-api-key: YOUR_API_KEY'
```

This will return the result of your query as a list of JSON objects:

```json
{
    "query_slug": "explorer-query-8OSP",
    "page": 1,
    "data": [
        {
            "id": "2666b548-e028-47bd-b44d-0704f2c3ebb8",
            "created_at": "2025-02-07T21:26:11.523",
            "updated_at": "2025-02-08T10:12:58.622",
            "description": "Description of first item",
            ...
        },
        {
            "id": "2691aa3a-278a-4cad-a152-c678d5cd7693",
            "created_at": "2024-12-23T20:02:32.046",
            "updated_at": "2025-03-06T21:30:57.207",
            "description": "Description of second item",
            ...
        }
    ]
}
```

This endpoint has a page size of 100 records. If the `data` array contains 100 records, you can fetch the next page by incrementing the `page_number` parameter. Continue fetching pages until a response has fewer than 100 records or an empty array:

```bash
curl --location 'localhost:5001/api/queries/explorer-query-8OSP/results?page_number=2' \
--header 'Content-Type: application/json' \
--header 'x-api-key: YOUR_API_KEY'
```

```json
{
    "query_slug": "explorer-query-8OSP",
    "page": 2,
    "data": []
}
```

