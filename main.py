#!/usr/bin/env python3
import io
import json
import time
from functools import wraps

import pandas as pd
import requests
from flask import Flask, Response, g, jsonify, request

app = Flask(__name__)

# Global variable to track application status
application_status = None

# Increase timeouts to at least 5 minutes (300 seconds)
APPLICATION_START_TIMEOUT = 300  # 5 minutes
QUERY_EXECUTION_TIMEOUT = 300  # 5 minutes
PAGE_SIZE = 100


def require_api_key(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        api_key = request.headers.get("x-api-key")
        if not api_key:
            return jsonify({"error": "Missing API key. Please provide 'x-api-key' header"}), 401
        g.api_key = api_key
        return f(*args, **kwargs)

    return decorated_function


def process_parquet_in_memory(presigned_url):
    """
    Download and process a parquet file from a presigned URL.

    Args:
        presigned_url: URL to download the parquet file from

    Returns:
        pandas DataFrame containing the data from the parquet file
    """
    app.logger.info("Downloading parquet file...")
    response = requests.get(presigned_url)

    if response.status_code == 200:
        app.logger.info("Download successful, processing data...")
        # Read the parquet file directly from memory
        buffer = io.BytesIO(response.content)
        df = pd.read_parquet(buffer)

        app.logger.info(f"DataFrame shape: {df.shape}")
        return df
    else:
        app.logger.error(f"Failed to download file. Status code: {response.status_code}")
        return None


def ensure_application_running(api_key):
    """
    Check if the application is running, if not start it and wait until it's active.

    Args:
        api_key: The API key for authentication

    Returns:
        bool: True if application started successfully, False otherwise
    """
    global application_status

    base_url = "https://api.nekt.ai/api/v1"
    headers = {"x-api-key": api_key}

    # Check if application is already running
    if application_status == "active":
        # Verify it's still active
        response = requests.get(f"{base_url}/explorer/application/", headers=headers)
        data = response.json()
        if data.get("status") == "active":
            return True
        application_status = None  # Reset if not active

    # Start application if not running
    app.logger.info("Starting application")
    response = requests.post(f"{base_url}/explorer/application/start/", headers=headers)
    if response.status_code not in [200, 201]:
        app.logger.error(f"Failed to start application: {response.status_code} - {response.text}")
        return False

    data = response.json()

    # Wait until application is active
    # Calculate number of retries based on timeout and check interval
    check_interval = 5  # seconds between status checks
    max_retries = APPLICATION_START_TIMEOUT // check_interval
    retry_count = 0

    while data.get("status") != "active" and retry_count < max_retries:
        app.logger.info(
            f"Waiting for application to start. Current status: {data.get('status')}. Retry {retry_count + 1}/{max_retries}"
        )
        time.sleep(check_interval)
        retry_count += 1

        response = requests.get(f"{base_url}/explorer/application/", headers=headers)
        if response.status_code not in [200, 201]:
            app.logger.error(f"Failed to check application status: {response.status_code} - {response.text}")
            return False

        data = response.json()

    if data.get("status") == "active":
        application_status = "active"
        app.logger.info("Application started successfully")
        return True
    else:
        app.logger.error(f"Application failed to start within timeout ({APPLICATION_START_TIMEOUT}s): {data}")
        return False


def execute_query(query_slug, page_number, api_key):
    """
    Execute a query for a specific page.

    Args:
        query_slug: The slug of the query to execute
        page_number: The page number to fetch
        api_key: API key for authentication

    Returns:
        Tuple of (DataFrame, error_message): DataFrame containing the results and error_message if any
    """
    base_url = "https://api.nekt.ai/api/v1"
    headers = {"x-api-key": api_key}

    # Execute query for the requested page
    app.logger.info(f"Executing query {query_slug} - page {page_number}")
    response = requests.post(
        f"{base_url}/explorer/queries/{query_slug}/execution/",
        headers=headers,
        json={"page_number": page_number},
    )

    # Consider both 200 and 201 as success
    if response.status_code not in [200, 201]:
        # Extract the most relevant error detail
        error_message = f"Request failed with status code {response.status_code}"
        try:
            error_obj = response.json()
            # Try to extract the specific error detail
            if "errors" in error_obj and len(error_obj["errors"]) > 0 and "detail" in error_obj["errors"][0]:
                error_message = error_obj["errors"][0]["detail"]
            elif "detail" in error_obj:
                error_message = error_obj["detail"]
            elif "message" in error_obj:
                error_message = error_obj["message"]
            elif "error" in error_obj:
                error_message = error_obj["error"]
        except:
            if response.text:
                error_message = response.text

        app.logger.error(f"Query execution request failed: {response.status_code} - {response.text}")
        return None, error_message

    execution = response.json()
    execution_id = execution.get("id")
    app.logger.info(f"Execution ID: {execution_id}")

    # Wait for execution to complete
    app.logger.info(f"Waiting for execution to complete - page {page_number}")
    response = requests.get(f"{base_url}/explorer/queries/{query_slug}/execution/{execution_id}/", headers=headers)
    execution = response.json()

    # Calculate number of retries based on timeout and check interval
    check_interval = 2  # seconds between status checks
    max_retries = QUERY_EXECUTION_TIMEOUT // check_interval
    retry_count = 0

    while execution.get("status") not in ["complete", "failed"] and retry_count < max_retries:
        app.logger.info(
            f"Waiting for execution to complete. Current status: {execution.get('status')}. Retry {retry_count + 1}/{max_retries}"
        )
        time.sleep(check_interval)
        retry_count += 1

        response = requests.get(f"{base_url}/explorer/queries/{query_slug}/execution/{execution_id}/", headers=headers)
        # Consider both 200 and 201 as success
        if response.status_code not in [200, 201]:
            error_message = f"Failed to check execution status: {response.status_code}"
            try:
                error_obj = response.json()
                if "errors" in error_obj and len(error_obj["errors"]) > 0 and "detail" in error_obj["errors"][0]:
                    error_message = error_obj["errors"][0]["detail"]
                elif "detail" in error_obj:
                    error_message = error_obj["detail"]
                elif "message" in error_obj:
                    error_message = error_obj["message"]
            except:
                if response.text:
                    error_message = response.text

            app.logger.error(f"Failed to check execution status: {response.status_code} - {response.text}")
            return None, error_message

        execution = response.json()

    if execution.get("status") == "failed":
        error_message = "Query execution failed"
        if "error" in execution:
            error_message = execution["error"]
        elif "message" in execution:
            error_message = execution["message"]

        app.logger.error(f"Query execution failed: {execution}")
        return None, error_message

    if execution.get("status") != "complete":
        error_message = f"Query execution timed out after {QUERY_EXECUTION_TIMEOUT}s"
        app.logger.error(f"Query execution timed out: {execution}")
        return None, error_message

    # Get results
    app.logger.info(f"Getting results - page {page_number}")
    response = requests.get(
        f"{base_url}/explorer/queries/{query_slug}/execution/{execution_id}/results/",
        headers=headers,
    )

    # Consider both 200 and 201 as success
    if response.status_code not in [200, 201]:
        error_message = f"Failed to get results: {response.status_code}"
        try:
            error_obj = response.json()
            if "errors" in error_obj and len(error_obj["errors"]) > 0 and "detail" in error_obj["errors"][0]:
                error_message = error_obj["errors"][0]["detail"]
            elif "detail" in error_obj:
                error_message = error_obj["detail"]
            elif "message" in error_obj:
                error_message = error_obj["message"]
        except:
            if response.text:
                error_message = response.text

        app.logger.error(f"Failed to get results: {response.status_code} - {response.text}")
        return None, error_message

    results = response.json()
    df = process_parquet_in_memory(results.get("presigned_url"))

    if df is None or df.empty:
        app.logger.info(f"No results returned for page {page_number}")
        if df is None:
            error_message = "Failed to process parquet file"
            return None, error_message
    else:
        app.logger.info(f"Received {len(df)} results for page {page_number}")

    return df, None


def create_query(sql_query, api_key):
    """
    Create a new query in the Nekt system.

    Args:
        sql_query: SQL query to execute
        api_key: API key for authentication

    Returns:
        The query slug if successful, None otherwise
    """
    base_url = "https://api.nekt.ai/api/v1"
    headers = {"x-api-key": api_key}

    app.logger.info("Creating query")
    response = requests.post(
        f"{base_url}/explorer/queries/",
        headers=headers,
        json={"sql_query": sql_query},
    )

    # Consider both 200 and 201 as success
    if response.status_code not in [200, 201]:
        app.logger.error(f"Failed to create query: {response.status_code} - {response.text}")
        return None

    query = response.json()
    query_slug = query.get("slug")
    app.logger.info(f"Query slug: {query_slug}")

    return query_slug


def build_sql_query(layer, table, limit=None):
    """
    Build a SQL query string from layer, table, and optional limit.

    Args:
        layer: The database layer/schema
        table: The table name
        limit: Optional row limit

    Returns:
        SQL query string
    """
    # Ensure layer and table are properly quoted
    quoted_layer = f'"{layer}"'
    quoted_table = f'"{table}"'

    # Build the base query
    sql_query = f"SELECT\n\t*\nFROM\n\t{quoted_layer}.{quoted_table}"

    # Add limit if provided
    if limit is not None:
        sql_query += f"\nLIMIT {limit}"

    return sql_query


def create_query_and_return_slug(sql_query, api_key):
    """
    Abstract function to create a query and return the slug.

    Args:
        sql_query: SQL query to execute
        api_key: API key for authentication

    Returns:
        Tuple of (slug, error_message): slug if successful, None and error_message if failed
    """
    # Ensure application is running
    if not ensure_application_running(api_key):
        return None, "Failed to start application"

    # Create the query
    query_slug = create_query(sql_query, api_key)
    if not query_slug:
        return None, "Failed to create query"

    return query_slug, None


@app.route("/api/queries", methods=["POST"])
@require_api_key
def create_query_endpoint():
    """
    Flexible endpoint to create a query from either:
    1. A direct SQL query
    2. Layer, table, and optional limit parameters

    Returns the query slug in both cases.
    """
    data = request.json

    if not data:
        return jsonify({"error": "No JSON data provided"}), 400

    # Get API key from the request
    api_key = g.api_key

    # Check which input method is being used
    sql_query = data.get("sql_query")

    if sql_query:
        # Direct SQL query method
        # Check that no other parameters are provided
        disallowed_params = [param for param in ["layer", "table", "limit"] if param in data]
        if disallowed_params:
            return (
                jsonify(
                    {
                        "error": f"When 'sql_query' is provided, these parameters are not allowed: {', '.join(disallowed_params)}"
                    }
                ),
                400,
            )
    else:
        # Table specification method
        # Get and validate required parameters
        layer = data.get("layer")
        table = data.get("table")
        limit = data.get("limit")  # Optional

        if not layer:
            return jsonify({"error": "Missing required parameter 'layer' when 'sql_query' is not provided"}), 400
        if not table:
            return jsonify({"error": "Missing required parameter 'table' when 'sql_query' is not provided"}), 400

        # Validate limit if provided
        if limit is not None:
            try:
                limit = int(limit)
                if limit <= 0:
                    return jsonify({"error": "Limit must be a positive integer"}), 400
            except (ValueError, TypeError):
                return jsonify({"error": "Limit must be a valid integer"}), 400

        # Build SQL query from parameters
        sql_query = build_sql_query(layer, table, limit)

    # At this point, we have a valid sql_query from either method

    # Create query and get slug
    query_slug, error = create_query_and_return_slug(sql_query, api_key)
    if query_slug is None:
        return jsonify({"error": error}), 500

    # Return the query slug and the SQL query used
    return jsonify(
        {
            "query_slug": query_slug,
            "sql_query": sql_query,  # Include the SQL query for reference
            "status": "success",
            "message": "Query created successfully",
        }
    )


@app.route("/api/queries/<query_slug>/results", methods=["GET"])
@require_api_key
def execute_query_endpoint(query_slug):
    """Endpoint to execute a query and return its results"""
    # Get page number from query string, default to 1
    try:
        page_number = int(request.args.get("page_number", 1))
        if page_number < 1:
            return jsonify({"error": "Page number must be a positive integer"}), 400
    except ValueError:
        return jsonify({"error": "Page number must be a valid integer"}), 400

    # Get API key from the request
    api_key = g.api_key

    # Ensure application is running
    if not ensure_application_running(api_key):
        return jsonify({"error": "Failed to start application"}), 500

    # Execute the query for the requested page
    df, error_message = execute_query(query_slug, page_number, api_key)
    if df is None:
        # Return a simplified error response with just the most relevant error message
        error_response = {
            "error": "Query execution failed",
            "query_slug": query_slug,
            "page_number": page_number,
            "details": error_message,  # This is now just the error message text
        }
        return jsonify(error_response), 500

    # Create response with metadata
    response_data = {
        "query_slug": query_slug,
        "page": page_number,
        # Use pandas built-in json serialization for data
        "data": json.loads(df.to_json(orient="records", date_format="iso")),
    }

    # Convert the entire response to JSON and return
    return Response(response=json.dumps(response_data), status=200, mimetype="application/json")


@app.route("/api/health", methods=["GET"])
def health_check():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    # Set up logging to console
    import logging

    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    app.logger.addHandler(handler)
    app.logger.setLevel(logging.INFO)

    # Increase Flask's default timeout
    app.config["TIMEOUT"] = 300  # 5 minutes

    app.run(debug=True, host="0.0.0.0", port=5001)
