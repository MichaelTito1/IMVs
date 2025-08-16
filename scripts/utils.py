import re

def remove_table_suffixes(sql: str) -> str:
    """
    Remove _{number} suffixes from table names (quoted or unquoted)
    in SQL statements.
    """
    pattern = re.compile(
        r'''(?P<quote>\")?
            (?P<name>[A-Za-z_][A-Za-z0-9_]*?)
            _\d+
            (?P=quote)?
            (?=\s|$|,|;|\)|\"|\.)
        ''',
        flags=re.VERBOSE
    )
    return pattern.sub(lambda m: f'{m.group("quote") or ""}{m.group("name")}{m.group("quote") or ""}',
                       sql)

def add_staging_prefix(sql_statement):
    """
    Add 'staging_' prefix to quoted table names in SQL statement.
    For write operations (INSERT, UPDATE, DELETE, MERGE), the target table stays unchanged.
    Also updates column references to match the prefixed table names.
    """
    # Remove semicolon and strip whitespace  
    sql = sql_statement.rstrip(';').strip()
    
    # Check if it's an INSERT operation and extract the target table
    insert_pattern = r'^\s*(INSERT\s+INTO)\s+"([^"]+)"'
    insert_match = re.match(insert_pattern, sql, re.IGNORECASE)
    
    # Check if it's a SELECT statement
    select_pattern = r'^\s*SELECT\b'
    is_select = re.match(select_pattern, sql, re.IGNORECASE) is not None
    
    # Check if it's an ignored write operation
    ignored_write_pattern = r'^\s*(UPDATE|DELETE\s+FROM|MERGE\s+INTO)\b'
    is_ignored_write = re.match(ignored_write_pattern, sql, re.IGNORECASE) is not None
    
    if is_ignored_write:
        # If it's UPDATE, DELETE, or MERGE, return unchanged
        return sql_statement
    
    target_table = None
    is_insert = False
    if insert_match:
        target_table = insert_match.group(2)
        is_insert = True
    
    # Identify which quoted items are likely table names vs column names
    # Table names typically appear after keywords like FROM, JOIN, INTO, UPDATE
    table_keywords = r'\b(?:FROM|JOIN|INNER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|FULL\s+JOIN|CROSS\s+JOIN|INTO|UPDATE|DELETE\s+FROM|MERGE\s+INTO)\s+'
    
    # Find table names that appear after these keywords
    table_name_pattern = table_keywords + r'"([^"]+)"'
    explicit_tables = re.findall(table_name_pattern, sql, re.IGNORECASE)
    
    # Also find tables in comma-separated lists (like FROM "table1", "table2")
    comma_tables = []
    from_pattern = r'\bFROM\s+((?:"[^"]+"\s*,\s*)*"[^"]+")'
    from_matches = re.findall(from_pattern, sql, re.IGNORECASE)
    for match in from_matches:
        comma_tables.extend(re.findall(r'"([^"]+)"', match))
    
    # Combine all table names
    table_names = set(explicit_tables + comma_tables)
    
    # Create mapping of original table names to prefixed names
    table_mappings = {}
    target_table_column_mapping = {}
    
    for table_name in table_names:
        if is_insert and target_table and table_name.lower() == target_table.lower():
            # For INSERT target table: map to staging version for FROM/JOIN, but we'll handle INSERT INTO separately
            table_mappings[table_name] = f"staging_{table_name}"
            # Don't need separate column mapping since we're using staging version everywhere except INSERT INTO
        elif table_name.startswith('staging_'):
            # Already prefixed
            table_mappings[table_name] = table_name  
        else:
            # Add prefix to all other tables
            table_mappings[table_name] = f"staging_{table_name}"
    
    # Apply the mappings to the SQL
    result = sql
    
    # For INSERT statements, we need to protect the INSERT INTO clause from replacement
    insert_into_placeholder = None
    if is_insert and target_table:
        # Replace INSERT INTO clause with a placeholder to protect it
        insert_into_pattern = f'(INSERT\\s+INTO\\s+)"{re.escape(target_table)}"'
        insert_into_placeholder = f'__INSERT_INTO_PLACEHOLDER__{target_table}__'
        result = re.sub(insert_into_pattern, f'\\1{insert_into_placeholder}', result, flags=re.IGNORECASE)
    
    for old_name, new_name in table_mappings.items():
        if old_name != new_name:  # Only update if table name actually changed
            # Replace table name declarations (after keywords)
            table_decl_pattern = f'(\\b(?:FROM|JOIN|INNER\\s+JOIN|LEFT\\s+JOIN|RIGHT\\s+JOIN|FULL\\s+JOIN|CROSS\\s+JOIN|INTO|UPDATE|DELETE\\s+FROM|MERGE\\s+INTO)\\s+)"{re.escape(old_name)}"'
            result = re.sub(table_decl_pattern, f'\\1"{new_name}"', result, flags=re.IGNORECASE)
            
            # Replace in comma-separated lists (both with trailing comma and without)
            comma_pattern1 = f'"{re.escape(old_name)}"(\\s*,)'  # table with comma after
            comma_pattern2 = f'(,\\s*)"{re.escape(old_name)}"'  # table with comma before
            comma_pattern3 = f'"{re.escape(old_name)}"(?!\\s*\\.)'  # standalone table (not followed by dot for column ref)
            
            result = re.sub(comma_pattern1, f'"{new_name}"\\1', result)
            result = re.sub(comma_pattern2, f'\\1"{new_name}"', result)
            # Handle standalone table names not followed by comma or dot
            result = re.sub(comma_pattern3, f'"{new_name}"', result)
            
            # Replace column references
            column_pattern = f'"{re.escape(old_name)}"\\.'
            result = re.sub(column_pattern, f'"{new_name}".', result)
    # Restore the original INSERT INTO clause if we used a placeholder
    if insert_into_placeholder and target_table:
        result = result.replace(insert_into_placeholder, f'"{target_table}"')
    
    return result