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