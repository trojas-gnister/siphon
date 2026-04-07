"""Custom transform functions for the incident XML import test."""


def build_work_location(address, city, state, postal_code, country):
    """Concatenate location components, skipping empty values."""
    parts = [str(p) for p in [address, city, state, postal_code, country]
             if p is not None and str(p).strip()]
    result = " ".join(parts)
    return result.replace('"', '').replace('\r', '').replace('\n', '') if result else ""


def reverse_name(name):
    """Convert 'LastName, FirstName' to 'FirstName LastName'."""
    if not name or not isinstance(name, str) or "," not in name:
        return name
    parts = name.split(",", 1)
    return f"{parts[1].strip()} {parts[0].strip()}"


def resolve_first_name(first_name, last_name):
    """Return first_name if non-empty, else last_name, else 'Unknown'."""
    if first_name and str(first_name).strip():
        return str(first_name).strip()
    if last_name and str(last_name).strip():
        return str(last_name).strip()
    return "Unknown"
