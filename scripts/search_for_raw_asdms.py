from pathlib import Path


def find_uid_directories(base_path):
    """
    Find all directories that start with 'uid' and end with '.ms'
    within the immediate subdirectories of base_path.

    Args:
        base_path: Path to the parent directory to search within

    Returns:
        List of Path objects matching the criteria
    """
    base = Path(base_path)

    # Check if base path exists
    if not base.exists():
        print(f"Error: Path '{base_path}' does not exist")
        return []

    if not base.is_dir():
        print(f"Error: Path '{base_path}' is not a directory")
        return []

    # Find matching directories using glob
    # */ searches within each subdirectory of base_path
    matching_dirs = [
        d
        for d in base.glob("*/uid*.ms")
        if d.is_dir() and (d.parent / "split").is_dir()
    ]

    return matching_dirs


# Example usage
if __name__ == "__main__":
    # Set your base directory here
    base_directory = "/arc/projects/ALMA-SAILS/datasets"

    # Find matching directories
    results = find_uid_directories(base_directory)

    # Display results
    if results:
        print(
            f"Found {len(results)} matching director{'y' if len(results) == 1 else 'ies'}:"
        )
        for directory in sorted(results):
            print(f"  - {directory}")
    else:
        print("No matching directories found.")
