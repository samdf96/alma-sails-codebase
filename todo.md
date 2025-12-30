# TODOs

- Once sqlite3 is added to casa containers, add back in database updates to:
  - run_listobs.sh
  - run_split.sh

- Re-format the leftover functions in db.py to new style

## IDEAS

- Implement DualPath class that knows both VM and platform locations:

```python
class DualPath:
    """Path that knows both VM and platform locations."""
    
    def __init__(self, path_str: str):
        # Auto-detect which type it is
        if str(path_str).startswith(str(VM_MOUNT_PREFIX)):
            self._vm = Path(path_str)
            self._platform = Path(str(path_str).replace(str(VM_MOUNT_PREFIX), str(PLATFORM_PREFIX)))
        else:
            self._platform = Path(path_str)
            self._vm = Path(str(path_str).replace(str(PLATFORM_PREFIX), str(VM_MOUNT_PREFIX)))
    
    @property
    def vm(self) -> Path:
        """Get VM mount path."""
        return self._vm
    
    @property
    def platform(self) -> str:
        """Get platform native path."""
        return str(self._platform)
    
    def __str__(self):
        """Default to VM path (for local operations)."""
        return str(self._vm)
    
    def __fspath__(self):
        """Support pathlib operations."""
        return str(self._vm)
```

The usage pattern would be something like:

```python
# Create once
splits_dir = DualPath(DATASETS_DIR / to_dir_mous_id(mous_id) / "splits")

# Use VM path for local operations
splits_dir.vm.mkdir(parents=True, exist_ok=True)
with open(splits_dir.vm / "file.txt", "w") as f:
    f.write("test")

# Use platform path for headless sessions
job_id = submit_headless_session(
    command=f"bash {script} {splits_dir.platform}"
)

# Works with Path operations automatically
json_path = splits_dir.vm / "payload.json"
```
