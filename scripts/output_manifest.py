from pathlib import Path


def write_output_manifest(filename: str, cloud_uri: str) -> None:
    Path(filename).write_text(cloud_uri)
