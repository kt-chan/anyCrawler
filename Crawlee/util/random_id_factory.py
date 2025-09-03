import asyncio
import hashlib
from pathlib import Path
import uuid
from typing import Dict, Optional, TypedDict
from processor.file_processor import get_root_domain_from_url


class FileMetadata(TypedDict):
    name: str
    url: str
    timestamp: float


class RandomIDFactory:
    _instance: Optional["RandomIDFactory"] = None
    _initialized: bool = False

    def __new__(cls) -> "RandomIDFactory":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if not RandomIDFactory._initialized:
            self.url_to_uuid: Dict[str, str] = {}
            self.metadata: Dict[str, FileMetadata] = {}
            RandomIDFactory._initialized = True

    def generateuniqueid(self) -> str:
        return str(uuid.uuid4())

    def getfilemetadata_by_id(self, fileid: str) -> Optional[FileMetadata]:
        return self.metadata.get(fileid)

    def getfilemetadata_by_url(self, url: str) -> Optional[FileMetadata]:
        fileid = self.url_to_uuid.get(url)
        return self.getfilemetadata_by_id(fileid) if fileid else None

    def registerfilemetadata(self, url: str) -> FileMetadata:
        # Check existing URL
        if existing_id := self.url_to_uuid.get(url):
            if existing_metadata := self.metadata.get(existing_id):
                return existing_metadata

        # Generate UUID from URL hash
        root_domain = get_root_domain_from_url(url)
        url_bytes = url.encode("utf-8")
        hash_bytes = hashlib.sha256(url_bytes).digest()[:16]
        candidate_uuid = str(Path(root_domain) / str(uuid.UUID(bytes=hash_bytes)))

        # Handle collisions
        max_retries = 10
        for nonce in range(max_retries + 1):
            if nonce > 0:
                modified_url = f"{url}_{nonce}"
                modified_hash = hashlib.sha256(modified_url.encode("utf-8")).digest()[
                    :16
                ]
                candidate_uuid = str(
                    Path(root_domain) / str(uuid.UUID(bytes=modified_hash))
                )

            if existing_metadata := self.metadata.get(candidate_uuid):
                if existing_metadata["url"] == url:
                    self.url_to_uuid[url] = candidate_uuid
                    return existing_metadata
            else:
                # Create new entry
                metadata_entry: FileMetadata = {
                    "name": candidate_uuid,
                    "url": url,
                    "timestamp": asyncio.get_running_loop().time(),
                }
                self.url_to_uuid[url] = candidate_uuid
                self.metadata[candidate_uuid] = metadata_entry
                return metadata_entry

        raise RuntimeError(
            f"Failed to generate unique UUID for URL after {max_retries} retries"
        )
