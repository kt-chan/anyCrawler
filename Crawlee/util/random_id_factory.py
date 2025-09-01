import asyncio
import uuid
import hashlib


class RandomIDFactory:
    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(RandomIDFactory, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not RandomIDFactory._initialized:
            self.url_to_uuid = {}
            self.metadata = {}
            RandomIDFactory._initialized = True

    def generateuniqueid(self):
        return str(uuid.uuid4())

    def getfilemetadata_by_id(self, fileid):
        return self.metadata.get(fileid, None)

    def getfilemetadata_by_url(self, url):
        fileid = self.url_to_uuid.get(url, None)
        return self.getfilemetadata_by_id(fileid)

    def registerfilemetadata(self, url):
        if url in self.url_to_uuid:
            fileid = self.url_to_uuid[url]
            return self.getfilemetadata_by_id(fileid)

        url_bytes = url.encode("utf-8")
        hash_obj = hashlib.sha256(url_bytes)
        hash_bytes = hash_obj.digest()[:16]
        candidate_uuid = str(uuid.UUID(bytes=hash_bytes))

        nonce = 0
        max_retries = 10
        while candidate_uuid in self.metadata:
            if self.metadata[candidate_uuid]["url"] == url:
                self.url_to_uuid[url] = candidate_uuid
                return self.getfilemetadata_by_id(candidate_uuid)

            nonce += 1
            if nonce > max_retries:
                raise RuntimeError(
                    f"Failed to generate unique UUID for URL after {max_retries} retries"
                )
            modified_url = f"{url}_{nonce}"
            modified_url_bytes = modified_url.encode("utf-8")
            modified_hash_obj = hashlib.sha256(modified_url_bytes)
            modified_hash_bytes = modified_hash_obj.digest()[:16]
            candidate_uuid = str(uuid.UUID(bytes=modified_hash_bytes))

        self.url_to_uuid[url] = candidate_uuid
        self.metadata[candidate_uuid] = {
            "name": candidate_uuid,
            "url": url,
            "timestamp": asyncio.get_running_loop().time(),
        }
        return self.getfilemetadata_by_id(candidate_uuid)
