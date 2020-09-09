import hashlib

from pulpcore.plugin.models import Artifact


def get_accepted_media_types(headers):
    """
    Returns a list of media types from the Accept headers.

    Args:
        headers (:class:`~aiohttp.multidict.CIMultiDictProxy` or dict):
            The request's headers to extract accepted media types from.

    Returns:
        List of media types supported by the client.

    """
    accepted_media_types = []
    for header, values in headers.items():
        if header == "Accept":
            values = [v.strip() for v in values.split(",")]
            accepted_media_types.extend(values)
    return accepted_media_types


def init_hashers():
    """Initialize all common algorithms used for hashing."""
    hashers = {}
    for algorithm in Artifact.DIGEST_FIELDS:
        hashers[algorithm] = getattr(hashlib, algorithm)()
    return hashers


def compute_digests(hashers):
    """Return the hexadecimal values of computed digests of the passed hashes."""
    digests = {}
    for algorithm in Artifact.DIGEST_FIELDS:
        digests[algorithm] = hashers[algorithm].hexdigest()
    return digests


def write_file(file_to_write, chunks):
    """Write chunks data to the passed file which was opened in the append mode.

    The function returns a file's size after the write together with computed digests
    of all common hashes (e.g. sha512, sha256, ...).
    """
    file_size = 0
    hashers = init_hashers()

    for chunk in chunks:
        while True:
            subchunk = chunk.read(2000000)
            if not subchunk:
                break
            file_to_write.write(subchunk)
            file_size += len(subchunk)

            for algorithm in Artifact.DIGEST_FIELDS:
                hashers[algorithm].update(subchunk)

    file_to_write.flush()

    digests = compute_digests(hashers)
    return file_size, digests
