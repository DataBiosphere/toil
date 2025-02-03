S3_PARALLELIZATION_FACTOR = 8
S3_PART_SIZE = 16 * 1024 * 1024
KiB = 1024
MiB = KiB * KiB

# Files must be larger than this before we consider multipart uploads.
AWS_MIN_CHUNK_SIZE = 64 * MiB
# Convenience variable for Boto3 TransferConfig(multipart_threhold=).
MULTIPART_THRESHOLD = AWS_MIN_CHUNK_SIZE + 1
# Maximum number of parts allowed in a multipart upload.  This is a limitation imposed by S3.
AWS_MAX_MULTIPART_COUNT = 10000


def get_s3_multipart_chunk_size(filesize: int) -> int:
    """Returns the chunk size of the S3 multipart object, given a file's size in bytes."""
    if filesize <= AWS_MAX_MULTIPART_COUNT * AWS_MIN_CHUNK_SIZE:
        return AWS_MIN_CHUNK_SIZE
    else:
        div = filesize // AWS_MAX_MULTIPART_COUNT
        if div * AWS_MAX_MULTIPART_COUNT < filesize:
            div += 1
        return ((div + MiB - 1) // MiB) * MiB
