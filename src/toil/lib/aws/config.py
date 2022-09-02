import math

from toil.lib.units import MIB, TB

S3_PARALLELIZATION_FACTOR = 8
S3_PART_SIZE = 16 * MIB

# AWS Defined Limits
# https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html
AWS_MAX_CHUNK_SIZE = 5 * TB
# Files must be larger than this before we consider multipart uploads.
AWS_MIN_CHUNK_SIZE = 64 * MIB
# Convenience variable for Boto3 TransferConfig(multipart_threshold=).
MULTIPART_THRESHOLD = AWS_MIN_CHUNK_SIZE + 1
# Maximum number of parts allowed in a multipart upload.  This is a limitation imposed by S3.
AWS_MAX_MULTIPART_COUNT = 10000
# Note: There is no minimum size limit on the last part of a multipart upload.

# The chunk size we chose arbitrarily, but it must be consistent for etags
DEFAULT_AWS_CHUNK_SIZE = 128 * MIB
assert AWS_MAX_CHUNK_SIZE > DEFAULT_AWS_CHUNK_SIZE > AWS_MIN_CHUNK_SIZE


def get_s3_multipart_chunk_size(file_size: int) -> int:
    if file_size >= AWS_MAX_CHUNK_SIZE * AWS_MAX_MULTIPART_COUNT:
        return AWS_MAX_CHUNK_SIZE
    elif file_size <= DEFAULT_AWS_CHUNK_SIZE * AWS_MAX_MULTIPART_COUNT:
        return DEFAULT_AWS_CHUNK_SIZE
    else:
        return math.ceil(file_size / AWS_MAX_MULTIPART_COUNT)
