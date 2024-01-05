import asyncio
from asyncio import Queue
import logging
from backoff import on_exception, expo
from aiobotocore.config import AioConfig
from aiobotocore.session import AioSession


# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


@on_exception(expo, Exception, max_tries=8)
async def list_s3_common_prefixes(bucket_name: str, prefix: str, s3_client: object) -> None:
    """
    Asynchronously returns a generator that yields the common prefixes of S3 objects in a given bucket and prefix.

    Args:
        bucket_name (str): The name of the S3 bucket.
        prefix (str): The prefix of the S3 objects.
        s3_client (botocore.client.S3): The S3 client used to interact with the S3 service.

    Yields:
        str: The key of each S3 object.

    Raises:
        Exception: If an error occurs when listing the S3 objects.

    """
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(
        Bucket=bucket_name, Prefix=prefix, Delimiter='/')
    async for page in page_iterator:
        if 'CommonPrefixes' in page:
            for folder in page['CommonPrefixes']:
                subfolder_name = folder['Prefix']
                yield subfolder_name
        else:
            logging.info(
                "No common prefixes found in bucket %s with prefix %s", bucket_name, prefix)


# Function with exponential backoff retry logic for S3 object listing
@on_exception(expo, Exception, max_tries=8)
async def list_s3_objects(bucket_name: str, prefix: str, s3_client: object, delimiter='/') -> None:
    """
    Asynchronously returns a generator that yields the keys of S3 objects in a given bucket and prefix.

    Args:
        bucket_name (str): The name of the S3 bucket.
        prefix (str): The prefix of the S3 objects.
        s3_client (botocore.client.S3): The S3 client used to interact with the S3 service.

    Yields:
        str: The key of each S3 object.

    Raises:
        Exception: If an error occurs when listing the S3 objects.

    """
    paginator = s3_client.get_paginator('list_objects_v2')
    # print("Listing objects in bucket: " +
    #      bucket_name + " with prefix: " + prefix)
    page_iterator = paginator.paginate(
        Bucket=bucket_name, Prefix=prefix, Delimiter=delimiter)
    async for page in page_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                e_tag = obj['ETag']
                date_modified = obj['LastModified']
                size = obj['Size']
                yield {'key': key, 'e_tag': e_tag,
                       'date_modified': date_modified, 'size': size}
        else:
            logging.info(
                "No objects found in bucket %s with prefix %s", bucket_name, prefix)



async def retrieve_files_queue(bucket_name, prefix: str, n: int, s3_client: object) -> None:
    """
    Retrieves files from an S3 bucket using a breadth-first search algorithm.

    Args:
        bucket_name (str): The name of the S3 bucket.
        prefix (str): The prefix to filter the objects in the bucket.
        n (int): The number of worker tasks to create.
        s3_client (S3Client): The S3 client used to interact with the bucket.

    Returns:
        None

    Raises:
        Exception: If there is an error retrieving files from the bucket.
    """
    queue = asyncio.Queue()  # Fixed queue initialization
    queue.put_nowait((prefix, 0))

    async def worker(i: int) -> None:
        while True:
            current_prefix, depth = await queue.get()
            try:
                if current_prefix is None:
                    break
                elif depth < 2 and current_prefix.endswith('/'):
                    print("Putting in queue " + current_prefix)
                    async for obj_key in list_s3_common_prefixes(bucket_name, current_prefix, s3_client):
                        await queue.put((obj_key, depth + 1))

                    async for obj_key in list_s3_objects(bucket_name, current_prefix, s3_client, '/'):
                        print(obj_key)
                else:
                    print(f"Worker {i}#: Getting files from " + current_prefix)
                    async for obj_key in list_s3_objects(bucket_name, current_prefix, s3_client, ''):
                        print(obj_key)
            except Exception as e:
                logging.error(
                    "Error retrieving files from %s: %s", current_prefix, e)
            finally:
                queue.task_done()

    workers = [asyncio.create_task(worker(i)) for i in range(n)]

    await queue.join()
    for _ in workers:
        await queue.put((None, 0))  # Sentinel values to stop workers


    # Wait for all worker tasks to complete
    await asyncio.gather(*workers)

    queue = Queue()
    queue.put_nowait((prefix, 0))


async def retrieve_s3_files(s3_path: str, n: int) -> None:
    """
    Retrieves files from an S3 bucket using the given S3 path and the maximum number of simultaneous tasks.

    Parameters:
        - s3_path (str): The S3 path specifying the bucket and prefix.
        - n (int): The maximum number of simultaneous tasks.

    Returns:
        None

    Raises:
        - Exception: If an error occurs during the retrieval process.
    """
    bucket_name, prefix = parse_s3_path(s3_path)
    config = AioConfig(max_pool_connections=n)
    session = AioSession()
    try:
        async with session.create_client('s3', region_name='eu-west-1', config=config) as s3_client:
            await retrieve_files_queue(bucket_name, prefix, n, s3_client)

    except Exception as e:
        logging.error("Error in main retrieval process: %s", e)

# Helper function to parse S3 path


def parse_s3_path(s3_path: str) -> (str, str):
    """
    Parses the S3 path into bucket name and prefix.

    Args:
        s3_path (str): The S3 path to parse.

    Returns:
        tuple: A tuple containing the bucket name and prefix.
    """
    if not s3_path.startswith('s3://'):
        raise ValueError("Invalid S3 path format. Must start with 's3://'.")

    parts = s3_path.replace('s3://', '').split('/', 1)
    if len(parts) == 1:
        # Return the bucket name and an empty prefix if no prefix is provided
        return parts[0], ''
    return parts[0], parts[1]


async def main():
    """
    Asynchronous function that serves as the entry point for the program.

    This function retrieves files from an S3 bucket using the provided S3 path and degree of parallelism.

    Parameters:
    - s3_path (str): The S3 path from which to retrieve the files.
    - n (int): The degree of parallelism, i.e., the number of tasks to execute concurrently.

    Returns:
    This function does not return any value.
    """
    s3_path = 's3://prd-dct-dlk-silver/payment/onepay_v2/transactions_delta/'
    n = 10  # Degree of parallelism
    print(f"Retrieving files from {s3_path} with {n} tasks")
    await retrieve_s3_files(s3_path, n)

# Create an event loop and run the main function
loop = asyncio.get_event_loop()
loop.run_until_complete(main())
