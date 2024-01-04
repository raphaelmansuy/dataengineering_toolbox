# How to query efficiently an AWS S3 bucket presenting a folder hierarchy structure

## Introduction

The AWS S3 listObjects API allows retrieving objects from an S3 bucket, but has some limitations when trying to use it to filter objects by date:

- No server-side filtering by metadata: listObjects only allows basic filtering parameters like prefix, delimiter, etc. There is no ability to filter by object metadata like lastModified date on the server side.

- Pagination required: listObjects only returns up to 1000 results per call. To retrieve all objects matching a date filter would require making multiple listObjects calls and filtering the aggregated results on the client side. This doesn't scale well for buckets with large numbers of objects.

- Requires full scan: Without index support in S3, filtering objects by dates requires a full scan of every object in the bucket, which is very inefficient.

By using a date partition hierarchy structure like the examples shown earlier, we can work around these limitations and enable very efficient date-based filtering:

- The partition structure creates "indexes" by date that we can leverage. Instead of scanning all bucket objects, we only need to list the specific "date partition" we want.

- The filtering happens on S3 folder structure level, avoiding full scans. S3 listObjects automatically handles the recursion through the folder hierarchy.

- Date filtering with precision no longer needs pagination. Fetching a day/hour partition likely retrieves fewer than 1000 objects.

For example to get all 2024/01/04 files, we just list the s3://bucket/year=2024/month=01/day=04/ folder directly. This provides efficient date filtering on S3 without needing any client-side post-filtering.

## Examples of S3 Folder Partitioning

Here are 10 examples of AWS S3 folder prefix hierarchies that organize files by date and/or hour:

**Example 1:**

```
s3://bucket/year=2024/month=01/day=04/hour=10/
```

This organizes files uploaded within an hour into separate folders by year, month, day, and hour.

**Example 2:**

```
s3://bucket/year=2024/month=01/day=04/file.csv
```

This stores files for a day in folders by year, month and day.

**Example 3:**

```
s3://bucket/year=2024/week=01/day=thursday/file.csv
```

This organizes by year, week of the year, day of the week.

**Example 4:**

```
s3://bucket/year=2024/month=01/date=20240104/
```

This uses a single date field to partition.

**Example 5:**

```
s3://bucket/topic=logs/year=2024/month=01/day=04/file.log
```

This adds a topic partition to organize different types of data.

**Example 6:**

```
s3://bucket/env=prod/service=api/year=2024/month=01/day=04/hour=11/file.json
```

This includes environment and service partitioning.

**Example 7:**

```
s3://bucket/country=US/year=2024/week=01/day=04/customerid=123/file.csv
```

This adds country and customer ID partitioning.

**Example 8:**

```
s3://bucket/year=2024/month=01/day=04/hour=00-11/file.csv
s3://bucket/year=2024/month=01/day=04/hour=12-23/file.csv
```

This divides a day into 2 partitions by hour.

**Example 9:**

```
s3://bucket/metric=cpu/year=2024/month=01/day=04/region=west/instance=1/cpu_metrics.csv
```

This organizes infrastructure metric data by dimension.

**Example 10:**

```
s3://bucket/year=2024/month=01/day=04/job=etl1/
s3://bucket/year=2024/month=01/day=04/job=etl2/
```

This separates outputs of different ETL jobs.

## Example of function to generate a list of S3 prefixes

```python

import datetime
from typing import List

def generate_date_prefixes(pattern: str, start_date: datetime.datetime, end_date: datetime.datetime) -> List[str]:
    """
    Generates S3 prefix patterns based on date ranges.

    Given an S3 prefix pattern with date format placeholders (%Y, %m, %d, %H, %M), 
    and a start and end date range, this generates the list of actual prefix 
    strings with the dates expanded out.

    Args:
        pattern (str): 
            Prefix pattern string with datetime format codes like %Y, %m, %d, %H, %M
        
        start_date (datetime.datetime):
            Start date as a datetime object 

        end_date (datetime.datetime):
            End date as a datetime object

    Returns:
        List[str]: List of actual S3 prefix strings with dates expanded
    """
    if start_date > end_date:
        raise ValueError("Start date must be before or equal to end date.")

    prefixes = []

    curr = start_date
    while curr <= end_date:
        prefix = curr.strftime(pattern)
        prefixes.append(prefix)

        # Increment the date
        if "%M" in pattern:
            curr += datetime.timedelta(minutes=1)
        elif "%H" in pattern:
            curr += datetime.timedelta(hours=1)
        elif "%d" in pattern:
            curr += datetime.timedelta(days=1)
        elif "%m" in pattern:
            # Handle month increment
            year, month = curr.year, curr.month
            month += 1
            if month > 12:
                month = 1
                year += 1
            curr = curr.replace(year=year, month=month, day=1)
        elif "%Y" in pattern:
            curr = curr.replace(year=curr.year + 1, month=1, day=1)
        else:
            raise ValueError("Pattern must contain a date placeholder.")

    return prefixes
  ```