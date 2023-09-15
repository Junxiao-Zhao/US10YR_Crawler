# US10YR crawler

## Introduction

This is a crawler to download US10YR' data.

## Prerequisites

`python >= 3.7`

`pip install -r requirements.txt`

## Usage

```
usage: start_crawler.py [-h] -s START -e END

Download stocks' data in the given range

options:
    -h, --help            show this help message and exit
    -s START, --start START
                          Start date (include) in format %Y%m%d
    -e END, --end END     End date (exclude) in format %Y%m%d
```

## Configuration

See [main.cfg](./config/main.cfg).

## Notes

- Change the `save_fp` in the [main.cfg](./config/main.cfg) to your destination.
- `num_crawler` in the [main.cfg](./config/main.cfg) controls the number of crawler threads; there will always exist one writer thread.