import os
import logging
import datetime
import time

FILE_NOT_FOUND_ERROR = 'Cannot find input file: {}'  # error message constant
FILE_LINES = 282932324

source_name = 'ncer'  # source name that appears in the api response
file_name = 'ncER_10bpBins_percentile_version1.txt'   # name of the file to read
delimiter = '\t'  # the delimiter that separates each field

# configure logger
logging.basicConfig(format='%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s', level=logging.INFO)
logger = logging.getLogger('ncer_logger')


def version(self):
    return 'v1'


def load_data(data_folder: str):
    """
    Load data from a specified file path. Parse each line into a dictionary according to the schema
    given by `data_schema`. Then process each dict by normalizing data format, remove null fields (optional).
    Append each dict into final result using its id.

    :param data_folder: the path(folder) where the data file is stored
    :return: a generator that yields data.
    """
    input_file = os.path.join(data_folder, file_name)
    # raise an error if file not found
    if not os.path.exists(input_file):
        logger.error(FILE_NOT_FOUND_ERROR.format(input_file))
        raise FileExistsError(FILE_NOT_FOUND_ERROR.format(input_file))

    with open(input_file, 'r') as file:
        start_time = time.time()
        logger.info(f'start reading file: {file_name}')
        count = 0
        skipped = []
        for line in file:
            count += 1
            ratio = count / FILE_LINES
            time_left = datetime.timedelta(seconds=(time.time() - start_time) * (1 - ratio) / ratio)
            # format to use 2 decimals for progress
            logger.info(f'reading line {count} ({(count / FILE_LINES * 100):.2f}%), estimated time left: {time_left}')
            # read and parse each line into a dict
            try:
                chrom, start, end, percentile = line.strip().split(delimiter)
            except ValueError:
                logger.error(f'failed to unpack line {count}: {line}')
                skipped.append(line)
                continue  # skip error line

            chrom = chrom.replace('chr', '')
            _id = f'chr{chrom}:g.{start}_{end}'
            try:
                # enforce data type
                variant = {
                    'chrom': chrom,
                    'start': int(start),
                    'end': int(end),
                    'percentile': float(percentile),
                }
            except ValueError as e:
                logger.error(f'failed to cast type for line {count}: {e}')
                skipped.append(line)
                continue  # skip error line

            # commit an entry by yielding
            yield {
                "_id": _id,
                source_name: variant
            }
        logger.info(f'parse completed, {len(skipped)}/{count} lines skipped.')
        for x in skipped:
            logger.info(f'skipped line: {x}')
