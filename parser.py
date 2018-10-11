import os
import logging
from biothings.utils.dataload import dict_sweep

FILE_NOT_FOUND_ERROR = 'Cannot find input file: {}'   # error message constant
FILE_LINES = 290366
# FILE_LINES = 282932324

# change following parameters accordingly
source_name = 'ncer'   # source name that appears in the api response
file_name = 'sample_data'   # name of the file to read
delimiter = '\t'    # the delimiter that separates each field
logging.basicConfig(format='%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s', level=logging.INFO)
logger = logging.getLogger('ncer_logger')


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
        logger.info('start reading file: {}'.format(file_name))
        count = 0
        for line in file:
            logger.info('reading line {} ({}%)'.format(count, format(count / FILE_LINES, '.2f')))
            count += 1
            # read and parse each line into a dict
            chrom, start, end, percentile = line.strip().split(delimiter)
            _id = '{chrom}:g.{start}_{end}'.format(chrom=chrom, start=start, end=end)
            variant = {
                "chrom": chrom,
                "start": start,
                "end": end,
                "percentile": percentile,
            }
            variant = dict_sweep(variant, vals=['', 'null', 'N/A', None, [], {}])
            yield {
                "_id": _id,
                source_name: variant
            }
        logger.info('complete, {} lines read'.format(count))
