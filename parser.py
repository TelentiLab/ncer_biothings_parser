import os
import unicodedata
from collections import defaultdict
from csv import DictReader

from biothings.utils.dataload import open_anyfile, dict_sweep

FILE_NOT_FOUND_ERROR = 'Cannot find input file: {}'   # error message constant

# change following parameters accordingly
source_name = 'ncer'   # source name that appears in the api response
file_name = 'sample_data'   # name of the file to read
delimiter = '\t'    # the delimiter that separates each field


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
    assert os.path.exists(input_file), FILE_NOT_FOUND_ERROR.format(input_file)

    with open(file_name, 'r') as file:
        results = defaultdict(list)
        for line in file:
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
            results[_id].append(variant)

        for k, v in results.items():
            yield {
                '_id': k,
                source_name: v
            }
