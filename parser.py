import os
import unicodedata
from collections import defaultdict
from csv import DictReader

from biothings.utils.dataload import open_anyfile, dict_sweep

FILE_NOT_FOUND_ERROR = 'Cannot find input file: {}'   # error message constant

# change following parameters accordingly
data_schema = ('chrom', 'start', 'end', 'ncer_percentile')    # field names of the data
source_name = 'ncer'   # source name that appears in the api response
file_name = 'sliding10bp_window10bp_ncER_OMNI.txt'   # name of the file to read
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

    with open_anyfile(input_file) as file:
        # Remove duplicated lines if any
        lines = set(list(file))
        # read and parse each line into a dict
        reader = DictReader(lines, fieldnames=data_schema, delimiter=delimiter)
        # access non existing keys will return an empty list by default
        results = defaultdict(list)

        for row in reader:  # start processing each lines of data (stored in dicts)
            # construct id (e.g. chr1:g.678900_679000)
            _id = '{chrom}:g.{start}_{end}'.format(chrom=row['chrom'], start=row['start'], end=row['end'])
            # optional step: normalize data
            variant = {k: unicodedata.normalize('NFKD', v) for k, v in row.items()}
            # optional step: delete invalid fields within each dict
            variant = dict_sweep(variant, vals=['', 'null', 'N/A', None, [], {}])
            # append dict to the result list using the _id
            results[_id].append(variant)

        for k, v in results.items():
            yield {
                '_id': k,
                source_name: v
            }
