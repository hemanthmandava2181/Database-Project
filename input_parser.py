# This file is for parsing the input
def check_for_comma(column_data):
    return column_data.str.contains(',').any()

def input_parser(data_file):
    data_file = data_file.astype(str)
    cols_with_comma = [
        column for column in data_file.columns if check_for_comma(data_file[column])]

    for column in cols_with_comma:
        data_file[column] = data_file[column].str.split(
            ',').apply(lambda items: [element.strip() for element in items])

    return data_file
