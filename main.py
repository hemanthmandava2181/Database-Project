# This is the main file for reading CSV data and performing normalization operations
import pandas as pd
import csv
import normalization_procedures
import input_parser
from sql_table_creator import generate_1nf, generate_2nf_3nf, generate_bcnf_4nf_5nf

# Reading the input csv file and the dependencies text file
input_file = pd.read_csv('/content/exampleInputTable.csv')
print('Input Relation Tables:')
print(input_file)
print('\n')

with open('/content/dependency_parser.txt', 'r') as file:
    lines = [line.strip() for line in file]

dependencies = {}
for line in lines:
    left_hand_side, right_hand_side = line.split(" -> ")
    left_hand_side = left_hand_side.split(", ")
    dependencies[tuple(left_hand_side)] = right_hand_side.split(", ")
print('Dependencies:')
print(dependencies)
print('\n')

# Input from the user
target_normal_form = input(
    'Choice of the highest normal form to reach (1: 1NF, 2: 2NF, 3: 3NF, B: BCNF, 4: 4NF, 5: 5NF):')
if target_normal_form in ["1", "2", "3", "4", "5"]:
    target_normal_form = int(target_normal_form)

# Find the highest normal form of the input relation
find_high_nf = int(
    input('Find the highest normal form of the input table? (1: Yes, 2: No):'))
high_nf = 'No normalization done.'

primary_key = input(
    "Enter the Primary Key values: ").split(', ')
print('\n')

keys = ()
for key in primary_key:
    keys = keys + (key,)

primary_key = keys

mvd_dependencies = {}
if not target_normal_form == 'B' and target_normal_form >= 4:
    with open('/content/mvd_dependencies.txt', 'r') as file:
        mvd_lines = [line.strip() for line in file]

    print(mvd_lines)

    for mvd in mvd_lines:
        left_hand_side, right_hand_side = mvd.split(" ->-> ")
        left_hand_side = left_hand_side.split(
            ", ") if ", " in left_hand_side else [left_hand_side]
        left_hand_side_str = str(left_hand_side)
        if left_hand_side_str in mvd_dependencies:
            mvd_dependencies[left_hand_side_str].append(right_hand_side)
        else:
            mvd_dependencies[left_hand_side_str] = [right_hand_side]

    print('Multi-valued Dependencies')
    print(mvd_dependencies)
    print('\n')

input_file = input_parser.input_parser(input_file)

if target_normal_form == 'B' or target_normal_form >= 1:
    first_nf_table, flag_1nf = normalization_procedures.validate_first_nf(
        input_file)

    if flag_1nf:
        high_nf = 'Highest Normal Form is: 1NF'

    if target_normal_form == 1:
        if flag_1nf:
            print('Already Normalized to 1NF')
            print('\n')

        print('Queries after decomposing to 1NF:')
        print('\n')
        generate_1nf(primary_key, first_nf_table)

if target_normal_form == 'B' or target_normal_form >= 2:
    second_nf_tables, flag_2nf = normalization_procedures.validate_second_nf(
        first_nf_table, primary_key, dependencies)

    if flag_1nf and flag_2nf:
        high_nf = 'Highest Normal Form is: 2NF'

    if target_normal_form == 2:
        if flag_2nf and flag_1nf:
            print('Already Normalized to 2NF')
            print('\n')

        print('Queries after decomposing to 2NF')
        print('\n')
        generate_2nf_3nf(second_nf_tables)

if target_normal_form == 'B' or target_normal_form >= 3:
    third_nf_tables, flag_3nf = normalization_procedures.validate_third_nf(
        second_nf_tables, primary_key, dependencies)

    if flag_1nf and flag_2nf and flag_3nf:
        high_nf = 'Highest Normal Form is: 3NF'

    if target_normal_form == 3:
        if flag_3nf and flag_2nf and flag_1nf:
            print('Already Normalized to 3NF')
            print('\n')

        print('Queries after decomposing to 3NF')
        print('\n')
        generate_2nf_3nf(third_nf_tables)

if target_normal_form == 'B' or target_normal_form >= 4:
    bcnf_tables, flag_bcnf = normalization_procedures.validate_bc_nf(
        third_nf_tables, primary_key, dependencies)

    if flag_1nf and flag_2nf and flag_3nf and flag_bcnf:
        high_nf = 'Highest Normal Form is: BCNF'

    if target_normal_form == 'B':
        if flag_bcnf and flag_3nf and flag_2nf and flag_1nf:
            print('Already Normalized to BCNF')
            print('\n')

        print('Queries after decomposing to BCNF')
        print('\n')
        generate_bcnf_4nf_5nf(bcnf_tables)

if not target_normal_form == 'B' and target_normal_form >= 4:
    fourth_nf_tables, flag_4nf = normalization_procedures.validate_fourth_nf(
        bcnf_tables, mvd_dependencies)

    if flag_1nf and flag_2nf and flag_3nf and flag_bcnf and flag_4nf:
        high_nf = 'Highest Normal Form is: 4NF'

    if target_normal_form == 4:
        if flag_4nf and flag_bcnf and flag_3nf and flag_2nf and flag_1nf:
            print('Already Normalized to 4NF')
            print('\n')

        print('Queries after decomposing to 4NF')
        print('\n')
        generate_bcnf_4nf_5nf(fourth_nf_tables)

if not target_normal_form == 'B' and target_normal_form >= 5:
    fifth_nf_tables, flag_5nf = normalization_procedures.validate_fifth_nf(
        fourth_nf_tables, primary_key, dependencies)

    if flag_1nf and flag_2nf and flag_3nf and flag_bcnf and flag_4nf and flag_5nf:
        high_nf = 'Highest Normal Form is: 5NF'

    if target_normal_form == 5:
        if flag_5nf and flag_4nf and flag_bcnf and flag_3nf and flag_2nf and flag_1nf:
            print('Already Normalized to 5NF')
            print('\n')

        print('Queries after decomposing to 5NF')
        print('\n')
        generate_bcnf_4nf_5nf(fifth_nf_tables)

if find_high_nf == 1:
    print('\n')
    print(high_nf)
    print('\n')
