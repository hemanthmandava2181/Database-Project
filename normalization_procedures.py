# This file is used for all the normalizations
import pandas as pd
from itertools import combinations
import re

def is_list_or_set(item):
    return isinstance(item, (list, set))


def is_superkey(relation, left_hand_side):
    grouped = relation.groupby(
        list(left_hand_side)).size().reset_index(name='count')
    return not any(grouped['count'] > 1)


def powerset(s):
    x = len(s)
    for i in range(1 << x):
        yield [s[j] for j in range(x) if (i & (1 << j)) > 0]


def bcnf_decomposition(relation, dependencies):
    for left_hand_side, right_hand_sides in dependencies.items():
        if set(left_hand_side).issubset(relation.columns) and not is_superkey(relation, left_hand_side):
            right_hand_side_cols = list(left_hand_side) + right_hand_sides
            new_relation1 = relation[right_hand_side_cols].drop_duplicates()
            remaining_cols = list(set(relation.columns) - set(right_hand_sides))
            new_relation2 = relation[remaining_cols].drop_duplicates()
            return [new_relation1, new_relation2]
    return [relation]


def check_1nf(relation):
    if relation.empty:
        return False

    for column in relation.columns:
        unique_types = relation[column].apply(type).nunique()
        if unique_types > 1:
            return False
        if relation[column].apply(lambda x: isinstance(x, (list, dict, set))).any():
            return False

    return True


def check_2nf(primary_key, dependencies):
    partial_dependencies_not_found = True
    for left_hand_side, right_hand_side in dependencies.items():
        if set(left_hand_side).issubset(primary_key) and set(left_hand_side) != set(primary_key):
            partial_dependencies_not_found = False
            break

    return partial_dependencies_not_found


def check_3nf(relations, dependencies):
    for relation in relations:
        attributes = set(relations[relation].columns)
        non_prime_attributes = attributes - set(relation)
        for left_hand_side, right_hand_sides in dependencies.items():
            if all(attr in non_prime_attributes for attr in left_hand_side):
                for right_hand_side in right_hand_sides:
                    if right_hand_side in non_prime_attributes:
                        return False
    return True


def check_bcnf(relations, primary_key, dependencies):
    for relation in relations:
        for left_hand_side, right_hand_sides in dependencies.items():
            if set(left_hand_side).issubset(relation.columns):
                if not is_superkey(relation, left_hand_side):
                    return False
    return True


def check_4nf(relations, mvd_dependencies):
    for relation in relations:
        for left_hand_side, right_hand_sides in mvd_dependencies.items():
            for right_hand_side in right_hand_sides:
                if isinstance(left_hand_side, tuple):
                    left_hand_side_cols = list(left_hand_side)
                else:
                    left_hand_side_cols = [left_hand_side]

                if all(col in relation.columns for col in left_hand_side_cols + [right_hand_side]):
                    grouped = relation.groupby(left_hand_side_cols)[
                        right_hand_side].apply(set).reset_index()
                    if len(grouped) < len(relation):
                        print(
                            f"Multi-valued dependency violation: {left_hand_side} ->-> {right_hand_side}")
                        return False
    return True


def check_5nf(relations):
    i = 0
    candidate_keys_dict = {}
    for relation in relations:
        print(relation)
        user_input = input("Enter the candidate keys:")
        print('\n')
        tuples = re.findall(r'\((.*?)\)', user_input)
        candidate_keys = [tuple(map(str.strip, t.split(','))) for t in tuples]
        candidate_keys_dict[i] = candidate_keys
        i += 1

    print(f'Candidate Keys for tables:')
    print(candidate_keys_dict)
    print('\n')

    j = 0
    for relation in relations:
        candidate_keys = candidate_keys_dict[j]
        j += 1

        data_tuples = [tuple(row) for row in relation.to_numpy()]

        def project(data, attributes):
            return {tuple(row[attr] for attr in attributes) for row in data}

        # Function to check if a set of attributes is a superkey
        def is_superkey(attributes):
            for key in candidate_keys:
                if set(key).issubset(attributes):
                    return True
            return False, candidate_keys_dict

        for i in range(1, len(relation.columns)):
            for attrs in combinations(relation.columns, i):
                if is_superkey(attrs):
                    continue

                projected_data = project(data_tuples, attrs)
                complement_attrs = set(relation.columns) - set(attrs)
                complement_data = project(data_tuples, complement_attrs)

                joined_data = {(row1 + row2)
                               for row1 in projected_data for row2 in complement_data}
                if set(data_tuples) != joined_data:
                    print("Failed 5NF check for attributes:", attrs)
                    return False, candidate_keys_dict

    return True, candidate_keys_dict


def validate_first_nf(relation):
    flag_1nf = check_1nf(relation)

    if flag_1nf:
        return relation, flag_1nf
    else:
        for col in relation.columns:
            if relation[col].apply(is_list_or_set).any():
                relation = relation.explode(col)

        print('Tables after 1NF decomposition:')
        print(relation)
        print('\n')
        return relation, flag_1nf


def validate_second_nf(relation, primary_key, dependencies):
    relations = {}
    original_relation = relation
    flag_2nf = check_2nf(primary_key, dependencies)

    if flag_2nf:
        relations[primary_key] = relation
        return relations, flag_2nf
    else:
        print('Tables after 2NF decomposition:')
        for left_hand_side, right_hand_side in dependencies.items():
            cols = list(left_hand_side) + right_hand_side
            relations[tuple(left_hand_side)] = relation[cols].drop_duplicates(
            ).reset_index(drop=True)
            print(relations[left_hand_side])
            print('\n')

        junction_cols = []
        relation_name = ''
        for relation in relations:
            if set(relation).issubset(primary_key):
                relation_name += "_".join(relation)
                junction_cols.append(relation)

        if len(junction_cols) > 1:
            jun_cols = list(junction_cols)
            cols = [element for tup in jun_cols for element in tup]
            temp_df = original_relation[cols].drop_duplicates(
            ).reset_index(drop=True)

            renamed_cols = [col + '_fk' for col in cols]
            temp_df.columns = renamed_cols + \
                [col for col in temp_df.columns if col not in cols]

            temp_df[relation_name] = range(1, len(temp_df) + 1)
            columns_order = [relation_name] + renamed_cols
            temp_df = temp_df[columns_order]
            relations[relation_name] = temp_df
            print(relations[relation_name])
            print('\n')

        return relations, flag_2nf


def validate_third_nf(relations, primary_key, dependencies):
    three_relations = {}
    flag_3nf = check_3nf(relations, dependencies)

    if flag_3nf:
        return relations, flag_3nf
    else:
        print('Tables after 3NF decomposition:')
        for relation in relations:
            original_relation = relations[relation]
            for left_hand_side, right_hand_side in dependencies.items():
                cols = list(left_hand_side) + right_hand_side
                three_relations[tuple(left_hand_side)] = relations[relation][cols].drop_duplicates(
                ).reset_index(drop=True)
                print(three_relations[left_hand_side])
                print('\n')

        junction_cols = []
        relation_name = ''
        for relation in three_relations:
            relation_name += "_".join(relation)
            junction_cols.append(relation)

        print(relation_name)

        if len(junction_cols) > 1:
            jun_cols = list(junction_cols)
            cols = [element for tup in jun_cols for element in tup]
            temp_df = original_relation[cols].drop_duplicates(
            ).reset_index(drop=True)

            renamed_cols = [col + '_fk' for col in cols]
            temp_df.columns = renamed_cols + \
                [col for col in temp_df.columns if col not in cols]

            temp_df[relation_name] = range(1, len(temp_df) + 1)
            columns_order = [relation_name] + renamed_cols
            temp_df = temp_df[columns_order]
            three_relations[relation_name] = temp_df
            print(three_relations[relation_name])
            print('\n')

        return three_relations, flag_3nf


def validate_bc_nf(relations, primary_key, dependencies):
    relations = list(relations.values())
    bcnf_relations = []
    flag_bcnf = check_bcnf(relations, primary_key, dependencies)

    if flag_bcnf:
        return relations, flag_bcnf
    else:
        print('Tables after BCNF decomposition:')
        for relation in relations:
            bcnf_decomposed_relation = bcnf_decomposition(
                relation, dependencies)
            if len(bcnf_decomposed_relation) == 1:
                bcnf_relations.append(bcnf_decomposed_relation)
            else:
                relations.extend(bcnf_decomposed_relation)

    return bcnf_relations, flag_bcnf


def validate_fourth_nf(relations, mvd_dependencies):
    four_relations = []
    flag_4nf = check_4nf(relations, mvd_dependencies)

    if flag_4nf:
        return relations, flag_4nf
    else:
        print('Tables after 4NF decomposition:')
        for relation in relations:
            for left_hand_side, right_hand_sides in mvd_dependencies.items():
                for right_hand_side in right_hand_sides:
                    if isinstance(left_hand_side, tuple):
                        left_hand_side_cols = list(left_hand_side)
                    else:
                        left_hand_side_cols = [left_hand_side]

                    if all(col in relation.columns for col in left_hand_side_cols + [right_hand_side]):
                        # Check for multi-valued dependency
                        grouped = relation.groupby(left_hand_side_cols)[
                            right_hand_side].apply(set).reset_index()
                        if len(grouped) < len(relation):
                            table_1 = relation[left_hand_side_cols +
                                               [right_hand_side]].drop_duplicates()
                            table_2 = relation[left_hand_side_cols + [col for col in relation.columns if col not in [
                                right_hand_side] + left_hand_side_cols]].drop_duplicates()

                            four_relations.extend([table_1, table_2])

                            break
                else:
                    continue
                break
            else:
                four_relations.append(relation)

    if len(four_relations) == len(relations):
        return four_relations 
    else:
        return validate_fourth_nf(four_relations, mvd_dependencies)


def decompose_5nf(dataframe, candidate_keys):
    def project(df, attributes):
        return df[list(attributes)].drop_duplicates().reset_index(drop=True)

    # Function to check if a decomposition is lossless
    def is_lossless(df, df1, df2):
        common_columns = set(df1.columns) & set(df2.columns)
        if not common_columns:
            return False
        joined_df = pd.merge(df1, df2, how='inner', on=list(common_columns))
        return df.equals(joined_df)

    decomposed_tables = [dataframe]

    for key in candidate_keys:
        new_tables = []
        for table in decomposed_tables:
            if set(key).issubset(set(table.columns)):
                table1 = project(table, key)
                remaining_columns = set(table.columns) - set(key)
                table2 = project(table, remaining_columns | set(key))
                
                if is_lossless(table, table1, table2):
                    new_tables.extend([table1, table2])
                else:
                    new_tables.append(table)
            else:
                new_tables.append(table)
        decomposed_tables = new_tables

    return decomposed_tables


def validate_fifth_nf(relations, primary_key, dependencies):
    five_relations = []
    flag_5nf, candidate_keys_dict = check_5nf(relations)

    if flag_5nf:
        return relations, flag_5nf
    else:
        print('Tables after 5NF decomposition:')
        i = 0
        for relation in relations:
            candidate_keys = candidate_keys_dict[i]
            i += 1
            decomposed_relations = decompose_5nf(relation, candidate_keys)
            five_relations.append(decomposed_relations)

    return five_relations, flag_5nf
