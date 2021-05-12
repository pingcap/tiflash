# -*- coding:utf-8 -*-

import sys
from string import Template
import random
import string
import re
import copy
import os
import errno


drop_stmt = Template("mysql> drop table if exists $database.$table\n")
create_stmt = Template("mysql> create table $database.$table($schema)\n")
alter_stmt = Template("mysql> alter table $database.$table set tiflash replica 1 location labels 'rack', 'host', 'abc'\n")
insert_stmt = Template("mysql> insert into $database.$table($columns) values($data)\n")
update_stmt = Template("mysql> update $database.$table set $exprs $condition\n")
delete_stmt = Template("mysql> delete from $database.$table $condition\n")
select_stmt = Template(">> select $columns from $database.$table\n")
tidb_select_stmt = Template("mysql> set SESSION tidb_isolation_read_engines = 'tiflash' ;select $columns from $database.$table ttt\n")
sleep_string = "\nSLEEP 15\n\n"
wait_table_stmt = Template("\nfunc> wait_table $database $table\n\n")


INSERT = "insert"
UPDATE = "update"
DELETE = "delete"
SELECT = "select"


def generate_column_name(types):
    name_prefix = "a"
    names = []
    for i, _ in enumerate(types):
        names.append(name_prefix + str(i))
    return names


def generate_schema(names, types, primary_key_type):
    columns = []
    primary_key_name = ""
    for (name, t) in zip(names, types):
        if t == primary_key_type:
            primary_key_name = name
        columns.append(name + " " + t)
    columns.append("primary key (" + primary_key_name + ")")
    return ", ".join(columns)


def random_string(n):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(int(n)))


def generate_data(type_name, types, sample_data):
    if type_name.startswith("varchar"):
        lengths = re.findall(r"\d+", type_name)
        if len(lengths) < 1:
            return ""
        else:
            return random_string(lengths[0])
    elif "int" in type_name:
        return str(random.randint(0, 100))
    else:
        return str(sample_data[random.choice(range(len(sample_data)))][types.index(type_name)])


def generate_exprs(names, values):
    exprs = []
    for (name, value) in zip(names, values):
        exprs.append(name + "=" + value)
    return ", ".join(exprs)


def generate_result(names, dataset):
    dataset = copy.deepcopy(dataset)
    for data_point in dataset:
        for i in range(len(data_point)):
            if data_point[i] == "null":
                data_point[i] = "\N"
    left_top_corner = "┌"
    right_top_corner = "┐"
    left_bottom_corner = "└"
    right_bottom_corner = "┘"
    header_split = "┬"
    footer_split = "┴"
    border = "─"
    body_border = "│"
    blank = " "

    cell_length = []
    for name in names:
        cell_length.append(len(name))
    for data in dataset:
        for i, ele in enumerate(data):
            if len(ele) > cell_length[i]:
                cell_length[i] = len(ele)

    lines = []

    header = []
    for i, name in enumerate(names):
        header_cell = ""
        if i == 0:
            header_cell += left_top_corner
        header_cell += border
        header_cell += name
        j = 0
        while cell_length[i] > len(name) + j:
            header_cell += border
            j += 1
        header_cell += border
        if i == len(names) - 1:
            header_cell += right_top_corner

        header.append(header_cell)

    lines.append(header_split.join(header))

    for data in dataset:
        cur = []
        for i, ele in enumerate(data):
            body_cell = ""
            if i == 0:
                body_cell += body_border
            body_cell += blank
            body_cell += ele
            j = 0
            while cell_length[i] > len(ele) + j:
                body_cell += blank
                j += 1
            body_cell += blank
            if i == len(data) - 1:
                body_cell += body_border
            cur.append(body_cell)

        lines.append(body_border.join(cur))

    footer = []
    for i, _ in enumerate(names):
        footer_cell = ""
        if i == 0:
            footer_cell += left_bottom_corner
        footer_cell += border
        j = 0
        while cell_length[i] > j:
            footer_cell += border
            j += 1
        footer_cell += border
        if i == len(names) - 1:
            footer_cell += right_bottom_corner

        footer.append(footer_cell)

    lines.append(footer_split.join(footer))

    return "\n".join(lines)

def tidb_generate_result(names, dataset):
    dataset = copy.deepcopy(dataset)
    for data_point in dataset:
        for i in range(len(data_point)):
            if data_point[i] == "null":
                data_point[i] = "NULL"
    left_top_corner = "+"
    right_top_corner = "+"
    left_bottom_corner = "+"
    right_bottom_corner = "+"
    header_split = "+"
    footer_split = "+"
    border = "-"
    body_border = "|"
    blank = " "

    cell_length = []
    for name in names:
        cell_length.append(len(name))
    for data in dataset:
        for i, ele in enumerate(data):
            if len(ele) > cell_length[i]:
                cell_length[i] = len(ele)

    lines = []

    topline = []
    for i, name in enumerate(names):
        topline_cell = ""
        if i == 0:
            topline_cell += left_top_corner
        topline_cell += border
        j = 0
        while cell_length[i] > j:
            topline_cell += border
            j += 1
        topline_cell += border
        if i == len(names) - 1:
            topline_cell += right_top_corner

        topline.append(topline_cell)

    lines.append(header_split.join(topline))

    header = []
    for i, name in enumerate(names):
        header_cell = ""
        if i == 0:
            header_cell += body_border
        header_cell += blank
        header_cell += name
        j = 0
        while cell_length[i] >=  len(name) + j:
            header_cell += blank
            j += 1
        if i == len(names) - 1:
            header_cell += body_border

        header.append(header_cell)

    lines.append(body_border.join(header))

    lines.append(header_split.join(topline))

    for data in dataset:
        cur = []
        for i, ele in enumerate(data):
            body_cell = ""
            if i == 0:
                body_cell += body_border
            body_cell += blank
            body_cell += ele
            j = 0
            while cell_length[i] > len(ele) + j:
                body_cell += blank
                j += 1
            body_cell += blank
            if i == len(data) - 1:
                body_cell += body_border
            cur.append(body_cell)

        lines.append(body_border.join(cur))

    footer = []
    for i, _ in enumerate(names):
        footer_cell = ""
        if i == 0:
            footer_cell += left_bottom_corner
        footer_cell += border
        j = 0
        while cell_length[i] > j:
            footer_cell += border
            j += 1
        footer_cell += border
        if i == len(names) - 1:
            footer_cell += right_bottom_corner

        footer.append(footer_cell)

    lines.append(footer_split.join(footer))

    return "\n".join(lines)

def generate_cases_inner(database, table, column_names, types, sample_data,
                         schema, primary_key_type, test_cases,  parent_dir):
    primary_key = column_names[len(column_names) - 1]
    for num, case in enumerate(test_cases):
        case_data = copy.deepcopy(sample_data)
        path = parent_dir + primary_key_type.replace(" ", "_") + "_case" + str(num) + ".test"
        with open(path, "w") as file:
            file.write(drop_stmt.substitute({"database": database, "table": table}))
            file.write(create_stmt.substitute({"database": database, "table": table, "schema": schema}))
            file.write(alter_stmt.substitute({"database": database, "table": table}))
            file.write(wait_table_stmt.substitute({"database": database, "table": table}))

            for op in case:
                if op == INSERT:
                    for k in range(len(case_data)):
                        file.write(insert_stmt.substitute({"database": database,
                                                           "table": table,
                                                           "columns": ", ".join(column_names),
                                                           "data": ", ".join([repr(d) if d != "null" else d for d in case_data[k]])}))
                if op == UPDATE:
                    for data_point in case_data:
                        condition = ""
                        exprs = []
                        for i in range(len(types)):
                            if column_names[i] == primary_key:
                                condition = "where " + primary_key + " = " + repr(data_point[i])
                                continue
                            ele = generate_data(types[i], types, sample_data)
                            data_point[i] = ele
                            value = repr(data_point[i])
                            if data_point[i] == "null":
                                value = data_point[i]
                            exprs.append(column_names[i] + "=" + value)
                        file.write(update_stmt.substitute({"database": database,
                                                           "table": table,
                                                           "exprs": ", ".join(exprs),
                                                           "condition": condition}))
                if op == DELETE:
                    new_case_data = random.sample(case_data, len(case_data) // 2)
                    for data_point in case_data:
                        if data_point in new_case_data:
                            continue
                        condition = ""
                        for i in range(len(types)):
                            if column_names[i] == primary_key:
                                condition = "where " + primary_key + " = " + repr(data_point[i])
                                break
                        file.write(delete_stmt.substitute({"database": database,
                                                           "table": table,
                                                           "condition": condition}))
                    case_data = new_case_data
                if op == SELECT:
                    # file.write(select_stmt.substitute({"columns": ", ".join(column_names),
                    #                                    "database": database,
                    #                                    "table": table}))
                    # file.write(generate_result(column_names, case_data) + "\n\n")
                    file.write(tidb_select_stmt.substitute({"columns": ", ".join(column_names),
                                                       "database": database,
                                                       "table": table}))
                    file.write(tidb_generate_result(column_names, case_data) + "\n\n")

            file.write(drop_stmt.substitute({"database": database, "table": table}))


def generate_cases(database, table, types, sample_data,
                   primary_key_candidates, primary_key_sample_data, test_cases,  parent_dir):
    for i, primary_key_type in enumerate(primary_key_candidates):
        case_types = copy.deepcopy(types)
        case_types.append(primary_key_type)
        column_names = generate_column_name(case_types)
        schema = generate_schema(column_names, case_types, primary_key_type)
        case_sample_data = copy.deepcopy(sample_data)
        for j in range(len(case_sample_data)):
            case_sample_data[j].append(primary_key_sample_data[j][i])
        generate_cases_inner(database, table, column_names, case_types, case_sample_data,
                             schema, primary_key_type, test_cases, parent_dir)


def generate_data_for_types(types, sample_data, allow_empty=True, no_duplicate=False, result_len=1):
    result = []
    if no_duplicate:
        for name in types:
            if len(sample_data[name]) < result_len:
                raise Exception("not enough data sample for type: ", name)
        for i in range(result_len):
            cur = []
            for name in types:
                cur.append(str(sample_data[name][i]))
            result.append(cur)
    else:
        for _ in range(result_len):
            cur = []
            for name in types:
                if name in sample_data:
                    samples = sample_data[name]
                    cur.append(str(random.choice(samples)))
                elif allow_empty:
                    cur.append("null")
                else:
                    raise Exception("type without valid data_sample: ", name)
            result.append(cur)
    return result


def run():
    if len(sys.argv) != 3:
        print 'usage: <bin> database table'
        sys.exit(1)

    database = sys.argv[1]
    table = sys.argv[2]

    primary_key_candidates = ["tinyint", "smallint", "mediumint", "int", "bigint",
             "tinyint unsigned", "smallint unsigned", "mediumint unsigned", "int unsigned", "bigint unsigned", ]
    types = ["decimal(1, 0)", "decimal(5, 2)", "decimal(65, 0)",
             "varchar(20)", "char(10)",
             "date", "datetime", "timestamp", ]
    min_values = {
        "tinyint": [-(1 << 7), ],
        "smallint": [-(1 << 15), ],
        "mediumint": [-(1 << 23), ],
        "int": [-(1 << 31), ],
        "bigint": [-(1 << 63), ],
        "tinyint unsigned": [0, ],
        "smallint unsigned": [0, ],
        "mediumint unsigned": [0, ],
        "int unsigned": [0, ],
        "bigint unsigned": [0, ],
        "decimal(1, 0)": [-9, ],
        "decimal(5, 2)": [-999.99, ],
        "decimal(65, 0)": [-(pow(10, 65) - 1), ],
        "decimal(65, 30)":[-99999999999999999999999999999999999.999999999999999999999999999999, ],
    }
    max_values = {
        "tinyint": [(1 << 7) - 1, ],
        "smallint": [(1 << 15) - 1, ],
        "mediumint": [(1 << 23) - 1, ],
        "int": [(1 << 31) - 1, ],
        "bigint": [(1 << 63) - 1, ],
        "tinyint unsigned": [(1 << 8) - 1, ],
        "smallint unsigned": [(1 << 16) - 1, ],
        "mediumint unsigned": [(1 << 24) - 1, ],
        "int unsigned": [(1 << 32) - 1, ],
        "bigint unsigned": [(1 << 64) - 1, ],
        "decimal(1, 0)": [9, ],
        "decimal(5, 2)": [999.99, ],
        "decimal(65, 0)": [pow(10, 65) - 1, ],
        "decimal(65, 30)": [99999999999999999999999999999999999.999999999999999999999999999999, ],
    }
    data_sample = {
        "tinyint": [8, 9, 10, 11, 12, 13, 14],
        "smallint": [8, 9, 10, 11, 12, 13, 14],
        "mediumint": [8, 9, 10, 11, 12, 13, 14],
        "int": [8, 9, 10, 11, 12, 13, 14],
        "bigint": [8, 9, 10, 11, 12, 13, 14],
        "tinyint unsigned": [8, 9, 10, 11, 12, 13, 14],
        "smallint unsigned": [8, 9, 10, 11, 12, 13, 14],
        "mediumint unsigned": [8, 9, 10, 11, 12, 13, 14],
        "int unsigned": [8, 9, 10, 11, 12, 13, 14],
        "bigint unsigned": [8, 9, 10, 11, 12, 13, 14],
        "decimal(1, 0)": [7, 3],
        "decimal(5, 2)": [3.45, 5.71],
        "decimal(65, 0)": [11, ],
        "decimal(65, 30)": [11, ],
        "varchar(20)": ["hello world", "hello world2", "hello world3", "hello world4", ],
        "char(10)": ["a" * 10, "b" * 10, ],
        "date": ["2000-01-01", "2019-10-10"],
        "datetime": ["2000-01-01 00:00:00", "2019-10-10 00:00:00"],
        "timestamp": ["2000-01-01 00:00:00", "2019-10-10 00:00:00"],
    }

    data_sample_num = 7
    primary_key_data = []
    for d in generate_data_for_types(primary_key_candidates, min_values, False, True, 1):
        primary_key_data.append(d)
    for d in generate_data_for_types(primary_key_candidates, max_values, False, True, 1):
        primary_key_data.append(d)
    for d in generate_data_for_types(primary_key_candidates, data_sample, False, True, data_sample_num - 2):
        primary_key_data.append(d)
    data = []
    for d in generate_data_for_types(types, min_values, True, False, 1):
        data.append(d)
    for d in generate_data_for_types(types, max_values, True, False, 1):
        data.append(d)
    for d in generate_data_for_types(types, data_sample, True, False, data_sample_num - 2):
        data.append(d)

    dml_test_cases = [
        [INSERT, SELECT, UPDATE, SELECT, ],
        [INSERT, SELECT, UPDATE, SELECT, DELETE, SELECT],
        [INSERT, SELECT, UPDATE, SELECT, UPDATE, SELECT, DELETE, SELECT],
        [INSERT, SELECT, UPDATE, SELECT, UPDATE, SELECT, UPDATE, SELECT, DELETE, SELECT],
    ]
    parent_dir = "./fullstack-test2/dml/dml_gen/"
    directory = os.path.dirname(parent_dir)
    try:
        os.makedirs(directory)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
    generate_cases(database, table, types, data, primary_key_candidates, primary_key_data, dml_test_cases, parent_dir)


def main():
    try:
        run()
    except KeyboardInterrupt:
        print 'KeyboardInterrupted'
        sys.exit(1)


main()
