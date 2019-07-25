# -*- coding:utf-8 -*-

import sys
from string import Template
import random
import string
import re
import copy

types = ["varchar(16)", "int", ]
sample_data = [
    ["hello world", "23892384", ],
    ["hello world2", "1000000000", ],
    ["hello world3", "100000", ],
    ["hello world4", "348912734", ],
]

extra = ["default null", "not null", ""]

drop_stmt = Template("t> drop table if exists $database.$table\n")
create_stmt = Template("t> create table $database.$table($schema)\n")
insert_stmt = Template("t> insert into $database.$table($columns) values($data)\n")
update_stmt = Template("t> update $database.$table set $exprs $condition\n")
delete_stmt = Template("t> delete from $database.$table $condition\n")
select_stmt = Template(">> select $columns from $database.$table\n")
sleep_string = "\nSLEEP 1\n\n"


INSERT = "insert"
UPDATE = "update"
DELETE = "delete"
SELECT = "select"

TEST_CASES = [
    [INSERT, SELECT, UPDATE, SELECT, DELETE, SELECT],
]


def generate_column_name(types):
    name_prefix = "a"
    names = []
    for i, _ in enumerate(types):
        names.append(name_prefix + str(i))
    return names


def generate_schema(names, types):
    columns = []
    for (name, t) in zip(names, types):
        columns.append(name + " " + t)
    columns.append("primary key (" + names[0] + ")")
    return ", ".join(columns), names[0]


def random_string(n):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(int(n)))


def generate_data(type_name):
    if type_name.startswith("varchar"):
        lengths = re.findall(r"\d+", type_name)
        if len(lengths) < 1:
            return ""
        else:
            return random_string(lengths[0])
    elif "int" in type_name:
        return str(random.randint(0, 100))


def generate_exprs(names, values):
    exprs = []
    for (name, value) in zip(names, values):
        exprs.append(name + "=" + value)
    return ", ".join(exprs)


def generate_result(names, dataset):
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


def generate_cases(database, table, types, sample_data, dir):
    column_names = generate_column_name(types)
    schema, primary_key = generate_schema(column_names, types)
    for i, case in enumerate(TEST_CASES):
        case_data = copy.deepcopy(sample_data)
        path = dir + "case" + str(i) + ".test"
        with open(path, "w") as file:
            file.write(drop_stmt.substitute({"database": database, "table": table}))
            file.write(create_stmt.substitute({"database": database, "table": table, "schema": schema}))

            for op in case:
                if op == INSERT:
                    for k in range(len(case_data)):
                        file.write(insert_stmt.substitute({"database": database,
                                                           "table": table,
                                                           "columns": ", ".join(column_names),
                                                           "data": ", ".join([repr(d) for d in case_data[k]])}))
                if op == UPDATE:
                    for data_point in case_data:
                        condition = ""
                        exprs = []
                        for i in range(len(types)):
                            ele = generate_data(types[i])
                            data_point[i] = ele
                            exprs.append(column_names[i] + "=" + repr(ele))
                            if column_names[i] == primary_key:
                                condition = "where " + primary_key + " = " + repr(ele)
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
                    file.write(sleep_string)
                    file.write(select_stmt.substitute({"columns": ", ".join(column_names),
                                                       "database": database,
                                                       "table": table}))
                    file.write(generate_result(column_names, case_data) + "\n\n")

            file.write(drop_stmt.substitute({"database": database, "table": table}))


def run():
    if len(sys.argv) != 3:
        print 'usage: <bin> database table'
        sys.exit(1)

    database = sys.argv[1]
    table = sys.argv[2]

    generate_cases(database, table, types, sample_data, "./fullstack-test/txn_syntax/")


def main():
    try:
        run()
    except KeyboardInterrupt:
        print 'KeyboardInterrupted'
        sys.exit(1)


main()
