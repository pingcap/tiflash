# Copyright 2023 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import errno
import os
import sys


class ColumnType(object):
    typeTinyInt = "tinyint"
    typeUTinyInt = "tinyint unsigned"
    typeSmallInt = "smallint"
    typeUSmallInt = "smallint unsigned"
    typeMediumInt = "mediumint"
    typeUMediumInt = "mediumint unsigned"
    typeInt = "int"
    typeUInt = "int unsigned"
    typeBigInt = "bigint"
    typeUBigInt = "bigint unsigned"
    typeBit64 = "bit(64)"
    typeBoolean = "boolean"
    typeFloat = "float"
    typeDouble = "double"
    typeDecimal1 = "decimal(1, 0)"
    typeDecimal2 = "decimal(65, 2)"
    typeChar = "char(200)"
    typeVarchar = "varchar(200)"


class ColumnTypeManager(object):
    # the index must be continuous below
    MinValueIndex = 0
    MaxValueIndex = 1
    NormalValueIndex = 2
    # when the column is not null and doesn't have default value
    EmptyValueIndex = 3

    def __init__(self):
        self.column_value_map = {}
        self.funcs_process_value_for_insert_stmt = {}
        self.funcs_get_select_expr = {}
        self.funcs_format_result = {}

    def register_type(self, column_type, values, process_value_func=None, get_select_expr=None, format_result=None):
        self.column_value_map[column_type] = values
        if process_value_func is not None:
            self.funcs_process_value_for_insert_stmt[column_type] = process_value_func
        if get_select_expr is not None:
            self.funcs_get_select_expr[column_type] = get_select_expr
        if format_result is not None:
            self.funcs_format_result[column_type] = format_result

    def get_all_column_elements(self):
        column_elements = []
        for i, t in enumerate(self.column_value_map):
            default_value = self.get_normal_value(t)
            column_elements.append(ColumnElement("a" + str(2 * i), t, False, False, default_value))
            column_elements.append(ColumnElement("a" + str(2 * i + 1), t, True, False, default_value))
        return column_elements

    def get_all_primary_key_elements(self):
        column_elements = []
        for i, t in enumerate(self.column_value_map):
            column_elements.append(ColumnElement("a" + str(2 * i), t, False, True))
        return column_elements

    def get_all_nullable_column_elements(self):
        column_elements = []
        for i, t in enumerate(self.column_value_map):
            column_elements.append(ColumnElement("a" + str(2 * i), t, True, False))
        return column_elements

    def get_min_value(self, column_type):
        return self.column_value_map[column_type][ColumnTypeManager.MinValueIndex]

    def get_max_value(self, column_type):
        return self.column_value_map[column_type][ColumnTypeManager.MaxValueIndex]

    def get_normal_value(self, column_type):
        return self.column_value_map[column_type][ColumnTypeManager.NormalValueIndex]

    def get_empty_value(self, column_type):
        return self.column_value_map[column_type][ColumnTypeManager.EmptyValueIndex]

    def get_value_for_dml_stmt(self, column_type, value):
        if column_type in self.funcs_process_value_for_insert_stmt \
                and self.funcs_process_value_for_insert_stmt[column_type] is not None:
            return self.funcs_process_value_for_insert_stmt[column_type](value)
        else:
            return str(value)

    # some type's outputs are hard to parse and compare, try convert it's result to string
    def get_select_expr(self, column_type, name):
        if column_type in self.funcs_get_select_expr \
                and self.funcs_get_select_expr[column_type] is not None:
            return self.funcs_get_select_expr[column_type](name)
        else:
            return name

    def format_result_value(self, column_type, value):
        if str(value).lower() == "null":
            return value
        if column_type in self.funcs_format_result \
                and self.funcs_format_result[column_type] is not None:
            return self.funcs_format_result[column_type](value)
        else:
            return value


def register_all_types(column_type_manager):
    # column_type_manager.register_type(ColumnType, values)
    # the first three value in values should be different for some primary key tests
    column_type_manager.register_type(ColumnType.typeTinyInt, [-128, 127, 100, 0])
    column_type_manager.register_type(ColumnType.typeUTinyInt, [0, 255, 68, 0])
    column_type_manager.register_type(ColumnType.typeSmallInt, [-32768, 32767, 11100, 0])
    column_type_manager.register_type(ColumnType.typeUSmallInt, [0, 65535, 68, 0])
    column_type_manager.register_type(ColumnType.typeMediumInt, [-8388608, 8388607, 68, 0])
    column_type_manager.register_type(ColumnType.typeUMediumInt, [0, 16777215, 68, 0])
    column_type_manager.register_type(ColumnType.typeInt, [-2147483648, 2147483647, 68, 0])
    column_type_manager.register_type(ColumnType.typeUInt, [0, 4294967295, 4239013, 0])
    column_type_manager.register_type(ColumnType.typeBigInt, [-9223372036854775808, 9223372036854775807, 68, 0])
    column_type_manager.register_type(ColumnType.typeUBigInt, [0, 18446744073709551615, 68, 0])
    column_type_manager.register_type(
        ColumnType.typeBit64,
        [0, (1 << 64) - 1, 79, 0],
        None,
        lambda name: "bin({})".format(name),
        lambda value: "{0:b}".format(int(value)))
    column_type_manager.register_type(ColumnType.typeBoolean, [0, 1, 1, 0])
    column_type_manager.register_type(ColumnType.typeFloat, ["-3.402e38", "3.402e38", "-1.17e-38", 0])
    column_type_manager.register_type(ColumnType.typeDouble, ["-1.797e308", "1.797e308", "2.225e-308", 0])
    column_type_manager.register_type(ColumnType.typeDecimal1, [-9, 9, 3, 0])
    column_type_manager.register_type(
        ColumnType.typeDecimal2,
        ['-' + '9' * 63 + '.' + '9' * 2, '9' * 63 + '.' + '9' * 2, 100.23, "0.00"])
    column_type_manager.register_type(
        ColumnType.typeChar,
        ["", "a" * 200, "test", ""],
        lambda value: "'" + value + "'")
    column_type_manager.register_type(
        ColumnType.typeVarchar,
        ["", "a" * 200, "tiflash", ""],
        lambda value: "'" + value + "'")


class ColumnElement(object):
    def __init__(self, name, column_type, nullable=False, is_primary_key=False, default_value=None):
        self.name = name
        self.column_type = column_type
        self.nullable = nullable
        self.default_value = default_value
        self.is_primary_key = is_primary_key

    def get_schema(self, column_type_manager):
        schema = self.name + " " + str(self.column_type)
        if not self.nullable:
            schema += " not null"
        if self.default_value is not None:
            schema += " default {}".format(column_type_manager.get_value_for_dml_stmt(self.column_type, self.default_value))
        return schema


class MysqlClientResultBuilder(object):
    def __init__(self, column_names):
        self.column_names = column_names
        self.rows = []
        # + 2 is for left and right blank
        self.cell_length = [len(name) + 2 for name in self.column_names]
        self.point = "+"
        self.vertical = "|"
        self.horizontal = "-"

    def add_row(self, column_values):
        assert len(column_values) == len(self.column_names)
        for i in range(len(self.cell_length)):
            self.cell_length[i] = max(self.cell_length[i], len(str(column_values[i])) + 2)
        self.rows.append(column_values)

    def finish(self, file):
        self._write_horizontal_line(file)
        self._write_header_content(file)
        self._write_horizontal_line(file)
        self._write_body_content(file)
        self._write_horizontal_line(file)

    def _write_horizontal_line(self, file):
        file.write(self.point)
        for length in self.cell_length:
            file.write(self.horizontal * length)
            file.write(self.point)
        file.write("\n")

    def _write_row(self, file, row_cells):
        file.write(self.vertical)
        for i in range(len(row_cells)):
            content = str(row_cells[i])
            file.write(" ")
            file.write(content)
            file.write(" " * (self.cell_length[i] - 1 - len(content)))
            file.write(self.vertical)
        file.write("\n")

    def _write_header_content(self, file):
        self._write_row(file, self.column_names)

    def _write_body_content(self, file):
        for row in self.rows:
            self._write_row(file, row)


class StmtWriter(object):
    def __init__(self, file, db_name, table_name):
        self.file = file
        self.db_name = db_name
        self.table_name = table_name

    def write_newline(self):
        self.file.write("\n")

    def write_drop_table_stmt(self):
        command = "mysql> drop table if exists {}.{}\n".format(self.db_name, self.table_name)
        self.file.write(command)

    def write_create_table_schema_stmt(self, column_elements, column_type_manager):
        column_schema = ", ".join([c.get_schema(column_type_manager) for c in column_elements])
        primary_key_names = []
        for c in column_elements:
            if c.is_primary_key:
                primary_key_names.append(c.name)
        if len(primary_key_names) > 0:
            column_schema += ", primary key({})".format(", ".join(primary_key_names))

        command = "mysql> create table {}.{}({})\n".format(self.db_name, self.table_name, column_schema)
        self.file.write(command)

    def write_create_tiflash_replica_stmt(self):
        command = "mysql> alter table {}.{} set tiflash replica 1\n".format(self.db_name, self.table_name)
        self.file.write(command)

    def write_wait_table_stmt(self):
        command = "func> wait_table {} {}\n".format(self.db_name, self.table_name)
        self.file.write(command)

    def write_insert_stmt(self, column_names, column_values):
        command = "mysql> insert into {}.{} ({}) values({})\n".format(
            self.db_name, self.table_name, ", ".join(column_names), ", ".join(column_values))
        self.file.write(command)

    def write_update_stmt(self, column_names, prev_values, after_values):
        assert len(column_names) == len(prev_values) == len(after_values)
        update_part = ""
        filter_part = ""
        for i in range(len(column_names)):
            update_part += "{}={}".format(column_names[i], after_values[i])
            filter_part += "{}={}".format(column_names[i], prev_values[i])

        command = "mysql> update {}.{} set {} where {}\n".format(
            self.db_name, self.table_name, update_part, filter_part)
        self.file.write(command)

    def write_delete_stmt(self, column_names, values):
        assert len(column_names) == len(values)
        filter_part = ""
        for i in range(len(column_names)):
            filter_part += "{}={}".format(column_names[i], values[i])

        command = "mysql> delete from {}.{} where {}\n".format(
            self.db_name, self.table_name, filter_part)
        self.file.write(command)

    def write_select_stmt(self, column_select_exprs):
        command = "mysql> set SESSION tidb_isolation_read_engines='tiflash'; select {} from {}.{}\n".format(
            ", ".join(column_select_exprs), self.db_name, self.table_name)
        self.file.write(command)

    def write_result(self, column_select_exprs, *row_column_values):
        result_builder = MysqlClientResultBuilder(column_select_exprs)
        for column_values in row_column_values:
            result_builder.add_row(column_values)
        result_builder.finish(self.file)


class TestCaseWriter(object):
    def __init__(self, column_type_manager):
        self.column_type_manager = column_type_manager

    def _build_dml_values(self, column_elements, column_values):
        return [self.column_type_manager.get_value_for_dml_stmt(
                                     column_elements[i].column_type, column_values[i]) for i in range(len(column_values))]

    def _build_formatted_values(self, column_elements, column_values):
        result = [self.column_type_manager.format_result_value(
                                     column_elements[i].column_type, column_values[i]) for i in range(len(column_values))]
        return result

    def _write_create_table(self, writer, column_elements):
        writer.write_drop_table_stmt()
        writer.write_create_table_schema_stmt(column_elements, self.column_type_manager)
        writer.write_create_tiflash_replica_stmt()
        writer.write_wait_table_stmt()
        writer.write_newline()

    def _write_min_max_value_test(self, writer):
        column_elements = self.column_type_manager.get_all_column_elements()
        self._write_create_table(writer, column_elements)
        # insert values
        column_names = [c.name for c in column_elements]
        column_min_values = [self.column_type_manager.get_min_value(c.column_type) for c in column_elements]
        writer.write_insert_stmt(column_names, self._build_dml_values(column_elements, column_min_values))
        column_max_values = [self.column_type_manager.get_max_value(c.column_type) for c in column_elements]
        writer.write_insert_stmt(column_names, self._build_dml_values(column_elements, column_max_values))
        column_normal_values = [self.column_type_manager.get_normal_value(c.column_type) for c in column_elements]
        writer.write_insert_stmt(column_names, self._build_dml_values(column_elements, column_normal_values))
        # check result
        column_select_exprs = [self.column_type_manager.get_select_expr(c.column_type, c.name) for c in column_elements]
        writer.write_select_stmt(column_select_exprs)
        writer.write_result(
            column_select_exprs,
            self._build_formatted_values(column_elements, column_min_values),
            self._build_formatted_values(column_elements, column_max_values),
            self._build_formatted_values(column_elements, column_normal_values))
        writer.write_newline()

    def _write_default_value_test(self, writer):
        column_elements = self.column_type_manager.get_all_column_elements()
        primary_key_name = "mykey"
        primary_key_value = 10000
        column_default_values = [c.default_value for c in column_elements]
        column_elements.append(ColumnElement(primary_key_name, ColumnType.typeUInt, False, True))
        column_default_values.append(primary_key_value)
        self._write_create_table(writer, column_elements)
        # write a row which only specify key value
        writer.write_insert_stmt([primary_key_name], [str(primary_key_value)])
        # check result
        column_select_exprs = [self.column_type_manager.get_select_expr(c.column_type, c.name) for c in column_elements]
        writer.write_select_stmt(column_select_exprs)
        writer.write_result(
            column_select_exprs,
            self._build_formatted_values(column_elements, column_default_values))

    def _write_null_value_test(self, writer):
        nonnull_column_elements = self.column_type_manager.get_all_nullable_column_elements()
        primary_key_name = "mykey"
        primary_key_value = 10000
        column_null_values = ["NULL" for _ in nonnull_column_elements]
        nonnull_column_elements.append(ColumnElement(primary_key_name, ColumnType.typeUInt, False, True))
        column_null_values.append(str(primary_key_value))
        self._write_create_table(writer, nonnull_column_elements)
        # write a row which only specify key value
        writer.write_insert_stmt([primary_key_name], [str(primary_key_value)])
        # check result
        column_select_exprs = [self.column_type_manager.get_select_expr(c.column_type, c.name) for c in
                               nonnull_column_elements]
        writer.write_select_stmt(column_select_exprs)
        writer.write_result(
            column_select_exprs,
            self._build_formatted_values(nonnull_column_elements, column_null_values))

    def write_basic_type_codec_test(self, file, db_name, table_name):
        writer = StmtWriter(file, db_name, table_name)
        self._write_min_max_value_test(writer)
        self._write_default_value_test(writer)
        self._write_null_value_test(writer)

    def _write_non_cluster_index_test(self, writer):
        column_elements = self.column_type_manager.get_all_column_elements()
        filter_column_name = "myfilter"
        filter_value1 = 10000
        filter_value2 = 20000
        column_names = [c.name for c in column_elements]
        column_values1 = [self.column_type_manager.get_normal_value(c.column_type) for c in column_elements]
        column_values2 = [self.column_type_manager.get_normal_value(c.column_type) for c in column_elements]
        column_elements.append(ColumnElement(filter_column_name, ColumnType.typeUInt))
        column_names.append(filter_column_name)
        column_values1.append(filter_value1)
        column_values2.append(filter_value2)
        self._write_create_table(writer, column_elements)
        writer.write_insert_stmt(column_names, self._build_dml_values(column_elements, column_values1))
        writer.write_insert_stmt(column_names, self._build_dml_values(column_elements, column_values2))
        # check result
        column_select_exprs = [self.column_type_manager.get_select_expr(c.column_type, c.name) for c in
                               column_elements]
        writer.write_select_stmt(column_select_exprs)
        writer.write_result(
            column_select_exprs,
            self._build_formatted_values(column_elements, column_values1),
            self._build_formatted_values(column_elements, column_values2))
        new_filter_value1 = 30000
        writer.write_newline()
        # update
        writer.write_update_stmt([filter_column_name], [filter_value1], [new_filter_value1])
        column_values1[len(column_values1) - 1] = new_filter_value1
        writer.write_select_stmt(column_select_exprs)
        writer.write_result(
            column_select_exprs,
            self._build_formatted_values(column_elements, column_values1),
            self._build_formatted_values(column_elements, column_values2))
        # delete
        writer.write_delete_stmt([filter_column_name], [filter_value2])
        writer.write_select_stmt(column_select_exprs)
        writer.write_result(
            column_select_exprs,
            self._build_formatted_values(column_elements, column_values1))

    def _write_pk_is_handle_test(self, writer, pk_column_element):
        column_elements = self.column_type_manager.get_all_column_elements()
        filter_column_name = "myfilter"
        filter_value1 = 10000
        filter_value2 = 20000
        column_names = [c.name for c in column_elements]
        column_values1 = [self.column_type_manager.get_normal_value(c.column_type) for c in column_elements]
        column_values2 = [self.column_type_manager.get_normal_value(c.column_type) for c in column_elements]
        # add pk column
        column_elements.append(pk_column_element)
        column_names.append(pk_column_element.name)
        pk_values1 = self.column_type_manager.get_normal_value(pk_column_element.column_type)
        pk_values2 = self.column_type_manager.get_normal_value(pk_column_element.column_type) + 1
        column_values1.append(pk_values1)
        column_values2.append(pk_values2)

        # add filter column
        column_elements.append(ColumnElement(filter_column_name, ColumnType.typeUInt))
        column_names.append(filter_column_name)
        column_values1.append(filter_value1)
        column_values2.append(filter_value2)
        self._write_create_table(writer, column_elements)
        writer.write_insert_stmt(column_names, self._build_dml_values(column_elements, column_values1))
        writer.write_insert_stmt(column_names, self._build_dml_values(column_elements, column_values2))
        # check result
        column_select_exprs = [self.column_type_manager.get_select_expr(c.column_type, c.name) for c in
                               column_elements]
        writer.write_select_stmt(column_select_exprs)
        writer.write_result(
            column_select_exprs,
            self._build_formatted_values(column_elements, column_values1),
            self._build_formatted_values(column_elements, column_values2))
        new_filter_value1 = 30000
        writer.write_newline()
        # update
        writer.write_update_stmt([filter_column_name], [filter_value1], [new_filter_value1])
        column_values1[len(column_values1) - 1] = new_filter_value1
        writer.write_select_stmt(column_select_exprs)
        writer.write_result(
            column_select_exprs,
            self._build_formatted_values(column_elements, column_values1),
            self._build_formatted_values(column_elements, column_values2))
        # delete
        writer.write_delete_stmt([filter_column_name], [filter_value2])
        writer.write_select_stmt(column_select_exprs)
        writer.write_result(
            column_select_exprs,
            self._build_formatted_values(column_elements, column_values1))

    def _write_cluster_index_test(self, writer, primary_key_columns):
        column_elements = primary_key_columns
        column_names = [c.name for c in column_elements]
        column_values1 = [self.column_type_manager.get_normal_value(c.column_type) for c in column_elements]
        column_values2 = [self.column_type_manager.get_min_value(c.column_type) for c in column_elements]

        filter_column_name = "myfilter"
        filter_value1 = 10000
        filter_value2 = 20000
        column_elements.append(ColumnElement(filter_column_name, ColumnType.typeInt))
        column_names.append(filter_column_name)
        column_values1.append(filter_value1)
        column_values2.append(filter_value2)

        self._write_create_table(writer, column_elements)
        writer.write_insert_stmt(column_names, self._build_dml_values(column_elements, column_values1))
        writer.write_insert_stmt(column_names, self._build_dml_values(column_elements, column_values2))
        # check result
        column_select_exprs = [self.column_type_manager.get_select_expr(c.column_type, c.name) for c in
                               column_elements]
        writer.write_select_stmt(column_select_exprs)
        writer.write_result(
            column_select_exprs,
            self._build_formatted_values(column_elements, column_values1),
            self._build_formatted_values(column_elements, column_values2))
        new_filter_value1 = 30000
        writer.write_newline()
        # update
        writer.write_update_stmt([filter_column_name], [filter_value1], [new_filter_value1])
        column_values1[len(column_values1) - 1] = new_filter_value1
        writer.write_select_stmt(column_select_exprs)
        writer.write_result(
            column_select_exprs,
            self._build_formatted_values(column_elements, column_values1),
            self._build_formatted_values(column_elements, column_values2))
        # delete
        writer.write_delete_stmt([filter_column_name], [filter_value2])
        writer.write_select_stmt(column_select_exprs)
        writer.write_result(
            column_select_exprs,
            self._build_formatted_values(column_elements, column_values1))

    def write_update_delete_test1(self, file, db_name, table_name):
        writer = StmtWriter(file, db_name, table_name)
        self._write_non_cluster_index_test(writer)
        file.write("# pk_is_handle test\n")
        for column_type in [ColumnType.typeTinyInt, ColumnType.typeUTinyInt, ColumnType.typeSmallInt, ColumnType.typeUSmallInt,
                            ColumnType.typeMediumInt, ColumnType.typeUMediumInt, ColumnType.typeInt, ColumnType.typeUInt,
                            ColumnType.typeBigInt, ColumnType.typeUInt]:
            self._write_pk_is_handle_test(writer, ColumnElement("mypk", column_type, False, True))

    def write_update_delete_test2(self, file, db_name, table_name):
        writer = StmtWriter(file, db_name, table_name)
        file.write("# cluster index test\n")
        file.write("mysql> set global tidb_enable_clustered_index=ON\n")
        primary_key_elements = self.column_type_manager.get_all_primary_key_elements()
        for primary_key_element in primary_key_elements:
            self._write_cluster_index_test(writer, [primary_key_element])
        # the max column num in primary key is 16
        max_columns_in_primary_key = 16
        self._write_cluster_index_test(writer, primary_key_elements[:max_columns_in_primary_key])
        # TODO: INT_ONLY may be removed in future release
        file.write("mysql> set global tidb_enable_clustered_index=INT_ONLY\n")


def write_case(path, db_name, table_name, case_func):
    with open(path, "w") as file:
        case_func(file, db_name, table_name)


def run(db_name, table_name, test_dir):
    column_type_manager = ColumnTypeManager()
    register_all_types(column_type_manager)

    case_writer = TestCaseWriter(column_type_manager)
    # case: decode min/max/normal/default/null values of different type
    write_case(test_dir + "/basic_codec.test", db_name, table_name, case_writer.write_basic_type_codec_test)
    # case: update/delete for different kinds of primary key
    write_case(test_dir + "/update_delete1.test", db_name, table_name, case_writer.write_update_delete_test1)
    write_case(test_dir + "/update_delete2.test", db_name, table_name, case_writer.write_update_delete_test2)


def main():
    if len(sys.argv) != 3:
        print('usage: <bin> database table')
        sys.exit(1)

    db_name = sys.argv[1]
    table_name = sys.argv[2]
    test_dir = "./fullstack-test2/auto_gen/"
    directory = os.path.dirname(test_dir)
    try:
        os.makedirs(directory)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise

    try:
        print("begin to create test case to path {}".format(test_dir))
        run(db_name, table_name, test_dir)
        print("create test case done")
    except KeyboardInterrupt:
        print('Test interrupted')
        sys.exit(1)


if __name__ == "__main__":
    main()
