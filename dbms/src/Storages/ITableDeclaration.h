// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Storages/ColumnsDescription.h>


namespace DB
{
/** Description of the table.
  * Is not thread safe. See IStorage::lockStructure ().
  */
class ITableDeclaration
{
public:
    virtual const ColumnsDescription & getColumns() const { return columns; }
    virtual void setColumns(ColumnsDescription columns_);

    /// NOTE: These methods should include virtual columns, but should NOT include ALIAS columns
    /// (they are treated separately).
    virtual NameAndTypePair getColumn(const String & column_name) const;
    virtual bool hasColumn(const String & column_name) const;

    Block getSampleBlock() const;
    Block getSampleBlockNonMaterialized() const;
    Block getSampleBlockForColumns(const Names & column_names) const;

    /** The hidden coloumns will not be returned. Mainly for INSERT query.
      */
    Block getSampleBlockNoHidden() const;
    Block getSampleBlockNonMaterializedNoHidden() const;

    /** Verify that all the requested names are in the table and are set correctly.
      * (the list of names is not empty and the names do not repeat)
      */
    void check(const Names & column_names) const;

    /** Check that all the requested names are in the table and have the correct types.
      */
    void check(const NamesAndTypesList & columns) const;

    /** Check that all names from the intersection of `names` and `columns` are in the table and have the same types.
      */
    void check(const NamesAndTypesList & columns, const Names & column_names) const;

    /** Check that the data block for the record contains all the columns of the table with the correct types,
      *  contains only the columns of the table, and all the columns are different.
      * If need_all, still checks that all the columns of the table are in the block.
      */
    void check(const Block & block, bool need_all = false) const;


    ITableDeclaration() = default;
    explicit ITableDeclaration(ColumnsDescription columns_);
    virtual ~ITableDeclaration() = default;

private:
    ColumnsDescription columns;

    OrderedNameSet empty_names;

    virtual const OrderedNameSet & getHiddenColumnsImpl() const { return empty_names; }
    void setColumnsImpl(ColumnsDescription columns_);
};

} // namespace DB
