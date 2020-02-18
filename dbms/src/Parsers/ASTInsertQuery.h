#pragma once

#include <Parsers/IAST.h>


namespace DB
{


/** INSERT query
  */
class ASTInsertQuery : public IAST
{
public:
    explicit ASTInsertQuery(bool is_import_ = false) : is_import(is_import_) {}
    explicit ASTInsertQuery(String database_, String table_, bool is_import_)
        : database(std::move(database_)), table(std::move(table_)), is_import(is_import_)
    {}

public:
    String database;
    String table;
    ASTPtr columns;
    String format;
    ASTPtr select;
    ASTPtr table_function;
    ASTPtr partition_expression_list;

    // Set to true if the data should only be inserted into attached views
    bool no_destination = false;

    /// Data to insert
    const char * data = nullptr;
    const char * end = nullptr;

    // If insert block is synced from TiDB, set is_import = true
    bool is_import = false;
    bool is_upsert = false;
    bool is_delete = false;

    /** Get the text that identifies this element. */
    String getID() const override { return "InsertQuery_" + database + "_" + table; };

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTInsertQuery>(*this);
        res->children.clear();

        if (columns)
        {
            res->columns = columns->clone();
            res->children.push_back(res->columns);
        }
        if (select)
        {
            res->select = select->clone();
            res->children.push_back(res->select);
        }
        if (table_function)
        {
            res->table_function = table_function->clone();
            res->children.push_back(res->table_function);
        }

        return res;
    }

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

} // namespace DB
