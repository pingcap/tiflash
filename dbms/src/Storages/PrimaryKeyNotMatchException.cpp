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


#include <Databases/DatabasesCommon.h>
#include <IO/FileProvider/FileProvider.h>
#include <IO/FileProvider/WriteBufferFromWritableFileBuilder.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Poco/File.h>
#include <Poco/Logger.h>
#include <Storages/PrimaryKeyNotMatchException.h>
#include <common/logger_useful.h>

#include <vector>

namespace DB
{
String fixCreateStatementWithPriKeyNotMatchException( //
    Context & context,
    const String old_definition,
    const String & table_metadata_path,
    const PrimaryKeyNotMatchException & ex,
    Poco::Logger * log)
{
    LOG_WARNING(
        log,
        "Try to fix statement in " + table_metadata_path + ", primary key [" + ex.pri_key + "] -> [" + ex.actual_pri_key
            + "]");
    // Try to fix the create statement.
    ParserCreateQuery parser;
    ASTPtr ast = parseQuery(
        parser,
        old_definition.data(),
        old_definition.data() + old_definition.size(),
        "in file " + table_metadata_path,
        0);
    ASTCreateQuery & ast_create_query = typeid_cast<ASTCreateQuery &>(*ast);
    auto args = ast_create_query.storage->engine->arguments;
    if (!args->children.empty())
    {
        ASTPtr pk_ast = std::make_shared<ASTExpressionList>();
        pk_ast->children.emplace_back(std::make_shared<ASTIdentifier>(ex.actual_pri_key));
        args->children[0] = pk_ast;
    }
    String statement = getTableDefinitionFromCreateQuery(ast);
    const String table_metadata_tmp_path = table_metadata_path + ".tmp";

    {
        // 1. Assume the case that we need to rename the file `t_31.sql.tmp` to `t_31.sql`,
        // and t_31.sql already exists and is a encrypted file.
        // 2. The implementation in this function assume that the rename operation is atomic.
        // 3. If we create new encryption info for `t_31.sql.tmp`,
        // then we cannot rename the encryption info and the file in an atomic operation.
        bool use_target_encrypt_info
            = context.getFileProvider()->isFileEncrypted(EncryptionPath(table_metadata_path, ""));
        EncryptionPath encryption_path = use_target_encrypt_info ? EncryptionPath(table_metadata_path, "")
                                                                 : EncryptionPath(table_metadata_tmp_path, "");
        {
            bool create_new_encryption_info = !use_target_encrypt_info && !statement.empty();
            auto out = WriteBufferFromWritableFileBuilder::build(
                context.getFileProvider(),
                table_metadata_tmp_path,
                encryption_path,
                create_new_encryption_info,
                nullptr,
                statement.size(),
                O_WRONLY | O_CREAT | O_EXCL);
            writeString(statement, out);
            out.next();
            if (context.getSettingsRef().fsync_metadata)
                out.sync();
            out.close();
        }

        try
        {
            /// rename atomically replaces the old file with the new one.
            context.getFileProvider()->renameFile(
                table_metadata_tmp_path,
                encryption_path,
                table_metadata_path,
                EncryptionPath(table_metadata_path, ""),
                !use_target_encrypt_info);
        }
        catch (...)
        {
            context.getFileProvider()->deleteRegularFile(
                table_metadata_tmp_path,
                EncryptionPath(table_metadata_tmp_path, ""));
            throw;
        }
    }
    return statement;
}

} // namespace DB
