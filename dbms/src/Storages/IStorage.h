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

#include <Common/Exception.h>
#include <Common/RWLock.h>
#include <Core/Names.h>
#include <Core/QueryProcessingStage.h>
#include <Storages/ITableDeclaration.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/TableLockHolder.h>

#include <memory>
#include <optional>
#include <shared_mutex>


namespace DB
{
class Context;
class IBlockInputStream;
class IBlockOutputStream;

using BlockOutputStreamPtr = std::shared_ptr<IBlockOutputStream>;
using BlockInputStreamPtr = std::shared_ptr<IBlockInputStream>;
using BlockInputStreams = std::vector<BlockInputStreamPtr>;

class PipelineExecutorContext;
class PipelineExecGroupBuilder;

class ASTCreateQuery;

class IStorage;

using StoragePtr = std::shared_ptr<IStorage>;
using StorageWeakPtr = std::weak_ptr<IStorage>;

struct Settings;

class AlterCommands;


/** Storage. Responsible for
  * - storage of the table data;
  * - the definition in which files (or not in files) the data is stored;
  * - data lookups and appends;
  * - data storage structure (compression, etc.)
  * - concurrent access to data (locks, etc.)
  */
class IStorage
    : public std::enable_shared_from_this<IStorage>
    , private boost::noncopyable
    , public ITableDeclaration
{
public:
    /// The main name of the table type (for example, StorageDeltaMerge).
    virtual std::string getName() const = 0;

    /** The name of the table.
      */
    virtual std::string getTableName() const = 0;

    /** Returns true if the storage receives data from a remote server or servers. */
    virtual bool isRemote() const { return false; }

    /** Returns true if the storage supports queries with the SAMPLE section. */
    virtual bool supportsSampling() const { return false; }

    /** Returns true if the storage supports queries with the FINAL section. */
    virtual bool supportsFinal() const { return false; }

    /** Returns true if the storage supports queries with the PREWHERE section. */
    virtual bool supportsPrewhere() const { return false; }

    /** Returns true if the storage replicates SELECT, INSERT and ALTER commands among replicas. */
    virtual bool supportsReplication() const { return false; }

    /** Returns true if the storage supports UPSERT, DELETE or UPDATE. */
    virtual bool supportsModification() const { return false; }

    /// Lock table for share. This lock must be acuqired if you want to be sure,
    /// that table will be not dropped while you holding this lock. It's used in
    /// variety of cases starting from SELECT queries to background merges in
    /// MergeTree.
    TableLockHolder lockForShare(
        const String & query_id,
        const std::chrono::milliseconds & acquire_timeout = std::chrono::milliseconds(0));

    /// Lock table for alter. This lock must be acuqired in ALTER queries to be
    /// sure, that we execute only one simultaneous alter. Doesn't affect share lock.
    TableLockHolder lockForAlter(
        const String & query_id,
        const std::chrono::milliseconds & acquire_timeout = std::chrono::milliseconds(0));

    /// Lock table for decoding KV pairs into Blocks.
    /// Mutiple Raft apply threads may decode data in concurrent, we ensure the structure
    /// won't be changed by this lock.
    /// After decoding done, we can release alter lock but keep drop lock for writing data.
    TableStructureLockHolder lockStructureForShare(
        const String & query_id,
        const std::chrono::milliseconds & acquire_timeout = std::chrono::milliseconds(0));

    /// Lock table exclusively. This lock must be acquired if you want to be
    /// sure, that no other thread (SELECT, merge, ALTER, etc.) doing something
    /// with table. For example it allows to wait all threads before DROP or
    /// truncate query.
    ///
    /// NOTE: You have to be 100% sure that you need this lock. It's extremely
    /// heavyweight and makes table irresponsive.
    TableExclusiveLockHolder lockExclusively(
        const String & query_id,
        const std::chrono::milliseconds & acquire_timeout = std::chrono::milliseconds(0));

    /** Read a set of columns from the table.
      * Accepts a list of columns to read, as well as a description of the query,
      *  from which information can be extracted about how to retrieve data
      *  (indexes, locks, etc.)
      * Returns a stream with which you can read data sequentially
      *  or multiple streams for parallel data reading.
      * The `processed_stage` info is also written to what stage the request was processed.
      * (Normally, the function only reads the columns from the list, but in other cases,
      *  for example, the request can be partially processed on a remote server.)
      *
      * context contains settings for one query.
      * Usually Storage does not care about these settings, since they are used in the interpreter.
      * But, for example, for distributed query processing, the settings are passed to the remote server.
      *
      * num_streams - a recommendation, how many streams to return,
      *  if the storage can return a different number of streams.
      *
      * The Storage schema can be changed during lifetime of the returned input streams, but the data read
      * is guaranteed to be immutable once the input streams are returned.
      */
    virtual BlockInputStreams read(
        const Names & /*column_names*/,
        const SelectQueryInfo & /*query_info*/,
        const Context & /*context*/,
        QueryProcessingStage::Enum & /*processed_stage*/,
        size_t /*max_block_size*/,
        unsigned /*num_streams*/)
    {
        throw Exception(
            "Method read(pull model) is not supported by storage " + getName(),
            ErrorCodes::NOT_IMPLEMENTED);
    }

    virtual void read(
        PipelineExecutorContext & /*exec_status*/,
        PipelineExecGroupBuilder & /*group_builder*/,
        const Names & /*column_names*/,
        const SelectQueryInfo & /*query_info*/,
        const Context & /*context*/,
        size_t /*max_block_size*/,
        unsigned /*num_streams*/)
    {
        throw Exception(
            fmt::format("Method read(push model) is not supported by storage {}", getName()),
            ErrorCodes::NOT_IMPLEMENTED);
    }

    /** Writes the data to a table.
      * Receives a description of the query, which can contain information about the data write method.
      * Returns an object by which you can write data sequentially.
      *
      * It is guaranteed that the table structure will not change over the lifetime of the returned streams (that is, there will not be ALTER, RENAME and DROP).
      */
    virtual BlockOutputStreamPtr write(const ASTPtr & /*query*/, const Settings & /*settings*/)
    {
        throw Exception("Method write is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /** Clear the table data. Called before drop the metadata and data of this storage.
      * The difference with `drop` is that after calling `clearData`, the storage must still be able to be restored.
      */
    virtual void clearData() {}

    /** Delete the table data. Called before deleting the directory with the data.
      * If you do not need any action other than deleting the directory with data, you can leave this method blank.
      */
    virtual void drop() {}

    /** Rename the table.
      * Renaming a name in a file with metadata, the name in the list of tables in the RAM, is done separately.
      * In this function, you need to rename the directory with the data, if any.
      * Called when the table structure is locked for write.
      */
    virtual void rename(
        const String & /*new_path_to_db*/,
        const String & /*new_database_name*/,
        const String & /*new_table_name*/)
    {
        throw Exception("Method rename is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /** ALTER tables in the form of column changes that do not affect the change to Storage or its parameters.
      * This method must fully execute the ALTER query, taking care of the locks itself.
      * To update the table metadata on disk, this method should call InterpreterAlterQuery::updateMetadata.
      */
    virtual void alter(
        const TableLockHolder &,
        const AlterCommands & /*params*/,
        const String & /*database_name*/,
        const String & /*table_name*/,
        const Context & /*context*/)
    {
        throw Exception("Method alter is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /** Execute CLEAR COLUMN ... IN PARTITION query which removes column from given partition. */
    virtual void clearColumnInPartition(
        const ASTPtr & /*partition*/,
        const Field & /*column_name*/,
        const Context & /*context*/)
    {
        throw Exception(
            "Method dropColumnFromPartition is not supported by storage " + getName(),
            ErrorCodes::NOT_IMPLEMENTED);
    }

    /** Run the query (DROP|DETACH) PARTITION.
      */
    virtual void dropPartition(
        const ASTPtr & /*query*/,
        const ASTPtr & /*partition*/,
        bool /*detach*/,
        const Context & /*context*/)
    {
        throw Exception("Method dropPartition is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /** Run the query TRUNCATE TABLE.
      */
    virtual void truncate(const ASTPtr & /*query*/, const Context & /*context*/)
    {
        throw Exception("Method truncate is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /** Run the ATTACH request (PART|PARTITION).
      */
    virtual void attachPartition(const ASTPtr & /*partition*/, bool /*part*/, const Context & /*context*/)
    {
        throw Exception("Method attachPartition is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /** Run the FETCH PARTITION query.
      */
    virtual void fetchPartition(const ASTPtr & /*partition*/, const String & /*from*/, const Context & /*context*/)
    {
        throw Exception("Method fetchPartition is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /** Run the FREEZE PARTITION request. That is, create a local backup (snapshot) of data using the `localBackup` function (see localBackup.h)
      */
    virtual void freezePartition(
        const ASTPtr & /*partition*/,
        const String & /*with_name*/,
        const Context & /*context*/)
    {
        throw Exception("Method freezePartition is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /** Perform any background work. For example, combining parts in a MergeTree type table.
      * Returns whether any work has been done.
      */
    virtual bool optimize(
        const ASTPtr & /*query*/,
        const ASTPtr & /*partition*/,
        bool /*final*/,
        bool /*deduplicate*/,
        const Context & /*context*/)
    {
        throw Exception("Method optimize is not supported by storage " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /** If the table have to do some complicated work on startup,
      *  that must be postponed after creation of table object
      *  (like launching some background threads),
      *  do it in this method.
      * You should call this method after creation of object.
      * By default, does nothing.
      * Cannot be called simultaneously by multiple threads.
      */
    virtual void startup() {}

    /** If the table have to do some complicated work when destroying an object - do it in advance.
      * For example, if the table contains any threads for background work - ask them to complete and wait for completion.
      * By default, does nothing.
      * Can be called simultaneously from different threads, even after a call to drop().
      */
    virtual void shutdown() {}

    bool is_dropped{false};

    /// Does table support index for IN sections
    virtual bool supportsIndexForIn() const { return false; }

    /// Provides a hint that the storage engine may evaluate the IN-condition by using an index.
    virtual bool mayBenefitFromIndexForIn(const ASTPtr & /* left_in_operand */) const { return false; }

    /// Checks validity of the data
    virtual bool checkData() const
    {
        throw DB::Exception("Check query is not supported for " + getName() + " storage");
    }

    /// Checks that table could be dropped right now
    /// If it can - returns true
    /// Otherwise - throws an exception with detailed information or returns false
    virtual bool checkTableCanBeDropped() const { return true; }

    /** Notify engine about updated dependencies for this storage. */
    virtual void updateDependencies() {}

    /// Returns data path if storage supports it, empty string otherwise.
    virtual String getDataPath() const { return {}; }

protected:
    using ITableDeclaration::ITableDeclaration;
    using std::enable_shared_from_this<IStorage>::shared_from_this;

private:
    RWLock::LockHolder tryLockTimed(
        const RWLockPtr & rwlock,
        RWLock::Type type,
        const String & query_id,
        const std::chrono::milliseconds & acquire_timeout) const;

    /// You always need to take the next two locks in this order.

    /// Lock required for alter queries (lockForAlter). Allows to execute only
    /// one simultaneous alter query. Also it should be taken by DROP-like
    /// queries, to be sure, that all alters are finished.
    mutable RWLockPtr alter_lock = RWLock::create();

    /// Lock required for drop queries. Every thread that want to ensure, that
    /// table is not dropped have to table this lock for read (lockForShare).
    /// DROP-like queries take this lock for write (lockExclusively), to be sure
    /// that all table threads finished.
    mutable RWLockPtr drop_lock = RWLock::create();
};

/// table name -> table
using Tables = std::map<String, StoragePtr>;

} // namespace DB
