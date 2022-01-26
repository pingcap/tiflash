#include <Flash/Coprocessor/DAGContext.h>

namespace DB
{

namespace ErrorCodes
{
extern const int TRUNCATE_ERROR;
extern const int OVERFLOW_ERROR;
extern const int DIVIDED_BY_ZERO;
extern const int INVALID_TIME;
} // namespace ErrorCodes

bool strictSqlMode(UInt64 sql_mode) { return sql_mode & TiDBSQLMode::STRICT_ALL_TABLES || sql_mode & TiDBSQLMode::STRICT_TRANS_TABLES; }

bool DAGContext::allowZeroInDate() const { return flags & TiDBSQLFlags::IGNORE_ZERO_IN_DATE; }

bool DAGContext::allowInvalidDate() const { return sql_mode & TiDBSQLMode::ALLOW_INVALID_DATES; }

std::map<String, ProfileStreamsInfo> & DAGContext::getProfileStreamsMap() { return profile_streams_map; }

std::unordered_map<String, BlockInputStreams> & DAGContext::getProfileStreamsMapForJoinBuildSide()
{
    return profile_streams_map_for_join_build_side;
}

std::unordered_map<UInt32, std::vector<String>> & DAGContext::getQBIdToJoinAliasMap() { return qb_id_to_join_alias_map; }

void DAGContext::handleTruncateError(const String & msg)
{
    if (!(flags & TiDBSQLFlags::IGNORE_TRUNCATE || flags & TiDBSQLFlags::TRUNCATE_AS_WARNING))
    {
        throw TiFlashException("Truncate error " + msg, Errors::Types::Truncated);
    }
    tipb::Error warning;
    warning.set_code(0);
    warning.set_msg(msg);
    appendWarning(warning);
}

void DAGContext::handleOverflowError(const String & msg, const TiFlashError & error)
{
    if (!(flags & TiDBSQLFlags::OVERFLOW_AS_WARNING))
    {
        throw TiFlashException("Overflow error: " + msg, error);
    }
    tipb::Error warning;
    warning.set_code(0);
    warning.set_msg("Overflow error: " + msg);
    appendWarning(warning);
}

void DAGContext::handleDivisionByZero()
{
    if (flags & TiDBSQLFlags::IN_INSERT_STMT || flags & TiDBSQLFlags::IN_UPDATE_OR_DELETE_STMT)
    {
        if (!(sql_mode & TiDBSQLMode::ERROR_FOR_DIVISION_BY_ZERO))
            return;
        if (strictSqlMode(sql_mode) && !(flags & TiDBSQLFlags::DIVIDED_BY_ZERO_AS_WARNING))
        {
            throw TiFlashException("Division by 0", Errors::Expression::DivisionByZero);
        }
    }
    tipb::Error warning;
    warning.set_code(0);
    warning.set_msg("Division by 0");
    appendWarning(warning);
}

void DAGContext::handleInvalidTime(const String & msg, const TiFlashError & error)
{
    if (!(error.is(Errors::Types::WrongValue) || error.is(Errors::Types::Truncated)))
    {
        throw TiFlashException(msg, error);
    }
    handleTruncateError(msg);
    if (strictSqlMode(sql_mode) && (flags & TiDBSQLFlags::IN_INSERT_STMT || flags & TiDBSQLFlags::IN_UPDATE_OR_DELETE_STMT))
    {
        throw TiFlashException(msg, error);
    }
}

bool DAGContext::shouldClipToZero() { return flags & TiDBSQLFlags::IN_INSERT_STMT || flags & TiDBSQLFlags::IN_LOAD_DATA_STMT; }

} // namespace DB
