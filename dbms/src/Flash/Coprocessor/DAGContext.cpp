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

enum Flag
{
    IGNORE_TRUNCATE = 1,
    TRUNCATE_AS_WARNING = 1u << 1u,
    PAD_CHAR_TO_FULL_LENGTH = 1u << 2u,
    IN_INSERT_STMT = 1u << 3u,
    IN_UPDATE_OR_DELETE_STMT = 1u << 4u,
    IN_SELECT_STMT = 1u << 5u,
    OVERFLOW_AS_WARNING = 1u << 6u,
    DIVIDED_BY_ZERO_AS_WARNING = 1u << 8u,
    IN_LOAD_DATA_STMT = 1u << 10u,
};

enum SqlMode
{
    STRICT_TRANS_TABLES = 1u << 22u,
    STRICT_ALL_TABLES = 1u << 23u,
    NO_ZERO_IN_DATE = 1u << 24u,
    NO_ZERO_DATE = 1u << 25u,
    INVALID_DATES = 1u << 26u,
    ERROR_FOR_DIVISION_BY_ZERO = 1u << 27u,
};

bool strictSqlMode(UInt64 sql_mode) { return sql_mode & SqlMode::STRICT_ALL_TABLES || sql_mode & SqlMode::STRICT_TRANS_TABLES; }

std::map<String, ProfileStreamsInfo> & DAGContext::getProfileStreamsMap() { return profile_streams_map; }

std::unordered_map<String, BlockInputStreams> & DAGContext::getProfileStreamsMapForJoinBuildSide()
{
    return profile_streams_map_for_join_build_side;
}

std::unordered_map<UInt32, std::vector<String>> & DAGContext::getQBIdToJoinAliasMap() { return qb_id_to_join_alias_map; }

void DAGContext::handleTruncateError(const String & msg)
{
    if (!(flags & Flag::IGNORE_TRUNCATE || flags & Flag::TRUNCATE_AS_WARNING))
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
    if (!(flags & Flag::OVERFLOW_AS_WARNING))
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
    if (flags & Flag::IN_INSERT_STMT || flags & Flag::IN_UPDATE_OR_DELETE_STMT)
    {
        if (!(sql_mode & SqlMode::ERROR_FOR_DIVISION_BY_ZERO))
            return;
        if (strictSqlMode(sql_mode) && !(flags & Flag::DIVIDED_BY_ZERO_AS_WARNING))
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
    if (strictSqlMode(sql_mode) && (flags & Flag::IN_INSERT_STMT || flags & Flag::IN_UPDATE_OR_DELETE_STMT))
    {
        throw TiFlashException(msg, error);
    }
}

bool DAGContext::shouldClipToZero() { return flags & Flag::IN_INSERT_STMT || flags & Flag::IN_LOAD_DATA_STMT; }

} // namespace DB
