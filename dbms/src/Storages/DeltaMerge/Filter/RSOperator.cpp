#include <Storages/DeltaMerge/Filter/And.h>
#include <Storages/DeltaMerge/Filter/Equal.h>
#include <Storages/DeltaMerge/Filter/Greater.h>
#include <Storages/DeltaMerge/Filter/GreaterEqual.h>
#include <Storages/DeltaMerge/Filter/In.h>
#include <Storages/DeltaMerge/Filter/Less.h>
#include <Storages/DeltaMerge/Filter/LessEqual.h>
#include <Storages/DeltaMerge/Filter/Like.h>
#include <Storages/DeltaMerge/Filter/Not.h>
#include <Storages/DeltaMerge/Filter/NotEqual.h>
#include <Storages/DeltaMerge/Filter/NotIn.h>
#include <Storages/DeltaMerge/Filter/NotLike.h>
#include <Storages/DeltaMerge/Filter/Or.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/Filter/Unsupported.h>

namespace DB
{
namespace DM
{
// clang-format off
RSOperatorPtr createAnd(const RSOperators & children)                                           { return std::make_shared<And>(children); }
RSOperatorPtr createEqual(const Attr & attr, const Field & value)                               { return std::make_shared<Equal>(attr, value); }
RSOperatorPtr createGreater(const Attr & attr, const Field & value, int null_direction)         { return std::make_shared<Greater>(attr, value, null_direction); }
RSOperatorPtr createGreaterEqual(const Attr & attr, const Field & value, int null_direction)    { return std::make_shared<GreaterEqual>(attr, value, null_direction); }
RSOperatorPtr createIn(const Attr & attr, const Fields & values)                                { return std::make_shared<In>(attr, values); }
RSOperatorPtr createLess(const Attr & attr, const Field & value, int null_direction)            { return std::make_shared<Less>(attr, value, null_direction); }
RSOperatorPtr createLessEqual(const Attr & attr, const Field & value, int null_direction)       { return std::make_shared<LessEqual>(attr, value, null_direction); }
RSOperatorPtr createLike(const Attr & attr, const Field & value)                                { return std::make_shared<Like>(attr, value); }
RSOperatorPtr createNot(const RSOperatorPtr & op)                                               { return std::make_shared<Not>(op); }
RSOperatorPtr createNotEqual(const Attr & attr, const Field & value)                            { return std::make_shared<NotEqual>(attr, value); }
RSOperatorPtr createNotIn(const Attr & attr, const Fields & values)                             { return std::make_shared<NotIn>(attr, values); }
RSOperatorPtr createNotLike(const Attr & attr, const Field & value)                             { return std::make_shared<NotLike>(attr, value); }
RSOperatorPtr createOr(const RSOperators & children)                                            { return std::make_shared<Or>(children); }
RSOperatorPtr createUnsupported(const String & content, const String & reason, bool is_not)     { return std::make_shared<Unsupported>(content, reason, is_not); }
// clang-format on
} // namespace DM
} // namespace DB