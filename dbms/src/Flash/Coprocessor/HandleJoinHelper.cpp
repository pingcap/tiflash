#include <Common/TiFlashException.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/getLeastSupertype.h>
#include <Flash/Coprocessor/DAGUtils.h>
#include <Flash/Coprocessor/HandleJoinHelper.h>
#include <Interpreters/Join.h>
#include <Storages/Transaction/TypeMapping.h>

#include <unordered_map>

namespace DB
{
std::pair<ASTTableJoin::Kind, size_t> HandleJoinHelper::getJoinKindAndBuildSideIndex(const tipb::Join & join)
{
    static const std::unordered_map<tipb::JoinType, ASTTableJoin::Kind> equal_join_type_map{
        {tipb::JoinType::TypeInnerJoin, ASTTableJoin::Kind::Inner},
        {tipb::JoinType::TypeLeftOuterJoin, ASTTableJoin::Kind::Left},
        {tipb::JoinType::TypeRightOuterJoin, ASTTableJoin::Kind::Right},
        {tipb::JoinType::TypeSemiJoin, ASTTableJoin::Kind::Inner},
        {tipb::JoinType::TypeAntiSemiJoin, ASTTableJoin::Kind::Anti},
        {tipb::JoinType::TypeLeftOuterSemiJoin, ASTTableJoin::Kind::LeftSemi},
        {tipb::JoinType::TypeAntiLeftOuterSemiJoin, ASTTableJoin::Kind::LeftAnti}};
    static const std::unordered_map<tipb::JoinType, ASTTableJoin::Kind> cartesian_join_type_map{
        {tipb::JoinType::TypeInnerJoin, ASTTableJoin::Kind::Cross},
        {tipb::JoinType::TypeLeftOuterJoin, ASTTableJoin::Kind::Cross_Left},
        {tipb::JoinType::TypeRightOuterJoin, ASTTableJoin::Kind::Cross_Right},
        {tipb::JoinType::TypeSemiJoin, ASTTableJoin::Kind::Cross},
        {tipb::JoinType::TypeAntiSemiJoin, ASTTableJoin::Kind::Cross_Anti},
        {tipb::JoinType::TypeLeftOuterSemiJoin, ASTTableJoin::Kind::Cross_LeftSemi},
        {tipb::JoinType::TypeAntiLeftOuterSemiJoin, ASTTableJoin::Kind::Cross_LeftAnti}};

    const auto & join_type_map = join.left_join_keys_size() != 0 ? equal_join_type_map : cartesian_join_type_map;
    auto join_type_it = join_type_map.find(join.join_type());
    if (join_type_it == join_type_map.end())
        throw TiFlashException("Unknown join type in dag request", Errors::Coprocessor::BadRequest);
    /// tmp kind, isn't the final result
    auto kind = join_type_it->second;

    /// in DAG request, inner part is the build side, however for TiFlash implementation,
    /// the build side must be the right side, so need to swap the join side if needed
    /// 1. for (cross) inner join, there is no problem in this swap.
    /// 2. for (cross) semi/anti-semi join, the build side is always right, needn't swap.
    /// 3. for non-cross left/right join, there is no problem in this swap.
    /// 4. for cross left join, the build side is always right, needn't and can't swap.
    /// 5. for cross right join, the build side is always left, so it will always swap and change to cross left join.
    /// note that whatever the build side is, we can't support cross-right join now.
    size_t build_side_index;
    if (kind == ASTTableJoin::Kind::Cross_Right)
    {
        build_side_index = 0;
        kind = ASTTableJoin::Kind::Cross_Left;
    }
    else if (kind == ASTTableJoin::Kind::Cross_Left)
    {
        build_side_index = 1;
    }
    else
    {
        build_side_index = join.inner_idx();
        if (build_side_index != 1)
        {
            if (kind == ASTTableJoin::Kind::Left)
                kind = ASTTableJoin::Kind::Right;
            else if (kind == ASTTableJoin::Kind::Right)
                kind = ASTTableJoin::Kind::Left;
        }
    }

    return {kind, build_side_index};
}

DataTypes HandleJoinHelper::getJoinKeyTypes(const tipb::Join & join)
{
    DataTypes key_types;
    for (int i = 0; i < join.left_join_keys().size(); ++i)
    {
        if (!exprHasValidFieldType(join.left_join_keys(i)) || !exprHasValidFieldType(join.right_join_keys(i)))
            throw TiFlashException("Join key without field type", Errors::Coprocessor::BadRequest);
        DataTypes types;
        types.emplace_back(getDataTypeByFieldTypeForComputingLayer(join.left_join_keys(i).field_type()));
        types.emplace_back(getDataTypeByFieldTypeForComputingLayer(join.right_join_keys(i).field_type()));
        DataTypePtr common_type = getLeastSupertype(types);
        key_types.emplace_back(common_type);
    }
    return key_types;
}

TiDB::TiDBCollators HandleJoinHelper::getJoinKeyCollators(const tipb::Join & join, const DataTypes & join_key_types)
{
    TiDB::TiDBCollators collators;
    size_t join_key_size = join_key_types.size();
    if (join.probe_types_size() == static_cast<int>(join_key_size) && join.build_types_size() == join.probe_types_size())
    {
        for (size_t i = 0; i < join_key_size; ++i)
        {
            if (removeNullable(join_key_types[i])->isString())
            {
                if (join.probe_types(i).collate() != join.build_types(i).collate())
                    throw TiFlashException("Join with different collators on the join key", Errors::Coprocessor::BadRequest);
                collators.push_back(getCollatorFromFieldType(join.probe_types(i)));
            }
            else
            {
                collators.push_back(nullptr);
            }
        }
    }
    return collators;
}

String HandleJoinHelper::genMatchHelperNameForLeftSemiFamily(const Block & left_header, const Block & right_header)
{
    String match_helper_name = Join::match_helper_prefix;
    for (int i = 1; left_header.has(match_helper_name) || right_header.has(match_helper_name); ++i)
    {
        match_helper_name = Join::match_helper_prefix + std::to_string(i);
    }
    return match_helper_name;
}
} // namespace DB
