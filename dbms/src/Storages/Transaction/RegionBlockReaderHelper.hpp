#include <sparsehash/dense_hash_map>
#include <sparsehash/dense_hash_set>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

// ColumnID => <ColumnPtr, <name, type>, offset>
struct ColumnDataInfoMap
{
    using ColTypeInfo = std::tuple<MutableColumnPtr, NameAndTypePair, size_t>;
    using ColTypeInfoData = std::vector<ColTypeInfo>;

    ColumnDataInfoMap(const size_t cap, const ColumnID empty_id)
    {
        column_data.reserve(cap);
        column_map.set_empty_key(empty_id);
        ori_cap = column_data.capacity();
    }

    /// Notice: iterator of std::vector will invalid after the capacity changed, so !!! must set the capacity big enough
    void checkValid() const
    {
        if (ori_cap != column_data.capacity())
            throw Exception("ColumnDataInfoMap capacity changes", ErrorCodes::LOGICAL_ERROR);
    }

    void insert(const ColumnID col_id, MutableColumnPtr && ptr, NameAndTypePair && name_pair, size_t index, const size_t cap)
    {
        column_data.emplace_back(std::move(ptr), std::move(name_pair), index);
        column_map.insert(std::make_pair(col_id, column_data.end() - 1));
        getMutableColumnPtr(col_id)->reserve(cap);
    }

    MutableColumnPtr & getMutableColumnPtr(const ColumnID col_id) { return getMutableColumnPtr((*this)[col_id]); }
    static MutableColumnPtr & getMutableColumnPtr(ColTypeInfo & info) { return std::get<0>(info); }

    NameAndTypePair & getNameAndTypePair(const ColumnID col_id) { return getNameAndTypePair((*this)[col_id]); }
    static NameAndTypePair & getNameAndTypePair(ColTypeInfo & info) { return std::get<1>(info); }

    static size_t getIndex(const ColTypeInfo & info) { return std::get<2>(info); }

    ColTypeInfo & operator[](const ColumnID col_id) { return *column_map[col_id]; }

private:
    ColTypeInfoData column_data;
    google::dense_hash_map<ColumnID, ColTypeInfoData::iterator> column_map;
    size_t ori_cap;
};

struct DecodedRecordData
{
    DecodedRecordData(const size_t cap)
    {
        additional_decoded_fields.reserve(cap);
        ori_cap = additional_decoded_fields.capacity();
    }

    /// just like ColumnDataInfoMap::checkValid
    void checkValid() const
    {
        if (ori_cap != additional_decoded_fields.capacity())
            throw Exception("DecodedRecordData capacity changes", ErrorCodes::LOGICAL_ERROR);
    }

    size_t size() const { return decoded_col_iter.size(); }

    void clear()
    {
        additional_decoded_fields.clear();
        decoded_col_iter.clear();
    }

    const DecodedFields::value_type & operator[](const size_t index) const { return *decoded_col_iter[index]; }

    template <class... _Args>
    void emplace_back(_Args &&... __args)
    {
        additional_decoded_fields.emplace_back(std::forward<_Args>(__args)...);
        decoded_col_iter.emplace_back(additional_decoded_fields.cend() - 1);
    }

    void push_back(const DecodedFields::const_iterator & iter) { decoded_col_iter.push_back(iter); }

private:
    DecodedFields additional_decoded_fields;
    std::vector<DecodedFields::const_iterator> decoded_col_iter;
    size_t ori_cap;
};

} // namespace DB
