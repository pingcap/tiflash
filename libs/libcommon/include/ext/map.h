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

#include <boost/iterator/transform_iterator.hpp>
#include <type_traits>


namespace ext
{
/// \brief Strip type off top level reference and cv-qualifiers thus allowing storage in containers
template <typename T>
using unqualified_t = std::remove_cv_t<std::remove_reference_t<T>>;

/** \brief Returns collection of the same container-type as the input collection,
      *    with each element transformed by the application of `mapper`.
      */
template <template <typename...> class Collection, typename... Params, typename Mapper>
auto map(const Collection<Params...> & collection, const Mapper mapper)
{
    using value_type = unqualified_t<decltype(mapper(*std::begin(collection)))>;

    return Collection<value_type>(
        boost::make_transform_iterator(std::begin(collection), mapper),
        boost::make_transform_iterator(std::end(collection), mapper));
};

/** \brief Returns collection of specified container-type,
      *    with each element transformed by the application of `mapper`.
      *    Allows conversion between different container-types, e.g. std::vector to std::list
      */
template <template <typename...> class ResultCollection, typename Collection, typename Mapper>
auto map(const Collection & collection, const Mapper mapper)
{
    using value_type = unqualified_t<decltype(mapper(*std::begin(collection)))>;

    return ResultCollection<value_type>(
        boost::make_transform_iterator(std::begin(collection), mapper),
        boost::make_transform_iterator(std::end(collection), mapper));
};

/** \brief Returns collection of specified type,
      *    with each element transformed by the application of `mapper`.
      *    Allows leveraging implicit conversion between the result of applying `mapper` and R::value_type.
      */
template <typename ResultCollection, typename Collection, typename Mapper>
auto map(const Collection & collection, const Mapper mapper)
{
    return ResultCollection(
        boost::make_transform_iterator(std::begin(collection), mapper),
        boost::make_transform_iterator(std::end(collection), mapper));
}
} // namespace ext
