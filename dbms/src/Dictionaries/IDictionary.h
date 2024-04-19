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

#include <Common/PODArray.h>
#include <Core/Field.h>
#include <Core/Names.h>
#include <Dictionaries/IDictionarySource.h>
#include <Interpreters/IExternalLoadable.h>
#include <Poco/Util/XMLConfiguration.h>
#include <common/StringRef.h>

#include <memory>

namespace DB
{

struct IDictionaryBase;
using DictionaryPtr = std::unique_ptr<IDictionaryBase>;

struct DictionaryStructure;
class ColumnString;

class IBlockInputStream;
using BlockInputStreamPtr = std::shared_ptr<IBlockInputStream>;


struct IDictionaryBase : public IExternalLoadable
{
    using Key = UInt64;

    virtual std::string getTypeName() const = 0;

    virtual size_t getBytesAllocated() const = 0;

    virtual size_t getQueryCount() const = 0;

    virtual double getHitRate() const = 0;

    virtual size_t getElementCount() const = 0;

    virtual double getLoadFactor() const = 0;

    virtual bool isCached() const = 0;

    virtual const IDictionarySource * getSource() const = 0;

    virtual const DictionaryStructure & getStructure() const = 0;

    virtual bool isInjective(const std::string & attribute_name) const = 0;

    bool supportUpdates() const override { return !isCached(); }

    bool isModified() const override
    {
        const auto * source = getSource();
        return source && source->isModified();
    }

    std::shared_ptr<IDictionaryBase> shared_from_this()
    {
        return std::static_pointer_cast<IDictionaryBase>(IExternalLoadable::shared_from_this());
    }

    std::shared_ptr<const IDictionaryBase> shared_from_this() const
    {
        return std::static_pointer_cast<const IDictionaryBase>(IExternalLoadable::shared_from_this());
    }
};


struct IDictionary : IDictionaryBase
{
    virtual bool hasHierarchy() const = 0;

    virtual void toParent(const PaddedPODArray<Key> & ids, PaddedPODArray<Key> & out) const = 0;

    virtual void has(const PaddedPODArray<Key> & ids, PaddedPODArray<UInt8> & out) const = 0;

    /// Methods for hierarchy.

    virtual void isInVectorVector(
        const PaddedPODArray<Key> & /*child_ids*/,
        const PaddedPODArray<Key> & /*ancestor_ids*/,
        PaddedPODArray<UInt8> & /*out*/) const
    {
        throw Exception("Hierarchy is not supported for " + getName() + " dictionary.", ErrorCodes::NOT_IMPLEMENTED);
    }

    virtual void isInVectorConstant(
        const PaddedPODArray<Key> & /*child_ids*/,
        const Key /*ancestor_id*/,
        PaddedPODArray<UInt8> & /*out*/) const
    {
        throw Exception("Hierarchy is not supported for " + getName() + " dictionary.", ErrorCodes::NOT_IMPLEMENTED);
    }

    virtual void isInConstantVector(
        const Key /*child_id*/,
        const PaddedPODArray<Key> & /*ancestor_ids*/,
        PaddedPODArray<UInt8> & /*out*/) const
    {
        throw Exception("Hierarchy is not supported for " + getName() + " dictionary.", ErrorCodes::NOT_IMPLEMENTED);
    }

    void isInConstantConstant(const Key child_id, const Key ancestor_id, UInt8 & out) const
    {
        PaddedPODArray<UInt8> out_arr(1);
        isInVectorConstant(PaddedPODArray<Key>(1, child_id), ancestor_id, out_arr);
        out = out_arr[0];
    }
};

} // namespace DB
