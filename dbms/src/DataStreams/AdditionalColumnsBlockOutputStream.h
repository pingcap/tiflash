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

#include <DataStreams/IBlockOutputStream.h>
#include <DataTypes/DataTypesNumber.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <pingcap/pd/IClient.h>

namespace DB
{

class IAdditionalBlockGenerator
{
public:
    virtual ColumnWithTypeAndName genColumn(const Block & ref) const = 0;
    virtual const std::string & getName() const = 0;
    virtual ~IAdditionalBlockGenerator() {}
};

using AdditionalBlockGeneratorPtr = std::shared_ptr<IAdditionalBlockGenerator>;
using AdditionalBlockGenerators = std::vector<AdditionalBlockGeneratorPtr>;


class AdditionalBlockGeneratorPD : public IAdditionalBlockGenerator
{
public:
    AdditionalBlockGeneratorPD(const String name_, pingcap::pd::ClientPtr pd_) : name(name_), pd(pd_) {}

    ColumnWithTypeAndName genColumn(const Block & ref) const override
    {
        auto data_type = std::make_shared<DataTypeNumber<UInt64> >();
        auto column = data_type->createColumn();
        size_t size = ref.rows();
        for (size_t i = 0; i < size; i++)
        {
            auto ts = pd->getTS();
            Field data(ts);
            column->insert(data);
        }
        return ColumnWithTypeAndName(std::move(column), data_type, name);
    }

    const std::string & getName() const override
    {
        return name;
    }

private:
    String name;
    pingcap::pd::ClientPtr pd;
};

template <typename T>
class AdditionalBlockGeneratorConst : public IAdditionalBlockGenerator
{
public:
    using AdditionalDataType = DataTypeNumber<T>;

    AdditionalBlockGeneratorConst(const String name_, const T & value_) : name(name_), value(value_)
    {}

    ColumnWithTypeAndName genColumn(const Block & ref) const override
    {
        auto data_type = std::make_shared<AdditionalDataType>();
        auto column = data_type->createColumn();
        Field data = value;
        size_t size = ref.rows();
        for (size_t i = 0; i < size; i++)
            column->insert(data);
        return ColumnWithTypeAndName(std::move(column), data_type, name);
    }

    const std::string & getName() const override
    {
        return name;
    }

private:
    String name;
    const UInt64 value;
};


template <typename T>
class AdditionalBlockGeneratorIncrease : public IAdditionalBlockGenerator
{
public:
    using AdditionalDataType = DataTypeNumber<T>;

    AdditionalBlockGeneratorIncrease(const String name_, const T & value_) : name(name_), value(value_)
    {}

    ColumnWithTypeAndName genColumn(const Block & ref) const override
    {
        auto data_type = std::make_shared<AdditionalDataType>();
        size_t size = ref.rows();
        auto column = data_type->createColumn();
        for (size_t i = 0; i < size; i++)
        {
            Field data = value + i;
            column->insert(data);
        }
        return ColumnWithTypeAndName(std::move(column), data_type, name);
    }

    const std::string & getName() const override
    {
        return name;
    }

private:
    String name;
    UInt64 value;
};


class AdditionalColumnsBlockOutputStream: public IBlockOutputStream
{
public:
    AdditionalColumnsBlockOutputStream(BlockOutputStreamPtr output_, AdditionalBlockGenerators gens_)
        : output(output_), gens(gens_) {}

    Block getHeader() const override
    {
        Block block = output->getHeader();
        for (auto it: gens)
        {
            if (!block.has(it->getName()))
                block.insert(it->genColumn(block));
        }
        return block;
    }

    void write(const Block & block) override
    {
        if (block)
        {
            Block clone = block;
            for (auto it: gens)
            {
                // TODO: dont' gen default columns
                if (clone.has(it->getName()))
                    clone.erase(it->getName());
                clone.insert(it->genColumn(block));
            }
            output->write(clone);
        } else
        {
            output->write(block);
        }
    }

    void writeSuffix() override
    {
        output->writeSuffix();
    }

    void writePrefix() override
    {
        output->writePrefix();
    }

    void flush() override
    {
        output->flush();
    }

    void setRowsBeforeLimit(size_t rows_before_limit) override
    {
        output->setRowsBeforeLimit(rows_before_limit);
    }

    void setTotals(const Block & totals) override
    {
        output->setTotals(totals);
    }

    void setExtremes(const Block & extremes) override
    {
        output->setExtremes(extremes);
    }

    void onProgress(const Progress & progress) override
    {
        output->onProgress(progress);
    }

    std::string getContentType() const override
    {
        return output->getContentType();
    }

private:
    BlockOutputStreamPtr output;
    AdditionalBlockGenerators gens;
};

}
