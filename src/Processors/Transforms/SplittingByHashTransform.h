#pragma once
#include <Processors/IProcessor.h>
#include <Core/ColumnNumbers.h>
#include <Common/WeakHash.h>

namespace DB
{

/// Split single stream of chunks to num_outputs streams.
/// Split is performed by formula (WeakHash(key_columns) * num_outputs / MAX_INT).
class SplittingByHashTransform : public IProcessor
{
public:
    SplittingByHashTransform(const Block & header, size_t num_outputs, ColumnNumbers key_columns_);

    String getName() const override { return "SplittingByHash"; }
    Status prepare() override;
    void work() override;

    InputPort & getInputPort() { return inputs.front(); }

private:
    ColumnNumbers key_columns;

    Chunk input_chunk;
    Chunks output_chunks;

    IColumn::Selector selector;
    WeakHash32 hash;

    bool is_generating_phase = false;

    Status prepareGenerate();
    Status prepareConsume();
};

}
