#pragma once

#include "processor/result/factorized_table.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

struct TargetNodes {};
struct IFEState {
    common::nodeID_t startNode;


};

struct IFEMorsel {
    IFEState* state;
    // Range from current frontier.
    common::offset_t startOffset;
    common::offset_t endOffset;
};

struct IFESharedState {
    // Input tuples.
    std::shared_ptr<FactorizedTable> inputTable;
    common::offset_t tableCursor;
    // Target nodes. Optional information
    TargetNodes targetNodes;

    // TODO: we probably don't need a dynamic data structure.
    std::vector<std::shared_ptr<IFEState>> activeIFEState;

    IFEMorsel getMorsel()  {
        if (tableCursor < inputTable->getNumTuples()) {
            // Scan from inputTable and generate new IFE.
            common::nodeID_t startNode;
            auto state = std::make_shared<IFEState>();
            tableCursor++;
            return IFEMorsel();
        }
        // Scan from active IFE state.
    }
};


class IFE : public PhysicalOperator {
public:
    bool getNextTuplesInternal(ExecutionContext *context) override {
        // try get a new IFE state
        // fail
    }


private:
    std::shared_ptr<IFESharedState> sharedState;
};

struct IFEScan {
    virtual void scan(const IFEMorsel& morsel) {

    }
};


}
}
