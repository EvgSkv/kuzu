#pragma once

#include "processor/result/factorized_table.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

struct TargetNodes {};
struct FrontierMorsel;

struct IFEState {
    std::mutex mtx;

    common::nodeID_t startNode;

    uint8_t currentLevel;

    uint32_t numThreadsRegistered;
    uint32_t numThreadsFinished;

    void registerThread();
    void finishThread();

    bool completed();
    bool currentLevelCompleted();
    void finializeLevel(); // Be careful! This will have a race condition.
    FrontierMorsel getFrontierMorsel();
};

struct FrontierMorsel {
    std::mutex mtx;
    // Frontier should be retrivable from
    IFEState* state;
    uint8_t level;
    // Range from current frontier.
    common::offset_t startOffset;
    common::offset_t endOffset;

    bool empty() const { return endOffset == startOffset; }
};

struct IFESharedState {
    std::mutex mtx;
    // Input tuples.
    std::shared_ptr<FactorizedTable> inputTable;
    common::offset_t tableCursor;
    // Target nodes. Optional information
    TargetNodes targetNodes;

    // TODO: we probably don't need a dynamic data structure.
    std::vector<std::shared_ptr<IFEState>> activeIFEState;

    IFEState* getIFEState()  {
        std::unique_lock lck{mtx};
        if (tableCursor < inputTable->getNumTuples()) {
            activeIFEState.push_back({});
            // Generate new IFE state and return
        }
        // Return an active IFE state.
        return activeIFEState[0].get();
    }
};

//struct Node {
//    common::nodeID_t nodeID;
//    Node* next;
//};

struct Frontier {
    // I need hash map implementation that allows me to go from a node_ID to an object.
    // This object vector<pair<node_ID, rel_ID, weight>>

    //
};

// The naive approach is
struct DGraph {

    std::vector<Frontier> frontiers;
    Frontier* current;
    Frontier* next;

    // Assuming
    void insert(common::nodeID_t u, common::relID_t r, common::nodeID_t v) {

    }
};

class IFE : public PhysicalOperator {
public:
    bool getNextTuplesInternal(ExecutionContext *context) override {
        while (true) {
            auto ifeState = sharedState->getIFEState();
            if (ifeState == nullptr) { // No more ife to compute
                return false;
            }
            // if can enumerate path
            //      return true;
            ifeState->registerThread(); // track how many threads
            computeIFE(ifeState);
        }
    }

    bool computeIFE(IFEState* state) {
        while (!state->completed()) {
            auto frontierMorsel = state->getFrontierMorsel();
            if (frontierMorsel.empty()) { // no more work on current frontier.
                state->finishThread(); // notify thread is done with current frontier.
                if (!state->currentLevelCompleted()) {
                    // thread sleep. wait for other thread to finish
                } else {
                    state->finializeLevel();
                }
                continue;
            }

            // perform thread local scan and extend


            // update local DGraph
        }
    }

    bool enumeratePath(IFEState* state) {
        // morselize based on last frontier
    }


private:
    std::shared_ptr<IFESharedState> sharedState;
};

}
}
