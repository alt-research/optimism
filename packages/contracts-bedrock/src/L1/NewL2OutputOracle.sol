// SPDX-License-Identifier: UNLICENSED
pragma solidity 0.8.15;

import { L2OutputOracle } from "src/L1/L2OutputOracle.sol";

contract AltL2OutputOracle is L2OutputOracle {
    function updateProposerAndChallenger(address _proposer, address _challenger) public {
        require(msg.sender == proposer, "L2OutputOracle: only last proposer can update to new address.");
        proposer = _proposer;
        challenger = _challenger;
    }
}
