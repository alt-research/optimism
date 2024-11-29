// SPDX-License-Identifier: UNLICENSED
pragma solidity 0.8.15;

import { L2OutputOracle } from "src/L1/L2OutputOracle.sol";

/// @custom:proxied
/// @title AltL2OutputOracle
/// @notice This contract is inherited from `L2OutputOracle` to provide a patch to update proposer and challenger.
contract AltL2OutputOracle is L2OutputOracle {

    /// @notice Emitted when accounts are updated.
    /// @param proposer   New proposer account.
    /// @param challenger New challenger account.
    event AccountsUpdate(address indexed proposer, address indexed challenger);

    /// @notice Accepts new proposer and challenger to replace current proposer and challenger.
    ///         This function may only be called by the current Proposer.
    function updateProposerAndChallenger(address _proposer, address _challenger) public {
        require(msg.sender == proposer, "L2OutputOracle: only current proposer can update addresses.");
        proposer = _proposer;
        challenger = _challenger;

        emit AccountsUpdate(proposer, challenger);
    }
}
