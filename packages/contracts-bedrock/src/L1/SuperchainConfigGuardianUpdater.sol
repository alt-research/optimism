// SPDX-License-Identifier: MIT
pragma solidity 0.8.15;

import { SuperchainConfig } from "src/L1/SuperchainConfig.sol";

/// @custom:audit none This contract is not yet audited.
/// @title SuperchainConfigGuardianUpdater
/// @notice The SuperchainConfigGuardianUpdater contract is a patch contract to update the guardian for SuperchainConfig.
/// @dev Inherits from SuperchainConfig and provides functionality to update the guardian address.
/// @dev The setGuardian function is marked as external to allow calls from other contracts.
contract SuperchainConfigGuardianUpdater is SuperchainConfig {

    /// @notice Sets the guardian address. Only allows the current guardian to update to a new one.
    /// @param _guardian The new guardian address.
    function setGuardian(address _guardian) external {
        require(msg.sender == guardian(), "Not the guardian");
        super._setGuardian(_guardian);
    }
}
