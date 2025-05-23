// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#include "iggy.h"

icp::net::IggyProtocolProvider::IggyProtocolProvider() {
    for (const auto& protocol : this->supportedProtocols) {
        this->supportedProtocolLookup[protocol.getName()] = protocol;
    }
}

const std::vector<icp::net::protocol::ProtocolDefinition>& icp::net::IggyProtocolProvider::getSupportedProtocols() const {
    return this->supportedProtocols;
}

const icp::net::protocol::ProtocolDefinition& icp::net::IggyProtocolProvider::getProtocolDefinition(const std::string& protocol) const {
    auto normalizedName = icp::net::protocol::normalizeProtocolName(protocol);
    auto it = this->supportedProtocolLookup.find(normalizedName);
    if (it != this->supportedProtocolLookup.end()) {
        return it->second;
    } else {
        throw std::invalid_argument("Unsupported protocol: " + protocol);
    }
}

const bool icp::net::IggyProtocolProvider::isSupported(const std::string& protocol) const {
    return this->supportedProtocolLookup.count(icp::net::protocol::normalizeProtocolName(protocol)) > 0;
}
