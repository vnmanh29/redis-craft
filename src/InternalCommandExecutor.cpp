//
// Created by Manh Nguyen Viet on 7/11/25.
//

#include "InternalCommandExecutor.h"

std::shared_ptr<AbstractInternalCommandExecutor>
AbstractInternalCommandExecutor::createCommandExecutor(const CommandType cmd_type) {
    switch (cmd_type) {
        case EchoCmd:
            return std::make_shared<EchoCommandExecutor>();
        case GetCmd:
            return std::make_shared<GetCommandExecutor>();
        case SetCmd:
            return std::make_shared<SetCommandExecutor>();
        case PingCmd:
            return std::make_shared<PingCommandExecutor>();
        case ConfigGetCmd:
            return std::make_shared<GetConfigCommandExecutor>();
        case KeysCmd:
            return std::make_shared<KeysCommandExecutor>();
        case InfoCmd:
            return std::make_shared<InfoCommandExecutor>();
        default:
            std::cerr << "Unknown command type: " << cmd_type << std::endl;
            break;
    }

    return nullptr;
}