//
// Created by Manh Nguyen Viet on 7/10/25.
//

#include "CommandExecutor.h"

void CommandExecutor::ReceiveRequest(const std::string &request) {
    resp::result res = decoder_.decode(request.c_str(), request.size());
    resp::unique_value rep = res.value();

    /// create query
    resp::unique_array<resp::unique_value> arr = rep.array();
    for (int i = 0;i < arr.size(); ++i)
    {
        query_.cmd_args.emplace_back(arr[i].bulkstr().data(), arr[i].bulkstr().size());
    }

    std::string cmd = query_.cmd_args.front();
    std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);
    if (std::strcmp(cmd.data(), "ECHO") == 0)
    {
        query_.cmd_type = EchoCmd;
    }
    else if (std::strcmp(cmd.data(), "GET") == 0)
    {
        query_.cmd_type = GetCmd;
    }
    else if (std::strcmp(cmd.data(), "SET") == 0)
    {
        query_.cmd_type = SetCmd;
    }
    else if (std::strcmp(cmd.data(), "PING") == 0)
    {
        query_.cmd_type = PingCmd;
    }
    else
    {
        query_.cmd_type = UnknownCmd;
    }
}

std::string CommandExecutor::Execute() {
    /// create the executor suit for the current command
    internal_executor_ = AbstractInternalCommandExecutor::createCommandExecutor(query_.cmd_type);
    if (!internal_executor_)
        return "+OK\r\n";

    /// execute command
    return internal_executor_->execute(query_);
}