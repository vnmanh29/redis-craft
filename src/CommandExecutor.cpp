//
// Created by Manh Nguyen Viet on 7/10/25.
//

#include "CommandExecutor.h"
#include "RedisError.h"
#include "Server.h"

int CommandExecutor::ReceiveData(const std::string &buffer) {
    /// append new data to the last. Useful in case that remain data in the previous request
    data_ = data_ + buffer;
    resp::result res = decoder_.decode(data_.c_str(), data_.size());
    if (res == resp::incompleted) {
        LOG_DEBUG(TAG, "Received buffer %s. Incompleted RESP with data %s\n", buffer.c_str(), data_.c_str())
        return IncompletedCommand;
    }

    if (res == resp::error) {
        fprintf(stderr, "Received buffer %s. Invalid RESP with data %s\n", buffer.c_str(), data_.c_str());
        return Error::InvalidCommandError;
    }

    /// clear previous data
    data_.clear();
    ResetQuery(query_);

    resp::unique_value rep = res.value();

    /// create query
    resp::unique_array<resp::unique_value> arr = rep.array();
    for (int i = 0; i < arr.size(); ++i) {
        query_.cmd_args.emplace_back(arr[i].bulkstr().data(), arr[i].bulkstr().size());
    }

    std::string cmd = query_.cmd_args.front();
//    std::cout << "receive cmd " << cmd << std::endl;
    std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);
    if (std::strcmp(cmd.data(), "ECHO") == 0) {
        query_.cmd_type = EchoCmd;
    } else if (std::strcmp(cmd.data(), "GET") == 0) {
        query_.cmd_type = GetCmd;
    } else if (std::strcmp(cmd.data(), "SET") == 0) {
        query_.cmd_type = SetCmd;
    } else if (std::strcmp(cmd.data(), "PING") == 0) {
        query_.cmd_type = PingCmd;
    } else if (std::strcmp(cmd.data(), "CONFIG") == 0) {
        std::string sub_cmd = query_.cmd_args.size() > 1 ? query_.cmd_args[1] : "";
        std::transform(sub_cmd.begin(), sub_cmd.end(), sub_cmd.begin(), ::toupper);
        if (std::strcmp(sub_cmd.data(), "GET") == 0) {
            query_.cmd_type = ConfigGetCmd;
        } else if (std::strcmp(sub_cmd.data(), "SET") == 0) {
            query_.cmd_type = ConfigSetCmd;
        } else {
            query_.cmd_type = UnknownCmd;
        }
    } else if (std::strcmp(cmd.data(), "KEYS") == 0) {
        query_.cmd_type = KeysCmd;
    } else if (std::strcmp(cmd.data(), "INFO") == 0) {
        query_.cmd_type = InfoCmd;
    } else if (std::strcmp(cmd.data(), "REPLCONF") == 0) {
        query_.cmd_type = ReplcofCmd;
    } else if (std::strcmp(cmd.data(), "PSYNC") == 0) {
        query_.cmd_type = PSyncCmd;
    } else {
        query_.cmd_type = UnknownCmd;
    }

    return 0;
}

ssize_t CommandExecutor::Execute(std::shared_ptr<Client> client) {
    /// create the executor suit for the current command
    internal_executor_ = AbstractInternalCommandExecutor::createCommandExecutor(query_.cmd_type);
    if (!internal_executor_)
        return InvalidCommandError;
//        return "+OK\r\n";

    internal_executor_->SetSocket(client->fd);

    /// execute command
    return internal_executor_->execute(query_, client);
}