//
// Created by Manh Nguyen Viet on 7/10/25.
//

#ifndef REDIS_STARTER_CPP_COMMANDEXECUTOR_H
#define REDIS_STARTER_CPP_COMMANDEXECUTOR_H

#include <string>
#include <memory>

#include "all.hpp"
#include "InternalCommandExecutor.h"
#include "Utils.h"

/*
 * Receive the command, execute and return the response
 * */


class CommandExecutor {
public:
    CommandExecutor() = default;

    ~CommandExecutor() = default;

    /// append available data to buffer.
    /// One by one, try to decode a command from buffer, execute it utils could not decode new command
    int ReceiveDataAndExecute(const std::string &buffer, std::shared_ptr<Client> client);

private:
    /// private method
    int BuildRedisCommand(const resp::unique_value &rep);

    int BuildExecutor();

private:
    resp::encoder<std::string> encoder_;
    resp::decoder decoder_;
    std::string data_;

    Query query_;
    std::shared_ptr<AbstractInternalCommandExecutor> internal_executor_;
};


#endif //REDIS_STARTER_CPP_COMMANDEXECUTOR_H
