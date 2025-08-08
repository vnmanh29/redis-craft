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

    /// receive and decode. return 0 with completed command, otherwise return the error in RedisError.h
    int ReceiveData(const std::string& buffer);

    ssize_t Execute(std::shared_ptr<Client> client);

private:
    /// private method

private:
    resp::encoder<std::string> encoder_;
    resp::decoder decoder_;
    std::string data_;

    Query query_;
    std::shared_ptr<AbstractInternalCommandExecutor> internal_executor_;
};


#endif //REDIS_STARTER_CPP_COMMANDEXECUTOR_H
