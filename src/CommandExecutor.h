//
// Created by Manh Nguyen Viet on 7/10/25.
//

#ifndef REDIS_STARTER_CPP_COMMANDEXECUTOR_H
#define REDIS_STARTER_CPP_COMMANDEXECUTOR_H

#include <string>
#include <memory>

#include "all.hpp"
#include "InternalCommandExecutor.h"

/*
 * Receive the command, execute and return the response
 * */

#include "Utils.h"
#include "InternalCommandExecutor.h"

class CommandExecutor {
public:
    CommandExecutor() = default;

    ~CommandExecutor() = default;

    /// receive and decode
    void ReceiveRequest(const std::string& request);

    std::string Execute();

private:
    /// private method

private:
    resp::encoder<std::string> encoder_;
    resp::decoder decoder_;

    Query query_;
    std::shared_ptr<AbstractInternalCommandExecutor> internal_executor_;
};


#endif //REDIS_STARTER_CPP_COMMANDEXECUTOR_H
