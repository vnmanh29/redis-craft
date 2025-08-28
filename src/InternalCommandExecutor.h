//
// Created by Manh Nguyen Viet on 7/11/25.
//

#ifndef REDIS_STARTER_CPP_INTERNALCOMMANDEXECUTOR_H
#define REDIS_STARTER_CPP_INTERNALCOMMANDEXECUTOR_H

#include <memory>
#include <chrono>
#include <iostream>

#include "all.hpp"

#include "Utils.h"
#include "Database.h"

class Client;

class AbstractInternalCommandExecutor : public std::enable_shared_from_this<AbstractInternalCommandExecutor> {
private:
    int flags_;

public:
    AbstractInternalCommandExecutor() : flags_(0) {};

    virtual ~AbstractInternalCommandExecutor() = default;

    virtual void execute(const Query &query, std::shared_ptr<Client> client) = 0;

    static std::shared_ptr<AbstractInternalCommandExecutor> createCommandExecutor(CommandType cmd_type);

};

#endif //REDIS_STARTER_CPP_INTERNALCOMMANDEXECUTOR_H
