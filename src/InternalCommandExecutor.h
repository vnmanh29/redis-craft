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

typedef struct Client Client;

class AbstractInternalCommandExecutor : public std::enable_shared_from_this<AbstractInternalCommandExecutor>
{
private:
    int fd_;

public:
    AbstractInternalCommandExecutor() : fd_(-1) {};

    virtual ~AbstractInternalCommandExecutor() = default;

    virtual ssize_t execute(const Query &query, std::shared_ptr<Client> client) = 0;

    static std::shared_ptr<AbstractInternalCommandExecutor> createCommandExecutor(CommandType cmd_type);

    inline void SetSocket(const int fd)
    {
        fd_ = fd;
    }

    inline int GetSocket() const {
        return fd_;
    }

};

#endif //REDIS_STARTER_CPP_INTERNALCOMMANDEXECUTOR_H
