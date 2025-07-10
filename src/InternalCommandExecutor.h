//
// Created by Manh Nguyen Viet on 7/11/25.
//

#ifndef REDIS_STARTER_CPP_INTERNALCOMMANDEXECUTOR_H
#define REDIS_STARTER_CPP_INTERNALCOMMANDEXECUTOR_H

#include <memory>

#include "all.hpp"
//#include "CommandExecutor.h"
#include "Utils.h"
#include "Database.h"

class AbstractInternalCommandExecutor : public std::enable_shared_from_this<AbstractInternalCommandExecutor>
{
public:
    AbstractInternalCommandExecutor() = default;

    virtual ~AbstractInternalCommandExecutor() = default;

    virtual std::string execute(const Query& query) = 0;

    static std::shared_ptr<AbstractInternalCommandExecutor> createCommandExecutor(CommandType cmd_type);
};

class EchoCommandExecutor : public AbstractInternalCommandExecutor
{
    std::string execute(const Query& query) override
    {
        if (query.cmd_args.size() < 2)
            return {};

        resp::encoder<std::string> enc;
        return enc.encode_bulk_str(query.cmd_args[1], query.cmd_args[1].size());
    }
};

class GetCommandExecutor : public AbstractInternalCommandExecutor
{
    std::string execute(const Query& query) override
    {
        if (query.cmd_args.size() < 2)
            return "";

        std::string resp = Database::GetInstance()->RetrieveValueOfKey(query.cmd_args[1]);

        if (resp.empty())
        {
            return "$-1\r\n"; // RESP format for nil
        }
        else
        {
            resp::encoder<std::string> encoder;
            auto s = encoder.encode_bulk_str(resp, resp.size());
            printf("Get key %s, val %s\n", query.cmd_args[1].c_str(), s.c_str());
            return s;
        }
    }
};

class SetCommandExecutor : public AbstractInternalCommandExecutor
{
    std::string execute(const Query& query) override
    {
        if (query.cmd_args.size() < 3)
            return {};
        auto key = query.cmd_args[1];
        auto val = query.cmd_args[2];

        Database::GetInstance()->SetKeyVal(key, val);

        return "+OK\r\n";
    }
};

class PingCommandExecutor : public AbstractInternalCommandExecutor
{
    std::string execute(const Query& query) override
    {
        if (query.cmd_args.size() < 1)
            return {};

        return "+PONG\r\n";
    }
};

#endif //REDIS_STARTER_CPP_INTERNALCOMMANDEXECUTOR_H
