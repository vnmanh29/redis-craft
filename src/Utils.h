//
// Created by Manh Nguyen Viet on 7/11/25.
//

#ifndef REDIS_STARTER_CPP_UTILS_H
#define REDIS_STARTER_CPP_UTILS_H

#include "all.hpp"

enum CommandType
{
    EchoCmd = 0,
    GetCmd,
    SetCmd,
    PingCmd,
    GetConfigCmd,
    SetConfigCmd,
    UnknownCmd
};

typedef struct Query
{
    CommandType cmd_type;
    std::vector<std::string> cmd_args;
} Query;

#endif //REDIS_STARTER_CPP_UTILS_H
