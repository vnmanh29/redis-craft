//
// Created by Manh Nguyen Viet on 7/11/25.
//

#ifndef REDIS_STARTER_CPP_UTILS_H
#define REDIS_STARTER_CPP_UTILS_H

#include "all.hpp"

#define CRLF "\r\n"
#define DEFAULT_REDIS_PORT 6379

#define DEFAULT_MASTER_REPLID "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"

enum CommandType
{
    EchoCmd = 0,
    GetCmd,
    SetCmd,
    PingCmd,
    ConfigGetCmd,
    ConfigSetCmd,
    KeysCmd,
    InfoCmd,
    ReplcofCmd,
    PSyncCmd,
    UnknownCmd
};

typedef struct Query
{
    CommandType cmd_type;
    std::vector<std::string> cmd_args;
} Query;

/// input: array of strings. Output: a string presents RESP Array
std::string EncodeArr2RespArr(std::vector<std::string>& arr);

std::string EncodeRespSimpleStr(std::string s);

#endif //REDIS_STARTER_CPP_UTILS_H
