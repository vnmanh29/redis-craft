//
// Created by Manh Nguyen Viet on 7/11/25.
//

#ifndef REDIS_STARTER_CPP_UTILS_H
#define REDIS_STARTER_CPP_UTILS_H

#include "all.hpp"
#include <sys/stat.h>
#include <unordered_map>

#define CRLF "\r\n"
#define DEFAULT_REDIS_PORT 6379

#define DEFAULT_MASTER_REPLID "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
#define MASTER_ID_LENGTH 40

#define CMD_WRITE (1<<1)
#define CMD_READ  (1<<2)

enum CommandType {
    EchoCmd = 0,
    GetCmd,
    SetCmd,
    PingCmd,
    ConfigGetCmd,
    ConfigSetCmd,
    KeysCmd,
    InfoCmd,
    ReplconfCmd,
    PSyncCmd,
    FullresyncCmd,
    UnknownCmd
};

typedef struct RedisCmd {
    CommandType cmd_type;
    uint64_t flags;
    std::unordered_map<std::string, RedisCmd *> subcmd_dict;

    explicit RedisCmd(CommandType type, uint64_t flag) : cmd_type(type), flags(flag) {}
} RedisCmd;

typedef struct Query {
    RedisCmd *cmd;    /// point to the global cmd
    uint64_t flags;   /// flag of cmd, like CMD_READ, CMD_WRITE, etc ...
    std::vector<std::string> cmd_args;      /// the list argv for execution
} Query;

/// input: array of strings. Output: a string presents RESP Array
std::string EncodeArr2RespArr(std::vector<std::string> arr);

std::string EncodeRespSimpleStr(std::string s);

void ResetQuery(Query &query);

int RdbStat(const std::string &file_name, struct stat &st);

std::string RdbHex2Bin(const std::string &hex);

std::string HexToBinary(const std::string &hexStr);

void hexToBinaryData(const std::string &hexStr, std::vector<unsigned char> &binaryData);

#endif //REDIS_STARTER_CPP_UTILS_H
