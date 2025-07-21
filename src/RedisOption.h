//
// Created by Manh Nguyen Viet on 7/12/25.
//

#ifndef REDIS_STARTER_CPP_REDISOPTION_H
#define REDIS_STARTER_CPP_REDISOPTION_H

#include <string>

#define DEFAULT_MASTER_REPLID "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"

typedef struct RedisConfig
{
    std::string dir_path;
    std::string dbfilename;
    int port;

    int is_replica;
    std::string master_host;
    int master_port;

    RedisConfig() : port(6379), is_replica(0) {} // Default port is 6379
} RedisConfig;

typedef struct RedisOptionDef
{
    char* name;
    int (*func_arg)(RedisConfig *, const char *);
} RedisOptionDef;

extern const RedisOptionDef redis_options[];

const RedisOptionDef* find_redis_option(const RedisOptionDef* options, const char* arg);

#endif //REDIS_STARTER_CPP_REDISOPTION_H
