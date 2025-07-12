//
// Created by Manh Nguyen Viet on 7/12/25.
//

#ifndef REDIS_STARTER_CPP_REDISOPTION_H
#define REDIS_STARTER_CPP_REDISOPTION_H

#include <string>

typedef struct RedisConfig
{
    std::string dir_path;
    std::string dbfilename;
} RedisConfig;

typedef struct RedisOptionDef
{
    char* name;
    int (*func_arg)(RedisConfig *, const char *);
} RedisOptionDef;

extern const RedisOptionDef redis_options[];

const RedisOptionDef* find_redis_option(const RedisOptionDef* options, const char* arg);

#endif //REDIS_STARTER_CPP_REDISOPTION_H
