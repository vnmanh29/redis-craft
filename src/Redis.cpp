#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "all.hpp"
#include "CommandExecutor.h"
#include "RedisOption.h"
#include "Server.h"

static int redis_parse_options(int argc, char** argv, std::shared_ptr<RedisConfig> cfg)
{
    int idx = 1;
    while (idx < argc)
    {
        char* arg = argv[idx];
        if (arg[0] == '-' && arg[1] == '-' && (&arg[2]))
        {
            /// get the name of option, find this option and set the value
            std::string name = static_cast<std::string>(&arg[2]);
            if (idx + 1 >= argc)
            {
                printf("%s, %d\n", __func__, __LINE__);
                return -1;
            }
            
            const char* val = argv[++idx];
            const RedisOptionDef* opt = find_redis_option(redis_options, name.c_str());
            if (opt)
            {
                opt->func_arg(cfg.get(), val);
            }
            else
            {
                fprintf(stderr, "%s, %d, idx %d, name %s\n", __func__, __LINE__, idx, name.c_str());
                return -1;
            }
        }
        else
        {
            
            /// TODO: handle another format
            fprintf(stderr, "%s, %d, idx %d\n", __func__, __LINE__, idx);
            return -2;
        }

        ++idx;
    }

    return 0;
}

static void redis_set_global_config(const std::shared_ptr<RedisConfig>& cfg)
{
    Database::GetInstance()->SetConfig(cfg);

    Server::GetInstance()->SetConfig(cfg);
}

int main(int argc, char **argv) {
    // Flush after every std::cout / std::cerr
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    /// parse the argv
    std::shared_ptr<RedisConfig> cfg = std::make_shared<RedisConfig>();
    int ret = redis_parse_options(argc, argv, cfg);
    if (ret < 0)
    {
        fprintf(stderr, "Invalid parameters, total %d ret %d\n", argc, ret);
        for (int i = 0;i < argc; ++i)
        {
            fprintf(stderr, "arg %d: %s\n", i, argv[i]);
        }
        return -1;
    }

    redis_set_global_config(cfg);

    Server::GetInstance()->Start();

    return 0;
}

/**
 * TODO: 
 * - Implement the RESP parser
 * 
 */
