#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "all.hpp"
#include "CommandExecutor.h"
#include "RedisOption.h"
#include "Server.h"
#include "RedisDef.h"

LogLevel global_log_level = LogLevel::Silent;
RedisConfig *globale_cfg = nullptr;

const char TAG[] = "RDB";

static int redis_parse_options(int argc, char **argv, RedisConfig *cfg) {
    int idx = 1;
    while (idx < argc) {
        char *arg = argv[idx];
        if (arg[0] == '-' && arg[1] == '-' && (&arg[2])) {
            /// get the name of option, find this option and set the value
            std::string name = static_cast<std::string>(&arg[2]);
            if (idx + 1 >= argc) {
                LOG_ERROR(TAG, "parse option failed");
                return -1;
            }

            const char *val = argv[++idx];
            const RedisOptionDef *opt = find_redis_option(redis_options, name.c_str());
            if (opt) {
                opt->func_arg(cfg, val);
            } else {
                LOG_ERROR(TAG, "%s, %d, idx %d, name %s\n", __func__, __LINE__, idx, name.c_str());
                return -1;
            }
        } else {

            /// TODO: handle another format
            LOG_ERROR(TAG, "%s, %d, idx %d\n", __func__, __LINE__, idx);
            return -2;
        }

        ++idx;
    }

    return 0;
}

static void redis_set_global_config() {
    Database::GetInstance()->SetConfig(globale_cfg);

    Server::GetInstance()->SetConfig(globale_cfg);
}

static void set_log_level(LogLevel lvl) {
    global_log_level = lvl;
}

int main(int argc, char **argv) {
    // Flush after every std::cout / std::cerr
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    set_log_level(LogLevel::Info);

    /// parse the argv
    globale_cfg = new RedisConfig();
    int ret = redis_parse_options(argc, argv, globale_cfg);
    if (ret < 0) {
        LOG_ERROR(TAG, "Invalid parameters, total %d ret %d", argc, ret);
        for (int i = 0; i < argc; ++i) {
            LOG_ERROR(TAG, "arg %d: %s\n", i, argv[i]);
        }
        return -1;
    }

    redis_set_global_config();

    Server::GetInstance()->Setup();

    if (globale_cfg->is_replica) {
        Server::GetInstance()->StartReplica();
    } else {
        Server::GetInstance()->StartMaster();
    }

    return 0;
}

/**
 * TODO: 
 * - Implement the RESP parser
 * 
 */
