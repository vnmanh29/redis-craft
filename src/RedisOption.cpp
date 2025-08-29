//
// Created by Manh Nguyen Viet on 7/12/25.
//

#include "RedisOption.h"

#include <cstring>
#include <sstream>
#include <iostream>

extern RedisConfig *globale_cfg;

static int opt_dir(RedisConfig *redis_cfg, const char *arg) {
    if (redis_cfg) {
        redis_cfg->dir_path = arg;
        return 0;
    }

    return -1;
}

static int opt_dbfilename(RedisConfig *redis_cfg, const char *arg) {
    if (redis_cfg) {
        redis_cfg->dbfilename = arg;
        return 0;
    }

    return -1;
}

static int opt_port(RedisConfig *redis_cfg, const char *arg) {
    if (redis_cfg) {
        try {
            redis_cfg->port = std::stoi(arg);
            server_port = redis_cfg->port;
            return 0;
        }
        catch (const std::exception &e) {
            return -1; // Invalid port number
        }
    }

    return -1;
}

static int opt_replicaof(RedisConfig *redis_cfg, const char *arg) {
    if (redis_cfg) {
        try {
            std::stringstream ss(static_cast<std::string>(arg));

            ss >> redis_cfg->master_host >> redis_cfg->master_port;

            redis_cfg->is_replica = 1;

            return 0;
        }
        catch (std::exception &ex) {
            std::cerr << "Invalid arguments of option replicaof" << std::endl;
            return -1; /// invalid argument
        }
    }

    return -1;
}

const RedisOptionDef redis_options[] =
        {
                {"dir",        opt_dir},
                {"dbfilename", opt_dbfilename},
                {"port",       opt_port},
                {"replicaof",  opt_replicaof},
                {nullptr}
        };

const RedisOptionDef *find_redis_option(const RedisOptionDef *options, const char *arg) {
    if (!options || !arg)
        return nullptr;

    while (options->name) {
        if (strcmp(options->name, arg) == 0)
            break;
        ++options;
    }

    return options;
}

std::string get_rdb_file_path() {
    if (globale_cfg) {
        std::string deliminator = (!globale_cfg->dir_path.empty() && globale_cfg->dir_path.back() == '/') ? "" : "/";
        return globale_cfg->dir_path + deliminator + globale_cfg->dbfilename;
    }

    return "";
}