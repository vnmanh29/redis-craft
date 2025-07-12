//
// Created by Manh Nguyen Viet on 7/11/25.
//

#ifndef REDIS_STARTER_CPP_DATABASE_H
#define REDIS_STARTER_CPP_DATABASE_H

#include <unordered_map>
#include <mutex>
#include <memory>

#include "all.hpp"
#include "RedisOption.h"

class Database {
private:

    static Database* instance_;
    Database() = default;

    std::unordered_map<std::string, std::pair<std::string, int64_t>> table_;
    std::mutex m_;

    std::shared_ptr<RedisConfig> rdb_cfg_;

public:
    Database& operator=(const Database& rhs) = delete;
    Database(const Database& rhs) = delete;

    static Database* GetInstance();

    int SetConfig(const std::shared_ptr<RedisConfig>& cfg);

    std::string GetConfigFromName(const std::string& property);

    void SetKeyVal(const std::string &key, const std::string &val, int on_exist, int64_t expired_ts);

    std::string RetrieveValueOfKey(const std::string& key);
};


#endif //REDIS_STARTER_CPP_DATABASE_H
