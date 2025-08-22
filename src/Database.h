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
#include "rdbparse.h"

class Database {
private:

    static Database *instance_;

    Database() = default;

    std::unordered_map<std::string, std::shared_ptr<RdbParser::ParsedResult>> table_;
    int version_;
    static std::mutex m_;

    std::shared_ptr<RedisConfig> rdb_cfg_;

private:
    bool IsEqualConfig(const std::shared_ptr<RedisConfig> &cfg) const;

public:
    Database &operator=(const Database &rhs) = delete;

    Database(const Database &rhs) = delete;

    static Database *GetInstance();

    int Reset();

    int SetConfig(const std::shared_ptr<RedisConfig> &cfg);

    std::string GetConfigFromName(const std::string &property);

    void SetKeyVal(const std::string &key, const std::string &val, int on_exist, int64_t expired_ts);

    std::string RetrieveValueOfKey(const std::string &key);

    std::vector<std::string> RetrieveKeysMatchPattern(const std::string &pattern);

    ssize_t SaveRdbBackground(const std::string &file_name);

    [[nodiscard]] std::string GetRdbPath() const;
};


#endif //REDIS_STARTER_CPP_DATABASE_H
