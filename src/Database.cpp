//
// Created by Manh Nguyen Viet on 7/11/25.
//

#include "Database.h"
#include "GlobMatcher.hpp"
#include "rdbparse.h"
#include "status.h"

#include <filesystem>

// Initialize static member outside the class
Database *Database::instance_ = nullptr;

Database *Database::GetInstance() {
    if (instance_ == nullptr) {
        instance_ = new Database();
    }

    return instance_;
}

void Database::SetKeyVal(const std::string &key, const std::string &val, int on_exist, int64_t expired_ts) {
    std::lock_guard lock(m_);
    /// return when require the key exist before but actually not
    if (on_exist == 0 && table_.find(key) != table_.end())
        return;

    /// return when require the key not exist before but actually yes
    if (on_exist == 1 && table_.find(key) == table_.end())
        return;
    
    printf("Set key %s, val %s, expire_time %lld\n", key.c_str(), val.c_str(), expired_ts);
    table_[key] = std::make_shared<RdbParser::ParsedResult>(key, val, expired_ts);
}

std::string Database::RetrieveValueOfKey(const std::string &key) {
    std::lock_guard lock(m_);
    try {
        int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
        auto p = table_.at(key);
        if (p->expire_time > 0 && p->expire_time < now) {
            printf("Key %s has expired at %lld, now %lld, removing it from the database.\n", 
                            key.c_str(), p->expire_time, now);
            return "";
        }
        return p->kv_value;
    }
    catch (std::exception &ex) {
        return "";
    }
}

int Database::SetConfig(const std::shared_ptr<RedisConfig> &cfg) {
    std::lock_guard lock(m_);
    if (IsEqualConfig(cfg))
        return 1;

    rdb_cfg_ = cfg;

    /// Onload new config
    std::string rdb_file_path = rdb_cfg_->dir_path + "/" + rdb_cfg_->dbfilename;
    std::filesystem::directory_entry file(rdb_file_path);
    if (!file.exists() || !file.is_regular_file()) {
        // blank database 
        printf("RDB file %s does not exist, creating a new empty database.\n", rdb_file_path.c_str());
        table_.clear();
    }
    else {
        /// read rdb file + load data to the memory
        RdbParser::RdbParse *parse;
        RdbParser::Status s = RdbParser::RdbParse::Open(rdb_file_path, &parse);
        if (!s.ok()) {
            std::cout << s.ToString() << std::endl;
            return 1;
        }
        while (parse->Valid()) {
            s = parse->Next();
            if (!s.ok()) {
                std::cout << "Failed:" << s.ToString() << std::endl;
                break;
            }
            RdbParser::ParsedResult *value = parse->Value();
            // value->Debug();
            std::string key = value->key;
            // printf("\nresult [%p], key %s\n", value, key.c_str());
            if (key.empty()) {
                continue; // skip empty keys
            }

            table_[key] = RdbParser::ResultMove(value);
        }
        delete parse;

    }
    return 0;
}

std::string Database::GetConfigFromName(const std::string &property) {
    std::lock_guard lock(m_);
    if (property == "dir") {
        return rdb_cfg_->dir_path;
    } else if (property == "dbfilename") {
        return rdb_cfg_->dbfilename;
    } else {
        return "";
    }
}

std::vector<std::string> Database::RetrieveKeysMatchPattern(const std::string &pattern) {
    std::vector<std::string> matched_keys;

    std::lock_guard lock(m_);
    for (auto &p: table_) {
        std::string key = p.first;
        if (matchGlob(key, pattern)) {
            matched_keys.push_back(key);
        }
    }

    return matched_keys;
}

bool Database::IsEqualConfig(const std::shared_ptr<RedisConfig> &cfg) const {
    if (!rdb_cfg_ && cfg) {
        return false;
    }

    return (rdb_cfg_->dbfilename == cfg->dbfilename
            && rdb_cfg_->dir_path == cfg->dir_path);
}


