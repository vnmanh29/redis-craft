//
// Created by Manh Nguyen Viet on 7/11/25.
//

#include "Database.h"


// Initialize static member outside the class
Database* Database::instance_ = nullptr;

Database *Database::GetInstance() {
    if (instance_ == nullptr)
    {
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
    if(on_exist == 1 && table_.find(key) == table_.end())
        return;
    table_[key] = {val, expired_ts};
}

std::string Database::RetrieveValueOfKey(const std::string& key) {
    std::lock_guard lock(m_);
    try {
        int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        auto p = table_.at(key);
        if (p.second > 0 && p.second < now)
        {
            return  "";
        }
        return p.first;
    }
    catch (std::exception& ex)
    {
        return "";
    }
}

int Database::SetConfig(const std::shared_ptr<RedisConfig> &cfg) {
    std::lock_guard lock(m_);
    rdb_cfg_ = cfg;
    return 0;
}

std::string Database::GetConfigFromName(const std::string &property) {
    std::lock_guard lock(m_);
    if (property == "dir")
    {
        return rdb_cfg_->dir_path;
    }
    else if (property == "dbfilename")
    {
        return rdb_cfg_->dbfilename;
    }
    else
    {
        return "";
    }
}


