//
// Created by Manh Nguyen Viet on 7/11/25.
//

#include "Database.h"
#include "GlobMatcher.hpp"
#include "rdbparse.h"
#include "status.h"
#include "Server.h"

#include <filesystem>
#include <unistd.h>

// Initialize static member outside the class
Database *Database::instance_ = nullptr;
std::mutex Database::m_;

Database *Database::GetInstance() {
    std::lock_guard lock(m_);
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

    LOG_DEBUG(TAG, "Set key %s, val %s, expire_time %lld", key.c_str(), val.c_str(), expired_ts);
    table_[key] = std::make_shared<RdbParser::ParsedResult>(key, val, expired_ts);
}

int Database::XAdd(const VString &argv, RdbParser::EntryID &entry_id) {
    std::string stream_key = argv[1];

    if (table_.find(stream_key) == table_.end()) {
        auto stream = std::make_shared<RdbParser::ParsedResult>("stream");
        table_[stream_key] = stream;
    }

    int ret = table_[stream_key]->AddStream(argv, entry_id);
    if (ret < 0) {
        LOG_ERROR("Stream", "Add stream fail %d", ret);
        if (ret == -1) {
            return NonMonotonicEntryIdError;
        } else if (ret == -2) {
            return InvalidXaddEntryIdError;
        }
        return UnknownError;
    }

    return 0;
}

std::string Database::RetrieveValueOfKey(const std::string &key) {
    std::lock_guard lock(m_);
    try {
        int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
        auto p = table_.at(key);
        if (p->expire_time > 0 && p->expire_time < now) {
            LOG_INFO(TAG, "Key %s has expired at %lld, now %lld, removing it from the database",
                     key.c_str(), p->expire_time, now);
            return "";
        }

        LOG_DEBUG(TAG, "Key %s has expired at %lld, now %lld", key.c_str(), p->expire_time, now);
        return p->kv_value;
    }
    catch (std::exception &ex) {
        return "";
    }
}

int Database::SetConfig(RedisConfig *cfg) {
    rdb_cfg_ = cfg;

    /// Onload new config
    std::string rdb_file_path = GetRdbPath();
    std::filesystem::directory_entry file(rdb_file_path);
    if (!file.exists() || !file.is_regular_file()) {
        // blank database 
        LOG_INFO(TAG, "RDB file %s does not exist, creating a new empty database.", rdb_file_path.c_str());
        table_.clear();
    } else {
        /// read rdb file + load data to the memory
        RdbParser::RdbParse *parse;
        RdbParser::Status s = RdbParser::RdbParse::Open(rdb_file_path, &parse);
        if (!s.ok()) {
            std::cout << s.ToString() << std::endl;
            return 1;
        }

        version_ = parse->GetVersion();

        while (parse->Valid()) {
            s = parse->Next();
            if (!s.ok()) {
//                std::cout << "Failed:" << s.ToString() << std::endl;
                LOG_ERROR(TAG, "Failed: %s", s.ToString().c_str());
                break;
            }
            RdbParser::ParsedResult *value = parse->Value();
            std::string key = value->key;
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

bool Database::IsKeyExist(const std::string &key) {
    std::lock_guard lock(m_);
    return table_.find(key) != table_.end();
}

std::string Database::GetKeyType(const std::string &key) {
    try {
        auto &val = table_.at(key);
        return val->type;
    }
    catch (const std::exception &e) {
        LOG_ERROR("DB", "Not found key %s, ex %s", key.c_str(), e.what());
        return "none";
    }
}

bool Database::IsEqualConfig(const std::shared_ptr<RedisConfig> &cfg) const {
    if (!rdb_cfg_ && cfg) {
        return false;
    }

    return (rdb_cfg_->dbfilename == cfg->dbfilename
            && rdb_cfg_->dir_path == cfg->dir_path);
}

std::string Database::GetRdbPath() const {
    if (rdb_cfg_) {
        std::string deliminator = (!rdb_cfg_->dir_path.empty() && rdb_cfg_->dir_path.back() == '/') ? "" : "/";
        return rdb_cfg_->dir_path + deliminator + rdb_cfg_->dbfilename;
    }

    return "";
}

int Database::Reset() {
    table_.clear();
    return 0;
}

int Database::LoadPersistentDb() {

    /// 1. open rdb file
    std::string rdb_file = GetRdbPath();
    if (rdb_file.empty()) {
        LOG_ERROR("DB", "Empty rdb file path");
        return -1;
    }

    /// 2. reset current db
    Reset();

    /// 3. TODO: load data to memory
    return 0;
}

std::vector<std::pair<RdbParser::EntryID, RdbParser::EntryStream>>
Database::GetStreamRange(const std::string &stream_key, const std::string &start_id, const std::string &end_id) {
    std::vector<std::pair<RdbParser::EntryID, RdbParser::EntryStream>> stream_range;

    try {
        auto &stream_entry = table_.at(stream_key)->stream;
        /// normalize start id
        RdbParser::EntryID start = RdbParser::BuildEntryId(start_id, 0);
        RdbParser::EntryID end = RdbParser::BuildEntryId(end_id, INT64_MAX);

        auto start_it = stream_entry.lower_bound(start);
        auto end_it = stream_entry.upper_bound(end);
        for (auto it = start_it; it != end_it && it != stream_entry.end(); ++it) {
            stream_range.emplace_back(it->first, it->second);
        }

        return stream_range;
    }
    catch (std::exception &ex) {
        return stream_range;
    }
}
