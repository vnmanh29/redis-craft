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

std::string Database::RetrieveValueOfKey(const std::string &key) {
    std::lock_guard lock(m_);
    try {
        int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
        auto p = table_.at(key);
        if (p->expire_time > 0 && p->expire_time < now) {
            LOG_INFO(TAG, "Key %s has expired at %lld, now %lld, removing it from the database.\n",
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
            // value->Debug();
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

bool Database::IsEqualConfig(const std::shared_ptr<RedisConfig> &cfg) const {
    if (!rdb_cfg_ && cfg) {
        return false;
    }

    return (rdb_cfg_->dbfilename == cfg->dbfilename
            && rdb_cfg_->dir_path == cfg->dir_path);
}

ssize_t Database::SaveRdbBackground(const std::string &file_name) {
    /// create pipe to write in child proc, read in parent proc
    Server::GetInstance()->OpenChildInfoPipe();

    pid_t child_pid;
    if ((child_pid = fork()) == 0) {
        /// child proc: save current db to rdb format

        /// close reading part in child proc
        close(Server::GetInstance()->GetChildInfoReadPipe());

        /// save the memory db to the local file
#if DEBUG
        char magic[10] = {0};
        /// version
        snprintf(magic,sizeof(magic),"REDIS%04d",version_);
        Server::SendData(fd, magic);
#else
        LOG_DEBUG(TAG, "Start save empty rdb");
        std::string hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

        std::string data = RdbHex2Bin(hex);
        std::vector<unsigned char> binaryData;
        hexToBinaryData(hex, binaryData);

        FILE *file = fopen(file_name.c_str(), "wb");

        fwrite(binaryData.data(), sizeof(unsigned char), binaryData.size(), file);

        fclose(file);
        LOG_DEBUG(TAG, "Finish save empty rdb")
#endif // DEBUG

        /// TODO: notify to parent end of child proc
        _exit(0);
    } else {
        /// parent proc
        if (child_pid == -1) {
            /// create fail
            Server::GetInstance()->CloseChildInfoPipe();
            return -1;
        }

        Server::GetInstance()->SetChildPid(child_pid);
        /// close unused writing part
        close(Server::GetInstance()->GetChildInfoWritePipe());
    }

    return 0;
}

std::string Database::GetRdbPath() const {
    if (rdb_cfg_)
    {
        std::string deliminator = (!rdb_cfg_->dir_path.empty() && rdb_cfg_->dir_path.back() == '/') ? "" : "/";
        return rdb_cfg_->dir_path + deliminator + rdb_cfg_->dbfilename;
    }

    return "";
}


