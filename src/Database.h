//
// Created by Manh Nguyen Viet on 7/11/25.
//

#ifndef REDIS_STARTER_CPP_DATABASE_H
#define REDIS_STARTER_CPP_DATABASE_H

#include <unordered_map>
#include <mutex>

#include "all.hpp"

class Database {
private:

    static Database* instance_;
    Database() = default;

    std::unordered_map<std::string, std::string> table_;
    std::mutex m_;

public:
    Database& operator=(const Database& rhs) = delete;
    Database(const Database& rhs) = delete;

    static Database* GetInstance();

    void SetKeyVal(const std::string& key, const std::string& val);

    std::string RetrieveValueOfKey(const std::string& key);
};


#endif //REDIS_STARTER_CPP_DATABASE_H
