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

void Database::SetKeyVal(const std::string& key, const std::string& val) {
    std::lock_guard lock(m_);
    table_[key] = val;
}

std::string Database::RetrieveValueOfKey(const std::string& key) {
    std::lock_guard lock(m_);
    return table_[key];
}


