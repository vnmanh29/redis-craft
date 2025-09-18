#ifndef __RDBPARSE_H__
#define __RDBPARSE_H__

#include <string>
#include <map>
#include <list>
#include <set>
#include <memory>
#include <vector>
#include <iostream>

#include "status.h"
#include "slice.h"

#define ROOT_ENTRY_ID "0-0"

namespace RdbParser {

    struct AuxKV {
        std::string aux_key;
        std::string aux_val;
    };

    typedef struct EntryID {
        int64_t timestamp;
        int64_t sequence_number;
    } EntryID;

    typedef struct EntryCmp {
        bool operator()(const EntryID &e1, const EntryID &e2) const {
            if (e1.timestamp == e2.timestamp) {
                return e1.sequence_number < e2.sequence_number;
            }

            return e1.timestamp < e2.timestamp;
        }
    } EntryCmp;

    inline EntryID BuildEntryId(const std::string &id, int64_t default_sequence_number = 0) {
        /// handle special case: "-" and "+"
        if (id == "-") {
            return {0, 0};
        }
        else if (id == "+") {
            return {INT64_MAX, INT64_MAX};
        }

        auto pos = id.find('-');
        EntryID entry_id{-1, -1};
        try {
            if (pos != std::string::npos) {
                /// format: <timestamp>-<sequence_number>
                auto time_part = id.substr(0, pos);
                auto number_part = id.substr(pos + 1);

                entry_id.timestamp = std::stoll(time_part);
                entry_id.sequence_number = std::stoll(number_part);
            }
            else {
                /// format: <timestamp>
                entry_id.timestamp = std::stoll(id);
                entry_id.sequence_number = default_sequence_number;

            }

            return entry_id;
        }
        catch (std::exception& ex) {
            std::cerr << "Build entry " << id << " fail " << ex.what() << std::endl;
            return {-1, -1};
        }
    }

    using KeyValStr = std::pair<std::string, std::string>;
    typedef std::vector<std::string> EntryStream;

    struct ParsedResult {
        ParsedResult() : expire_time(-1) {}

        ParsedResult(const std::string &k, const std::string &v, int64_t expire_ts = -1) :
                key(k), kv_value(v), expire_time(expire_ts), type("string") {}

        ParsedResult(const std::string &data_type, int64_t expire_ts = -1) :
                expire_time(expire_ts), type(data_type) {}


        std::string type;
        uint32_t db_num;
        uint32_t idle;
        uint32_t db_size;
        uint32_t expire_size;
        uint32_t freq;
        AuxKV aux_field;
        int64_t expire_time;

        void set_dbnum(uint32_t _db_num) {
            db_num = _db_num;
        }

        void set_idle(uint32_t _idle) {
            idle = _idle;
        }

        void set_dbsize(uint32_t _db_size) {
            db_size = _db_size;
        }

        void set_expiresize(uint32_t _exire_size) {
            expire_size = _exire_size;
        }

        void set_expiretime(int64_t _expire_time) {
            expire_time = _expire_time;
        }

        void set_freq(uint32_t _freq) {
            freq = _freq;
        }

        void set_auxkv(const std::string &key, const std::string val) {
            aux_field.aux_val = key;
            aux_field.aux_val = val;
        }

        std::string key;
        std::string kv_value;
        std::set<std::string> set_value;
        std::map<std::string, std::string> map_value;
        std::map<std::string, double> zset_value;
        std::list<std::string> list_value;

        void Debug();

        std::map<EntryID, EntryStream, EntryCmp> stream;

        /// add new entry stream. auto generate the 
        int AddStream(const std::vector<std::string> &data, EntryID &entry_id);
    };

    std::shared_ptr<ParsedResult> ResultMove(ParsedResult *result);

    class RdbParse {
    public:
        static Status Open(const std::string &path, RdbParse **rdb);

        virtual Status Next() = 0;

        virtual bool Valid() = 0;

        virtual ParsedResult *Value() = 0;

        RdbParse() = default;

        virtual ~RdbParse();

        RdbParse(const RdbParse &) = delete;

        RdbParse &operator=(const RdbParse &) = delete;

        virtual int GetVersion() = 0;
    };

}
#endif // __RDBPARSE_H__

