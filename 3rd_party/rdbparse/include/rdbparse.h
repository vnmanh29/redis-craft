#ifndef __RDBPARSE_H__
#define __RDBPARSE_H__

#include <string>
#include <map>
#include <list>
#include <set>
#include <memory>
#include <vector>

#include "status.h"
#include "slice.h"

#define ROOT_ENTRY_ID "0-0"

namespace RdbParser {

    struct AuxKV {
        std::string aux_key;
        std::string aux_val;
    };

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

        using KeyValStr = std::pair<std::string, std::string>;

        typedef struct EntryStream {
            std::vector<KeyValStr> key_vals;
        } EntryStream;

        std::map<std::string, EntryStream> stream;

        /// add new entry stream. auto generate the 
        int AddStream(const std::vector<std::string> &data, std::string &entry_id);
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
#endif

