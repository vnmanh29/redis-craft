//
// Created by Manh Nguyen Viet on 7/11/25.
//

#include "InternalCommandExecutor.h"
#include "Server.h"
#include "RedisError.h"

class EchoCommandExecutor : public AbstractInternalCommandExecutor
{
    ssize_t execute(const Query& query) override
    {
        if (query.cmd_args.size() < 2)
            return {};

        resp::encoder<std::string> enc;

        std::string response = enc.encode_bulk_str(query.cmd_args[1], query.cmd_args[1].size());

        int fd = GetSocket();

        return Server::SendData(fd, response);
    }
};

class GetCommandExecutor : public AbstractInternalCommandExecutor
{
private:
    static std::string GetResponse(const Query& query)
    {
        std::string response;

        if (query.cmd_args.size() < 2)
        {
            std::cout << "GetCommandExecutor: Invalid number of arguments" << std::endl;
            return "";
        }

        std::string resp = Database::GetInstance()->RetrieveValueOfKey(query.cmd_args[1]);

        if (resp.empty())
        {
            std::cout << "GetCommandExecutor: Key not found" << std::endl;
            return "$-1\r\n"; // RESP format for nil
        }
        else
        {
            resp::encoder<std::string> encoder;
            auto s = encoder.encode_bulk_str(resp, resp.size());
            printf("Get key %s, val %s\n", query.cmd_args[1].c_str(), s.c_str());
            return s;
        }
    }

    ssize_t execute(const Query& query) override
    {

        std::string response = GetResponse(query);
        if (response.empty())
            return -1;

        int fd = GetSocket();

        return Server::SendData(fd, response);
    }
};

class SetCommandExecutor : public AbstractInternalCommandExecutor
{
private:
    typedef struct Options
    {
        int set_on_exist;
        int64_t expired_ts;
        Options() : set_on_exist(-1), expired_ts(0) {}
    } Options;

    int ParseArgs(std::vector<std::string>& args, Options& opts)
    {
        for (size_t i = 2;i < args.size(); ++i)
        {
            std::string arg = args[i];
            std::transform(arg.begin(), arg.end(), arg.begin(), ::toupper);
            if (arg == "NX")
            {
                if (opts.set_on_exist == 1)
                    return -1;
                opts.set_on_exist = 0;
            }
            else if (arg == "XX")
            {
                if (opts.set_on_exist == 0)
                    return -1;
                opts.set_on_exist = 1;
            }
            else if (arg == "EX")
            {
                if (opts.expired_ts != 0 || i + 1 >= args.size())
                    return -1;

                opts.expired_ts = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count() + stoll(args[i+1]) * 1000;
                ++i;
            }
            else if (arg == "PX")
            {
                if (opts.expired_ts != 0 || i + 1 >= args.size())
                    return -1;
                opts.expired_ts = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count() + stoll(args[i+1]);
                ++i;
            }
        }

        return 0;
    }

    std::string GetResponse(const Query& query)
    {

        if (query.cmd_args.size() < 3)
            return {};
        auto key = query.cmd_args[1];
        auto val = query.cmd_args[2];

        /// parse the args
        std::vector<std::string> args = std::ref(query.cmd_args);
        Options opts;
        int ret = ParseArgs(args, opts);

        if (ret < 0)
            return "!12\r\nInvalid args\r\n";

        Database::GetInstance()->SetKeyVal(key, val, opts.set_on_exist, opts.expired_ts);

        return "+OK\r\n";
    }

public:
    ssize_t execute(const Query& query) override
    {
        std::string response = GetResponse(query);
        if (response.empty())
            return InvalidResponseError;
        int fd = GetSocket();

        return Server::SendData(fd, response);
    }

};

class PingCommandExecutor : public AbstractInternalCommandExecutor
{
    ssize_t execute(const Query& query) override
    {
        if (query.cmd_args.size() < 1)
            return InvalidCommandError;

        int fd = GetSocket();
        std::string response = "+PONG\r\n";

        return Server::SendData(fd, response);
    }
};

class GetConfigCommandExecutor : public AbstractInternalCommandExecutor
{
    std::string GetResponse(const Query& query)
    {
        if (query.cmd_args.size() < 2)
            return "!12\r\nInvalid args\r\n";

        std::vector<std::string> configs;
        for (int i = 2;i < query.cmd_args.size(); ++i)
        {
            std::string property = query.cmd_args[i];
            std::string cfg = Database::GetInstance()->GetConfigFromName(property);
            if (cfg.empty())
            {
                /// return invalid
                return "!12\r\nInvalid args\r\n";
            }
            else
            {
                configs.push_back(property);
                configs.push_back(cfg);
            }
        }

        resp::encoder<std::string> enc;
        std::vector<std::string> replies = enc.encode_arr(configs);
        std::string response;
        for (auto& rely : replies)
        {
            response += rely;
        }

        return response;
    }

public:
    ssize_t execute(const Query& query) override
    {
        std::string response = GetResponse(query);
        if (response.empty())
        {
            return InvalidResponseError;
        }

        int fd = GetSocket();
        return Server::SendData(fd, response);
    }
};

class KeysCommandExecutor : public AbstractInternalCommandExecutor
{
private:
    std::string GetResponse(const Query& query)
    {
        if (query.cmd_args.size() < 2)
            return "!12\r\nInvalid args\r\n";

        // get the pattern
        std::string pattern = query.cmd_args[1];
        auto matched_keys = Database::GetInstance()->RetrieveKeysMatchPattern(pattern);

        resp::encoder<std::string> enc;
        std::vector<std::string> resp_keys = enc.encode_arr(matched_keys);
        std::string response;
        for (auto& resp_key : resp_keys) {
            response += resp_key;
        }

        return response;
    }

public:
    ssize_t execute(const Query& query) override
    {
        std::string response = GetResponse(query);
        if (response.empty())
        {
            return InvalidResponseError;
        }

        int fd = GetSocket();
        return Server::SendData(fd, response);
    }

};

class InfoCommandExecutor : public AbstractInternalCommandExecutor
{
private:
    std::string GetResponse(const Query& query)
    {
        std::string section = "default";
        if (query.cmd_args.size() > 2)
        {
            return "!12\r\nInvalid args\r\n";
        }
        else if (query.cmd_args.size() == 2)
        {
            section = query.cmd_args[1];
        }

        if (section == "replication")
        {
            /// show the info of server
            std::string replication_info = Server::GetInstance()->ShowReplicationInfo();
            resp::encoder<std::string> enc;
            return enc.encode_bulk_str(replication_info, replication_info.size());
        }

        return "";
    }

public:
    ssize_t execute(const Query& query) override
    {
        std::string response = GetResponse(query);
        int fd = GetSocket();

        if (fd < 0 || response.empty())
            return InvalidResponseError;

        return Server::SendData(fd, response);
    }
};

class ReplconfCommandExecutor : public AbstractInternalCommandExecutor
{
    ssize_t execute(const Query& query) override
    {
        /// TODO: handle the argument
        std::string response = "+OK\r\n";
        int fd = GetSocket();

        return Server::SendData(fd, response);
    }
};

class PSyncCommandExecutor : public AbstractInternalCommandExecutor
{
    ssize_t execute(const Query& query) override
    {
        return 0;
//
//        /// if could not perform incremental replication
//        /// TODO: handle the argument
//        std::string master_repid = Server::GetInstance()->GetReplicationInfo().master_replid;
//        std::string master_repl_offset = std::to_string(Server::GetInstance()->GetReplicationInfo().master_repl_offset);
//
//        return EncodeRespSimpleStr("FULLRESYNC " + master_repid + " " + master_repl_offset);
    }
};

std::shared_ptr<AbstractInternalCommandExecutor>
AbstractInternalCommandExecutor::createCommandExecutor(const CommandType cmd_type) {
    switch (cmd_type) {
        case EchoCmd:
            return std::make_shared<EchoCommandExecutor>();
        case GetCmd:
            return std::make_shared<GetCommandExecutor>();
        case SetCmd:
            return std::make_shared<SetCommandExecutor>();
        case PingCmd:
            return std::make_shared<PingCommandExecutor>();
        case ConfigGetCmd:
            return std::make_shared<GetConfigCommandExecutor>();
        case KeysCmd:
            return std::make_shared<KeysCommandExecutor>();
        case InfoCmd:
            return std::make_shared<InfoCommandExecutor>();
        case ReplcofCmd:
            return std::make_shared<ReplconfCommandExecutor>();
        case PSyncCmd:
            return std::make_shared<PSyncCommandExecutor>();
        default:
            std::cerr << "Unknown command type: " << cmd_type << std::endl;
            break;
    }

    return nullptr;
}