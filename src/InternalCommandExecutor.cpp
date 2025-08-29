//
// Created by Manh Nguyen Viet on 7/11/25.
//

#include "InternalCommandExecutor.h"
#include "Server.h"
#include "RedisError.h"

class EchoCommandExecutor : public AbstractInternalCommandExecutor {
    void execute(const Query &query, std::shared_ptr<Client> client) override {
        if (query.cmd_args.size() < 2)
            return;

        resp::encoder<std::string> enc;

        std::string response = enc.encode_bulk_str(query.cmd_args[1], query.cmd_args[1].size());

        client->WriteAsync(response, APP_RECV | ALL_SEND);
    }
};

class GetCommandExecutor : public AbstractInternalCommandExecutor {
private:
    std::string GetResponse(const Query &query) {
        std::string response;

        if (query.cmd_args.size() < 2) {
            LOG_ERROR(EXECUTOR, "GetCommandExecutor: Invalid number of arguments")
            return "";
        }

        std::string resp = Database::GetInstance()->RetrieveValueOfKey(query.cmd_args[1]);

        if (resp.empty()) {
            LOG_ERROR(EXECUTOR, "GetCommandExecutor: Key %s not found", query.cmd_args[1].c_str());
            return "$-1\r\n"; // RESP format for nil
        } else {
            resp::encoder<std::string> encoder;
            auto s = encoder.encode_bulk_str(resp, resp.size());
            LOG_DEBUG(EXECUTOR, "Get key %s, val %s\n", query.cmd_args[1].c_str(), s.c_str());
            return s;
        }
    }

    void execute(const Query &query, std::shared_ptr<Client> client) override {
        std::string response = GetResponse(query);
        if (response.empty())
            return;

        client->WriteAsync(response, APP_RECV | MASTER_SEND | SLAVE_SEND);
    }
};

class SetCommandExecutor : public AbstractInternalCommandExecutor {
private:
    typedef struct Options {
        int set_on_exist;
        int64_t expired_ts;

        Options() : set_on_exist(-1), expired_ts(0) {}
    } Options;

    int ParseArgs(std::vector<std::string> &args, Options &opts) {
        for (size_t i = 2; i < args.size(); ++i) {
            std::string arg = args[i];
            std::transform(arg.begin(), arg.end(), arg.begin(), ::toupper);
            if (arg == "NX") {
                if (opts.set_on_exist == 1)
                    return -1;
                opts.set_on_exist = 0;
            } else if (arg == "XX") {
                if (opts.set_on_exist == 0)
                    return -1;
                opts.set_on_exist = 1;
            } else if (arg == "EX") {
                if (opts.expired_ts != 0 || i + 1 >= args.size())
                    return -1;

                opts.expired_ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch()).count() + stoll(args[i + 1]) * 1000;
                ++i;
            } else if (arg == "PX") {
                if (opts.expired_ts != 0 || i + 1 >= args.size())
                    return -1;
                opts.expired_ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch()).count() + stoll(args[i + 1]);
                ++i;
            }
        }

        return 0;
    }

    std::string GetResponse(const Query &query) {

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
    void execute(const Query &query, std::shared_ptr<Client> client) override {
        std::string response = GetResponse(query);
        if (response.empty())
            return;

        client->WriteAsync(response, APP_RECV | MASTER_SEND);
    }

};

class PingCommandExecutor : public AbstractInternalCommandExecutor {
    void execute(const Query &query, std::shared_ptr<Client> client) override {
        if (query.cmd_args.size() < 1)
            return;

        std::string response = RESP_PONG;

        client->WriteAsync(response, APP_RECV | MASTER_SEND | SLAVE_SEND);
    }
};

class GetConfigCommandExecutor : public AbstractInternalCommandExecutor {
    std::string GetResponse(const Query &query) {
        if (query.cmd_args.size() < 2)
            return "!12\r\nInvalid args\r\n";

        std::vector<std::string> configs;
        for (int i = 2; i < query.cmd_args.size(); ++i) {
            std::string property = query.cmd_args[i];
            std::string cfg = Database::GetInstance()->GetConfigFromName(property);
            if (cfg.empty()) {
                /// return invalid
                return "!12\r\nInvalid args\r\n";
            } else {
                configs.push_back(property);
                configs.push_back(cfg);
            }
        }

        resp::encoder<std::string> enc;
        std::vector<std::string> replies = enc.encode_arr(configs);
        std::string response;
        for (auto &rely: replies) {
            response += rely;
        }

        return response;
    }

public:
    void execute(const Query &query, std::shared_ptr<Client> client) override {
        std::string response = GetResponse(query);

        client->WriteAsync(response, APP_RECV | ALL_SEND);
    }
};

class KeysCommandExecutor : public AbstractInternalCommandExecutor {
private:
    std::string GetResponse(const Query &query) {
        if (query.cmd_args.size() < 2)
            return "!12\r\nInvalid args\r\n";

        // get the pattern
        std::string pattern = query.cmd_args[1];
        auto matched_keys = Database::GetInstance()->RetrieveKeysMatchPattern(pattern);

        resp::encoder<std::string> enc;
        std::vector<std::string> resp_keys = enc.encode_arr(matched_keys);
        std::string response;
        for (auto &resp_key: resp_keys) {
            response += resp_key;
        }

        return response;
    }

public:
    void execute(const Query &query, std::shared_ptr<Client> client) override {
        std::string response = GetResponse(query);

        client->WriteAsync(response, APP_RECV | ALL_SEND);
    }

};

class InfoCommandExecutor : public AbstractInternalCommandExecutor {
private:
    std::string GetResponse(const Query &query) {
        std::string section = "default";
        if (query.cmd_args.size() > 2) {
            return "!12\r\nInvalid args\r\n";
        } else if (query.cmd_args.size() == 2) {
            section = query.cmd_args[1];
        }

        if (section == "replication") {
            /// show the info of server
            std::string replication_info = Server::GetInstance()->ShowReplicationInfo();
            resp::encoder<std::string> enc;
            return enc.encode_bulk_str(replication_info, replication_info.size());
        }

        return "";
    }

public:
    void execute(const Query &query, std::shared_ptr<Client> client) override {
        std::string response = GetResponse(query);

        client->WriteAsync(response, APP_RECV | ALL_SEND);
    }
};

class ReplconfListeningPortCommandExecutor : public AbstractInternalCommandExecutor {
private:
    std::string GetResponse(const Query &query, std::shared_ptr<Client> client) {
        std::string reply = RESP_NIL;

        if (query.cmd_args.size() < 3) {
            return RESP_OK;
        } else {
            std::string arg1 = query.cmd_args[1];
            std::string arg2 = query.cmd_args[2];
            std::transform(arg1.begin(), arg1.end(), arg1.begin(), ::toupper);
            if (arg1 != "LISTENING-PORT") {
                return RESP_NIL;
            }

            /// send from slave to master for psync cmd
            /// set it become slave server
            client->SetClientType(ClientType::TypeSlave);
            client->UnsetWriteFlags(APP_RECV);
            client->SetWriteFlags(SLAVE_RECV);

            client->SetSlaveState(SlaveState::WaitBGSaveStart);

            /// TODO: handle arg2
            reply = RESP_OK;
        }
        return reply;
    }

public:
    void execute(const Query &query, std::shared_ptr<Client> client) override {
        /// TODO: handle the argument
        std::string response = GetResponse(query, client);

        client->WriteAsync(response, SLAVE_RECV | MASTER_SEND);
    }
};

class ReplconfCapaCommandExecutor : public AbstractInternalCommandExecutor {
private:
    std::string GetResponse(const Query &query, std::shared_ptr<Client> client) {
        std::string reply = RESP_NIL;

        if (query.cmd_args.size() < 3) {
            return RESP_OK;
        } else {
            std::string arg1 = query.cmd_args[1];
            std::string arg2 = query.cmd_args[2];
            std::transform(arg1.begin(), arg1.end(), arg1.begin(), ::toupper);
            if (arg1 != "CAPA") { /// send from slave to master for psync
                return RESP_NIL;
            }

            /// set it become slave server
            client->SetClientType(ClientType::TypeSlave);
            client->UnsetWriteFlags(APP_RECV);
            client->SetWriteFlags(SLAVE_RECV);

            client->SetSlaveState(SlaveState::WaitBGSaveStart);

            /// TODO: handle arg2
            reply = RESP_OK;
        }
        return reply;
    }

public:
    void execute(const Query &query, std::shared_ptr<Client> client) override {
        /// TODO: handle the argument
        std::string response = GetResponse(query, client);

        client->WriteAsync(response, SLAVE_RECV | MASTER_SEND);
    }
};

class ReplconfAckCommandExecutor : public AbstractInternalCommandExecutor {
private:
    std::string GetResponse(const Query &query, std::shared_ptr<Client> client) {
        std::string reply = RESP_OK;

        return reply;
    }

public:
    void execute(const Query &query, std::shared_ptr<Client> client) override {
        if (query.cmd_args.size() < 3) {
            LOG_ERROR(EXECUTOR, "ReplconfAckCommandExecutor: Invalid number of arguments");
            return;
        } else {
            std::string arg1 = query.cmd_args[1];
            std::string arg2 = query.cmd_args[2];
            std::transform(arg1.begin(), arg1.end(), arg1.begin(), ::toupper);
            if (arg1 != "ACK") {
                /// invalid args
                LOG_ERROR(EXECUTOR, "ReplconfAckCommandExecutor: Invalid args");
                return;
            }

            int offset = std::stoi(arg2);
            auto prev_offset = client->GetSlavePrevOffset();

            /// update the offset of slave
            client->SetSlaveOffset(offset);
            /// validate all client that in state WaitCmdBlocked
            auto clients = Server::GetInstance()->GetClients();
            for (auto &cli: clients) {
                if (cli->SlaveState() == WaitCmdBlocked && cli->TargetOffset() <= offset &&
                    cli->TargetOffset() > prev_offset) {
                    /// this slave now become updated with cli
                    int num_good_replicas = cli->GetNumGoodReplicas();
                    ++num_good_replicas;
                    cli->SetNumGoodReplicas(num_good_replicas);
                    if (num_good_replicas >= cli->GetMinGoodReplicas()) {
                        /// enough good replica. Stop timer
                        cli->CancelWaiting();
                    } else {
                        /// update good replica
                        cli->SetNumGoodReplicas(num_good_replicas);
                    }
                }
            }
        }
    }
};

class ReplconfGetAckCommandExecutor : public AbstractInternalCommandExecutor {
private:
    std::string GetResponse(const Query &query, std::shared_ptr<Client> client) {
        std::string reply = RESP_NIL;

        if (query.cmd_args.size() < 3) {
            return RESP_NIL;
        } else {
            std::string arg1 = query.cmd_args[1];
            std::string arg2 = query.cmd_args[2];
            std::transform(arg1.begin(), arg1.end(), arg1.begin(), ::toupper);
            if (arg1 == "GETACK") { /// from master send to slave
                /// set it is master server
                client->SetClientType(ClientType::TypeMaster);

                int64_t offset = Server::GetInstance()->GetServerOffset();

                reply = EncodeArr2RespArr({"REPLCONF", "ACK", std::to_string(offset)});
            } else {
                reply = RESP_NIL;
            }
            return reply;
        }
    }

public:
    void execute(const Query &query, std::shared_ptr<Client> client) override {
        /// TODO: handle the argument
        std::string response = GetResponse(query, client);

        client->WriteAsync(response, MASTER_RECV | SLAVE_SEND);
    }
};

class PSyncCommandExecutor : public AbstractInternalCommandExecutor {
    void execute(const Query &query, std::shared_ptr<Client> client) override {

        /// if could not perform incremental replication
        /// TODO: handle the argument
        std::string master_repid = Server::GetInstance()->GetReplicationInfo().master_replid;
        std::string master_repl_offset = std::to_string(Server::GetInstance()->GetReplicationInfo().master_repl_offset);
        std::string rdb_file = Database::GetInstance()->GetRdbPath();

        /// psync command supports responds to slave

        /// Case 1: full sync
        std::string response = EncodeRespSimpleStr("FULLRESYNC " + master_repid + " " + master_repl_offset);

        client->WriteAsync(response, SLAVE_RECV | MASTER_SEND);

        client->SetSlaveState(SlaveState::WaitBGSaveEnd);

        /// Parent proc creates a child proc
        /// Snapshot the current database in child process and send it over TCP, notify to the parent when finish
        /// The parent set Redis to SAVE_DB_MODE, buffering all received commands.
        /// After received notification from the child (using waitpid()), the parent handles all command in buffer and propagate all write commands to the replicas
        ssize_t ret = Server::GetInstance()->SaveRdbBackground(rdb_file);
    }
};

class FullresyncCommandExecutor : public AbstractInternalCommandExecutor {
    void execute(const Query &query, std::shared_ptr<Client> client) override {

    }
};

class WaitCommandExecutor : public AbstractInternalCommandExecutor {
    void execute(const Query &query, std::shared_ptr<Client> client) override {
        if (query.cmd_args.size() < 3) {
            LOG_ERROR(EXECUTOR, "Invalid wait command");
            return;
        }

        int min_good_replicas = std::stoi(query.cmd_args[1]);
        int timeout = std::stoi(query.cmd_args[2]);
        timeout = (timeout == 0) ? INT_MAX : timeout;

        LOG_INFO(EXECUTOR, "wait at least %d replica in %d", min_good_replicas, timeout);
        if (min_good_replicas == 0) {
            client->WriteAsync(EncodeRespInteger(0), APP_RECV | MASTER_SEND);
            return;
        }

        int64_t master_offset = Server::GetInstance()->GetServerOffset();
        int num_good_replicas = 0;

        /// loop all its replicas to check their offset
        auto clients = Server::GetInstance()->GetClients();
        std::vector<std::shared_ptr<Client>> lag_replicas;
        for (auto &cli: clients) {
            if (cli->ClientType() == ClientType::TypeSlave) {
                if (cli->GetSlaveOffset() >= master_offset) {
                    ++num_good_replicas;
                } else {
                    lag_replicas.push_back(cli);
                }
            }
        }

        if (num_good_replicas >= min_good_replicas) {
            /// already enough replicas, write response and return
            client->WriteAsync(EncodeRespInteger(num_good_replicas), APP_RECV | MASTER_SEND);
            return;
        }

        /// not enough good replicas. Block this client and set target offset
        client->SetNumGoodReplicas(num_good_replicas);
        client->SetMinGoodReplicas(min_good_replicas);
        client->SetTargetOffset(master_offset);
        client->SetSlaveState(WaitCmdBlocked);

        /// send a GETACK command to all lag replicas to get their offset
        if (min_good_replicas > 0) {
            std::string getack_cmd = EncodeArr2RespArr({"REPLCONF", "GETACK", "*"});
            for (auto &cli: lag_replicas) {
                cli->WriteAsync(getack_cmd, SLAVE_RECV | MASTER_SEND);
            }
        }

        /// wait for reaching timeout
        client->HandleWaitCommand(timeout);
    }
};

class TypeCommandExecutor : public AbstractInternalCommandExecutor {
    void execute(const Query &query, std::shared_ptr<Client> client) override {
        if (query.cmd_args.size() < 2) {
            LOG_ERROR(EXECUTOR, "Invalid argc of command Type");
            return;
        }

        std::string arg1 = query.cmd_args[1];
        std::string reply = RESP_NONE;
        
        if (Database::GetInstance()->IsKeyExist(arg1)) {
            reply = "+string\r\n";
        } else {
            reply = RESP_NONE;
        }

        client->WriteAsync(reply, APP_RECV | ALL_SEND);
    }
};

class UnknownCommandExecutor : public AbstractInternalCommandExecutor {
    void execute(const Query &query, std::shared_ptr<Client> client) override {

        /// FIXME: handle with unknown command
        LOG_INFO(EXECUTOR, "Unknown command");
        client->WriteAsync(RESP_OK, APP_RECV | ALL_SEND);
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
        case ReplconfListeningPortCmd:
            return std::make_shared<ReplconfListeningPortCommandExecutor>();
        case ReplconfCapaCmd:
            return std::make_shared<ReplconfCapaCommandExecutor>();
        case ReplconfAckCmd:
            return std::make_shared<ReplconfAckCommandExecutor>();
        case ReplconfGetackCmd:
            return std::make_shared<ReplconfGetAckCommandExecutor>();
        case PSyncCmd:
            return std::make_shared<PSyncCommandExecutor>();
        case FullresyncCmd:
            return std::make_shared<FullresyncCommandExecutor>();
        case WaitCmd:
            return std::make_shared<WaitCommandExecutor>();
        case TypeCmd:
            return std::make_shared<TypeCommandExecutor>();
        default:
            std::cerr << "Unknown command type: " << cmd_type << std::endl;
            return std::make_shared<UnknownCommandExecutor>();
    }

    return nullptr;
}