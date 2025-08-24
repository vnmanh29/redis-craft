//
// Created by Manh Nguyen Viet on 7/20/25.
//

#include "Server.h"
#include "CommandExecutor.h"
#include "Utils.h"
#include "RedisError.h"
#include "Utils.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <netinet/tcp.h> // Required for TCP_NODELAY

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "asio.hpp"

Server *Server::instance_{nullptr};
std::mutex Server::m_;
asio::io_context global_context;

int server_port = DEFAULT_REDIS_PORT;

Server::Server(asio::io_context &io_context) : acceptor_(io_context, tcp::endpoint(tcp::v4(), server_port)),
                                               port_(server_port),
                                               io_context_(io_context),
                                               replica_socket_(io_context),
                                               signal_(io_context, SIGCHLD) {
}

Server *Server::GetInstance() {
    std::lock_guard lock(m_);
    if (instance_ == nullptr) {
        instance_ = new Server(global_context);
    }
    return instance_;
}

Server::~Server() {
    acceptor_.close();
    replica_socket_.close();
    LOG_INFO("Server", "Destructor server instance");
}

void Server::OnReady() {
    LOG_LINE();
    CheckChildrenDone();
    LOG_LINE();
//    acceptor_ = tcp::acceptor(io_context_, tcp::endpoint(tcp::v4(), port_));
    DoAccept();
}

int Server::StartMaster() {

    OnReady();
    io_context_.run();
    return 0;
}

void Server::SetConfig(const std::shared_ptr<RedisConfig> &cfg) {
    if (cfg) {
        /// TODO: add more config properties belong to network???
//        port_ = cfg->port;

        if (cfg->is_replica) {
            replication_info_.role = ReplicationRole::Slave;

            replication_info_.is_replica = cfg->is_replica;
            replication_info_.master_host = cfg->master_host;
            replication_info_.master_port = cfg->master_port;
        }
    }
}

std::string Server::ShowReplicationInfo() const {
    std::stringstream ss;

    ss << "role:";
    ss << ((replication_info_.role == ReplicationRole::Master) ? "master" : "slave") << CRLF;

    ss << "connected_slave:" << replication_info_.connected_slaves << CRLF;
    ss << "master_replid:" << replication_info_.master_replid << CRLF;
    ss << "master_repl_offset:" << replication_info_.master_repl_offset << CRLF;

    if (replication_info_.is_replica) {
        ss << "master_host:" << replication_info_.master_host << CRLF;
        ss << "master_port:" << replication_info_.master_port << CRLF;
    }

    return ss.str();
}

int Server::Setup() {
    /// 0. setup commands
    SetupCommands();

    /// 1. setup the replica
    int ret = SyncWithMaster();

    return ret;
}

int Server::SyncWithMaster() {
    if (!replication_info_.is_replica)
        return ReplicationRole::Master;

    /// blocking operation. This method will block until the RDB file is received from the master
    std::string host = replication_info_.master_host;
    std::string port = std::to_string(replication_info_.master_port);
    LOG_DEBUG(TAG, "sync with master server, host: %s, port %s", host.c_str(), port.c_str());

    try {
        /// create a client presents for its master server
        auto master_server = Client::create(io_context_);
        master_server->SetClientType(ClientType::TypeMaster);

        int ret = master_server->ConnectAsync(io_context_, host, port);
        if (ret < 0) {
            LOG_ERROR(TAG, "Connect to host %s, port %s fail", host.c_str(), port.c_str());
            return SocketConnectError;
        }


        LOG_LINE();
        io_context_.run();
//        io_context_.restart();
        LOG_LINE();
        return 0;
//
//        std::vector<char> buf(128);
//        /// Handshake part
//
//        LOG_LINE();
//        std::string response;
//        /// 1. PING to master
//        master_server->WriteSync(EncodeArr2RespArr({"PING"}));
//        master_server->ReadSyncWithLength(response, strlen(RESP_PONG));
//        if (strncasecmp(response.data(), RESP_PONG, strlen(RESP_PONG)) != 0) {
//            LOG_ERROR("Handshake", "response of PING: %s, expect %s", response.data(), RESP_PONG);
//            return HandShakeRecvError;
//        }
//
//        LOG_LINE();
//        /// 2. send Replconf to master
//        master_server->WriteSync(EncodeArr2RespArr({"replconf", "listening-port", std::to_string(port_)}));
//        master_server->ReadSyncWithLength(response, strlen(RESP_OK));
//        if (strncasecmp(response.data(), RESP_OK, strlen(RESP_OK)) != 0) {
//            LOG_ERROR("Handshake", "compare %zu bytes response of REPLCONF listening-port: %s, expect %s",
//                      strlen(RESP_OK), response.data(), RESP_OK);
//            return HandShakeRecvError;
//        }
//
//        master_server->WriteSync((EncodeArr2RespArr({"replconf", "capa", "psync2"})));
//        master_server->ReadSyncWithLength(response, strlen(RESP_OK));
//        if (strncasecmp(response.data(), RESP_OK, strlen(RESP_OK)) != 0) {
//            LOG_ERROR("Handshake", "response of REPLCONF capa: %s, expect %s", response.data(), RESP_OK);
//            return HandShakeRecvError;
//        }
//
//        LOG_LINE();
//        /// 3. PSYNC step
//        master_server->WriteSync((EncodeArr2RespArr({"psync", "?", "-1"})));
//
//        /// try to read FULLRESYNC response: read byte by byte util meet '\n'
//        /// FIXME: handle with timeout
//        ret = master_server->ReadSyncNewLine(response);
//        if (ret < 0) {
//            LOG_ERROR(TAG, "read new line from command FULLRESYNC fail");
//            return ret;
//        }
//
//        /// parse 'buf': verify response with token RESP_FULLRESYNC, get master_uid, get offset
//        size_t idx = response.find(' ');
//        if (idx == std::string::npos || response.substr(0, strlen(RESP_FULLRESYNC)) != RESP_FULLRESYNC) {
//            LOG_ERROR(TAG, "Invalid response of PSYNC, not found FULLRESYNC");
//            return HandShakeRecvError;
//        }
//
//        ++idx;
//        size_t idx1 = response.find(' ', idx);
//        if (idx1 == std::string::npos) {
//            LOG_ERROR(TAG, "Invalid master_uid in response of PSYNC");
//            return HandShakeRecvError;
//        }
//
//        replication_info_.master_replid = response.substr(idx, idx1 - idx);
//        replication_info_.master_repl_offset = std::stoll(response.substr(idx1 + 1));
//
//        LOG_LINE();
//        /// 4. Reading file .rdb with async method
//        /// Format of file: $<length_of_file>\r\n<binary_contents_of_file>
//        /// 4.1 read and parse the size of rdb file
//        ret = master_server->ReadSyncNewLine(response);
//        if (ret < 0) {
//            LOG_ERROR(TAG, "read new line to parse the rdb size fail");
//            return SyncReadError;
//        }
//        LOG_LINE();
//        /// 4.2 parse the length
//        if (response.size() <= 1 || response[0] != '$') {
//            LOG_ERROR(TAG, "parse the size of rdb file fail");
//            return InvalidResponseError;
//        }
//
//        LOG_DEBUG("RDB", "response %s", response.c_str());
//        long rdb_filesize = std::stol(response.substr(1));
//        LOG_DEBUG("RDB", "rdb file size %lu", rdb_filesize);
//        ReceiveRdbFromMaster(master_server, rdb_filesize);
//
//        /// TODO: 5. Load rdb file to memory
//
//        return RedisSuccess;

    } catch (const asio::system_error &e) {
        LOG_ERROR("Asio", "Connect to host %s, port %s fail %d: %s", host.c_str(), port.c_str(), e.code().value(),
                  e.what());
        return UnknownError;
    }
}

int Server::OpenChildInfoPipe() {
    int ret = pipe(child_info_pipe_);
    if (ret == -1) {
        /// close to make sure not leak
        LOG_ERROR(TAG, "create pip fail %d msg: %s", errno, strerror(errno));
        CloseChildInfoPipe();
        return -1;
    }

    return 0;
}

void Server::CloseChildInfoPipe() {
    if (child_info_pipe_[0] != -1 || child_info_pipe_[1] != -1) {
        close(child_info_pipe_[0]);
        close(child_info_pipe_[0]);
        child_info_pipe_[0] = -1;
        child_info_pipe_[1] = -1;
    }
}

void Server::CheckChildrenDone() {
    LOG_LINE();
    signal_.async_wait(
            [this](const std::error_code error, const int signal) {
                // validate the parent proc
                LOG_LINE();
                if (acceptor_.is_open()) {
                    LOG_LINE();
                    int status = 0;
                    pid_t pid;
                    if ((pid = waitpid(-1, &status, WNOHANG)) != 0) {
                        if (pid == -1) {
                            /// return there is not any waiting child proc
                            if (errno == ECHILD)
                                return;
                            LOG_ERROR(TAG, "waitpid() returned an error: %s, child proc %d", strerror(errno),
                                      child_pid_);
                            /// TODO: handle error
                        } else {
                            /// handle by exit code, the child proc type
                            int exitcode = WIFEXITED(status) ? WEXITSTATUS(status) : -1;

                            if (pid == child_pid_) {
                                LOG_DEBUG(TAG, "Handle the child proc %d exit with status %d, exitcode %d", pid, status,
                                          exitcode)
                                /// TODO: handle the result of child proc in parent proc
                                OnSaveRdbBackgroundDone(exitcode);
                            } else {
                                LOG_ERROR(TAG, "Conflict between child proc %d and finished pid %d", child_pid_, pid)
                                /// TODO: handle conflict
                            }
                        }
                    }

                    CheckChildrenDone();
                } else {
                    LOG_LINE();
                }
            });
    LOG_LINE();
}

void Server::OnSaveRdbBackgroundDone(const int exitcode) {
    /// TODO: update state???
    LOG_ERROR(TAG, "there are %lu clients of this server", clients_.size());
    /// possibly there are some slaves waiting for. Transfer dump file .rdb
    if (exitcode == 0) {
        for (auto &client: clients_) {
            LOG_ERROR(TAG, "Try to propagate rdb to client sock %d, client type %u", client->Socket().native_handle(),
                      client->ClientType());
            if (client->ClientType() == TypeSlave && client->SlaveState() == SlaveState::WaitBGSaveEnd) {
                FullSyncRdbToReplica(client);
                /// FIXME: handle when sent partially
            }
        }
    }
    LOG_LINE()
}

void Server::FullSyncRdbToReplica(const std::shared_ptr<Client> &slave) {
    std::string rdb_file_path = Database::GetInstance()->GetRdbPath();

    slave->PropagateRdb(rdb_file_path);;
}

RedisCmd *Server::GetRedisCommand(const std::string &cmd_name) {
    try {
        return global_commands_.at(cmd_name);
    } catch (std::exception &ex) {
        return nullptr;
    }
}

void Server::AddCommand(const std::string &command, CommandType type, uint64_t flag) {
    try {
        auto rcmd = global_commands_.at(command);
        rcmd->cmd_type = type;
        rcmd->flags |= flag;
    }
    catch (std::exception &ex) {
        global_commands_[command] = new RedisCmd(type, flag);
    }
}

void
Server::AddCommand(const std::string &command, const std::string &subcmd, CommandType type, uint64_t flag) {
    RedisCmd *rcmd = nullptr;
    try {
        rcmd = global_commands_.at(command);
    }
    catch (std::exception &ex) {
        global_commands_[command] = new RedisCmd(type, flag);
        rcmd = global_commands_[command];
    }

    rcmd->subcmd_dict[subcmd] = new RedisCmd(type, flag);
}

int Server::SetupCommands() {

    AddCommand("echo", EchoCmd, CMD_READ);

    AddCommand("get", GetCmd, CMD_READ);
    AddCommand("set", SetCmd, CMD_WRITE);

    AddCommand("ping", PingCmd, CMD_READ);

    AddCommand("config", "get", ConfigGetCmd, CMD_READ);
    AddCommand("config", "set", ConfigSetCmd, CMD_WRITE);

    AddCommand("keys", KeysCmd, CMD_READ);
    AddCommand("info", InfoCmd, CMD_READ);
    AddCommand("replconf", ReplconfCmd, CMD_READ);
    AddCommand("psync", PSyncCmd, CMD_READ);

    return 0;
}

int Server::ReceiveRdbFromMaster(std::shared_ptr<Client> master_server, const long total_size) {
    /**
     * 1. discard all data in current database */
    Database::GetInstance()->Reset();

    /**
     * 2. Create a temporary file in disk */
    std::string tmp_file_path = Database::GetInstance()->GetRdbPath();
    FILE *ptmp_file = fopen(tmp_file_path.c_str(), "wb");
    if (!ptmp_file) {
        LOG_ERROR(TAG, "open temp file %s fail %s", tmp_file_path.c_str(), strerror(errno));
        return OpenRdbFileError;
    }

    /**
     * 3. Read incoming data */
    master_server->ReadBulkAsyncWriteFile(total_size, 0, ptmp_file);

    LOG_LINE()
    return (replication_info_.replica_state == ReplicationState::ReplStateSynced);
}

void Server::DoAccept() {
    LOG_INFO(TAG, "Wait new connection ...");
    acceptor_.async_accept([this](const std::error_code &error, tcp::socket socket) {
        if (!error) {
            auto client = Client::CreateBindSocket(io_context_, std::move(socket));
            LOG_INFO(TAG, "New connection, start receiving data from the client sock %d",
                     client->Socket().native_handle());
            clients_.push_back(client);
            client->ReadAsync();
        } else {
            LOG_ERROR("Asio", "Accept new connection fail %s", error.message().c_str());
        }

        DoAccept();
    });
    LOG_LINE();
}

int Server::HandleFullResyncReply(const std::string &reply) {

    /// parse 'buf': verify response with token RESP_FULLRESYNC, get master_uid, get offset
    size_t idx = reply.find(' ');
    if (idx == std::string::npos || reply.substr(0, strlen(RESP_FULLRESYNC)) != RESP_FULLRESYNC) {
        LOG_ERROR("HandShake", "Invalid response of PSYNC, not found FULLRESYNC");
        return HandShakeRecvError;
    }
    LOG_LINE();

    ++idx;
    size_t idx1 = reply.find(' ', idx);
    if (idx1 == std::string::npos) {
        LOG_ERROR(TAG, "Invalid master_uid in response of PSYNC");
        return HandShakeRecvError;
    }

    LOG_LINE();

    replication_info_.master_replid = reply.substr(idx, idx1 - idx);
    replication_info_.master_repl_offset = std::stoll(reply.substr(idx1 + 1));

    return 0;
}