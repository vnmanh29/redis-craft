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
                                               signal_(io_context, SIGCHLD),
                                               timer_(io_context), heartbeat_retry_(0) {
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
    DoAccept();
}

int Server::StartMaster() {

    OnReady();
    io_context_.run();
    return 0;
}

int Server::StartReplica() {
    SyncWithMaster();

    OnReady();

    io_context_.run();
    return 0;
}

void Server::SetConfig(RedisConfig *cfg) {
    if (cfg) {
        /// TODO: add more config properties belong to network???

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

    return 0;
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
        master_ = Client::create(io_context_);
        master_->SetClientType(ClientType::TypeMaster);
        master_->UnsetWriteFlags(APP_RECV);
        master_->SetWriteFlags(MASTER_RECV);

        int ret = master_->ConnectAsync(io_context_, host, port);
        if (ret < 0) {
            LOG_ERROR(TAG, "Connect to host %s, port %s fail", host.c_str(), port.c_str());
            return SocketConnectError;
        }

        return 0;
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
    signal_.async_wait(
            [this](const std::error_code error, const int signal) {
                // validate the parent proc
                if (acceptor_.is_open()) {
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
    LOG_DEBUG(TAG, "there are %lu clients of this server", clients_.size());
    /// possibly there are some slaves waiting for. Transfer dump file .rdb
    if (exitcode == 0) {
        for (auto &client: clients_) {
            LOG_INFO(TAG, "Try to propagate rdb to client sock %d, client type %u", client->Socket().native_handle(),
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

    AddCommand("echo", EchoCmd, 0);

    AddCommand("get", GetCmd, READ_CMD);
    AddCommand("set", SetCmd, MASTER_SEND | SLAVE_RECV | WRITE_CMD | REPL_CMD);

    AddCommand("ping", PingCmd, MASTER_SEND | SLAVE_RECV);

    AddCommand("config", "get", ConfigGetCmd, READ_CMD);
    AddCommand("config", "set", ConfigSetCmd, WRITE_CMD);

    AddCommand("keys", KeysCmd, READ_CMD);
    AddCommand("info", InfoCmd, READ_CMD);

    AddCommand("replconf", "listening-port", ReplconfListeningPortCmd, READ_CMD);
    AddCommand("replconf", "capa", ReplconfCapaCmd, READ_CMD);
    AddCommand("replconf", "ack", ReplconfAckCmd, MASTER_RECV | SLAVE_SEND);
    AddCommand("replconf", "getack", ReplconfGetackCmd, SLAVE_RECV | MASTER_SEND | REPL_CMD);

    AddCommand("psync", PSyncCmd, READ_CMD);

    AddCommand("wait", WaitCmd, READ_CMD);

    AddCommand("type", TypeCmd, READ_CMD);
    AddCommand("xadd", XAddCmd, READ_CMD);

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
            if (!client) {
                LOG_ERROR(TAG, "Create client for new connection fail");
            } else {
                if (replication_info_.role == ReplicationRole::Master) {
                    client->SetWriteFlags(MASTER_SEND);
                } else if (replication_info_.role == ReplicationRole::Slave) {
                    client->SetWriteFlags(SLAVE_SEND);
                }

                LOG_INFO(TAG, "New connection, start receiving data from the client sock %d",
                         client->Socket().native_handle());
                clients_.push_back(client);
                client->ReadAsync();
            }
        } else {
            LOG_ERROR("Asio", "Accept new connection fail %s", error.message().c_str());
        }

        DoAccept();
    });
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

ssize_t Server::SaveRdbBackground(const std::string &file_name) {

    /// create pipe to write in child proc, read in parent proc
    OpenChildInfoPipe();

    pid_t child_pid;
    if ((child_pid = fork()) == 0) {
        /// child proc: save current db to rdb format
        io_context_.notify_fork(asio::io_context::fork_child);
        acceptor_.close();
        signal_.cancel();

        /// close reading part in child proc
        close(GetChildInfoReadPipe());

        /// save the memory db to the local file
#if HARDCODE
        LOG_DEBUG(TAG, "Start save empty rdb");
        std::string hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

        std::string data = RdbHex2Bin(hex);
        std::vector<unsigned char> binaryData;
        hexToBinaryData(hex, binaryData);

        FILE *file = fopen(file_name.c_str(), "wb");

        fwrite(binaryData.data(), sizeof(unsigned char), binaryData.size(), file);

        fclose(file);
        LOG_DEBUG(TAG, "Finish save empty rdb %s", file_name.c_str())

#else
        char magic[10] = {0};
    /// version
    snprintf(magic,sizeof(magic),"REDIS%04d",version_);
    Server::SendData(fd, magic);
#endif // HARDCODE

        /// TODO: notify to parent end of child proc
        _exit(0);
    } else {
        /// parent proc
        io_context_.notify_fork(asio::io_context::fork_parent);
        if (child_pid == -1) {
            /// create fail
            Server::GetInstance()->CloseChildInfoPipe();
            return -1;
        }

        SetChildPid(child_pid);
        /// close unused writing part
        close(GetChildInfoWritePipe());
    }

    return 0;
}

void Server::AddBackLogBuffer(const std::string &data) {
    if (replication_info_.is_replica) {
        LOG_DEBUG(TAG, "Add %zu bytes to backlog buffer, but this server is a replica, ignore", data.size());
        /// only update offset, no need store backlog buffer
        replication_info_.repl_offset += data.size();
    } else {
        AppendDataBuffer(&backlog_, data);
        LOG_DEBUG(TAG, "Add %zu bytes to backlog buffer, current size %zu", data.size(), backlog_.size);
        replication_info_.master_repl_offset += data.size();
    }
}

void Server::HeartbeatMechanism() {
    if (!master_)
        return;

    /// async send ACK to its master
    std::string msg = EncodeArr2RespArr({"REPLCONF", "ACK", std::to_string(replication_info_.repl_offset)});
    LOG_DEBUG("Slave", "Prepare send ACK info to its master: %s", msg.c_str());
    asio::async_write(master_->Socket(), asio::buffer(msg),
                      [this](std::error_code ec, std::size_t /*length*/) {
                          if (!ec) {
                              LOG_DEBUG("Heartbeat", "Sent ACK done");
                              // Schedule next write
                              heartbeat_retry_ = 0;
                              timer_.expires_after(std::chrono::milliseconds(replication_info_.heartbeat_interval_));
                              timer_.async_wait([this](std::error_code ec) {
                                  if (!ec) {
                                      HeartbeatMechanism();
                                  }
                              });
                          } else {
                              LOG_ERROR("Heartbeat", "write error: %s, tried time %d", ec.message().c_str(),
                                        heartbeat_retry_);
                              ++heartbeat_retry_;
                              if (heartbeat_retry_ < 5) {
                                  timer_.expires_after(std::chrono::milliseconds(1000 * heartbeat_retry_));
                                  timer_.async_wait([this](std::error_code ec) {
                                      if (!ec) {
                                          HeartbeatMechanism();
                                      }
                                  });
                              }
                          }
                      });
}

