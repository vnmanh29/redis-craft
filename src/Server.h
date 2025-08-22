//
// Created by Manh Nguyen Viet on 7/20/25.
//

#ifndef REDIS_STARTER_CPP_SERVER_H
#define REDIS_STARTER_CPP_SERVER_H

#include <mutex>
#include <vector>
#include <memory>
#include <unistd.h>
#include <atomic>
#include <fstream>

#include "RedisOption.h"
#include "CommandExecutor.h"
#include "RedisDef.h"
#include "Client.h"

#if ASIO_LIB

#include "asio.hpp"

using asio::ip::tcp;

#endif

enum ReplicationRole {
    Master = 0,
    Slave
};

/// State when setup replication
enum ReplicationState {
    ReplStateNone = 0,
    ReplStateConnect = 1,
    ReplStateConnecting = 2,
    ReplStateReceivePingReply = 3,
    ReplStateSendHandShake = 4,

    ReplStateReceivePSyncReply = 5,
    ReplStateSyncing = 6,
    ReplStateSynced = 7,
};

typedef struct ReplicationInfo {

    ReplicationRole role;
    int connected_slaves;
    std::string master_replid;
    int64_t master_repl_offset;

    // If the instance is a replica, these additional fields are provided
    int is_replica;
    std::string master_host;
    int master_port;
    ReplicationState replica_state;

    // default constructor
    ReplicationInfo() : role(Master), connected_slaves(0), master_repl_offset(0), is_replica(0),
                        replica_state(ReplicationState::ReplStateNone) {
        master_replid = DEFAULT_MASTER_REPLID;
    }
} ReplicationInfo;

#define BUF_SIZE 4096

class Server {
private:
    static Server *instance_;
    static std::mutex m_;

    int server_fd_;                                  /// the fd of redis server to listen all requests
    int replica_fd_;                                 /// <replica only>: the fd in replica server connect to the master server
    std::vector<std::shared_ptr<Client>> clients_;   /// list of clients connect to this redis server

    uint16_t port_;

    pid_t child_pid_;

    std::atomic_bool running_;
    ReplicationInfo replication_info_;

    int child_info_pipe_[2];

    std::unordered_map<std::string, RedisCmd *> global_commands_;


    asio::io_context &io_context_;   /// asio io_context to handle async operations
    asio::ip::tcp::acceptor acceptor_;  /// asio acceptor to handle incoming connections
    tcp::socket replica_socket_;    /// <replica only>: socket in the replica server connect to the master
    asio::signal_set signal_;   /// use to check the changing state of child process

private:
    Server() = default;

    Server(asio::io_context &io_context) : io_context_(io_context), acceptor_(io_context), replica_socket_(io_context),
                                           signal_(io_context, SIGCHLD) {
    }

    /// only the replica server call this method
    int SyncWithMaster();

    int ReceiveRdbFromMaster(std::shared_ptr<Client> master_server, const long total_size);

    int SetupCommands();

    void CheckChildrenDone();

    void OnSaveRdbBackgroundDone(const int exitcode);

    ssize_t FullSyncRdbToReplica(const std::shared_ptr<Client> &slave);

    void AddCommand(const std::string &command, CommandType type, uint64_t flag);

    void AddCommand(const std::string &command, const std::string &subcmd, CommandType type, uint64_t flag);

public:
    Server &operator=(const Server &sv) = delete;

    Server(const Server &rhs) = delete;

    ~Server();

    static Server *GetInstance();

    /// initialize the server from config, include sync data from master server (if this is a replica)
    int Setup();

    /// start the Redis server, already to listen all command after the preparing
    int Start();

    /// Accept coming connections
    void DoAccept();

    void SetConfig(const std::shared_ptr<RedisConfig> &cfg);

    void SetReplicaState(const ReplicationState state) { replication_info_.replica_state = state; }

    std::string ShowReplicationInfo() const;

    inline ReplicationInfo GetReplicationInfo() const {
        return replication_info_;
    }

    inline void SetChildPid(pid_t pid) {
        child_pid_ = pid;
    }

    int OpenChildInfoPipe();

    void CloseChildInfoPipe();

    int GetChildInfoReadPipe() { return child_info_pipe_[0]; }

    int GetChildInfoWritePipe() { return child_info_pipe_[1]; }

    std::vector<std::shared_ptr<Client>> GetClients() const { return clients_; }

    RedisCmd *GetRedisCommand(const std::string &cmd_name);
};


#endif //REDIS_STARTER_CPP_SERVER_H
