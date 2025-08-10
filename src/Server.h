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

#include "RedisOption.h"
#include "CommandExecutor.h"
#include "RedisDef.h"

typedef struct ReplicationInfo {
    enum ReplicationRole {
        Master = 0,
        Slave
    };
    ReplicationRole role;
    int connected_slaves;
    std::string master_replid;
    int64_t master_repl_offset;

    // If the instance is a replica, these additional fields are provided
    int is_replica;
    std::string master_host;
    int master_port;

    // default constructor
    ReplicationInfo() : role(Master), connected_slaves(0), master_repl_offset(0), is_replica(0) {
        master_replid = DEFAULT_MASTER_REPLID;
    }
} ReplicationInfo;

enum SlaveState {
    WaitBGSaveStart = 1,
    WaitBGSaveEnd = 2,
};

typedef struct Client {
    int fd;                   /// the socket connects to this client
    int active;

    int is_slave;
    int slave_state;

    CommandExecutor executor; /// the executor for this client

    explicit Client(int fd_) : fd(fd_), active(1), is_slave(0) {}

    ~Client() {
        close(fd);
    }
} Client;

class Server {
private:
    static Server *instance_;
    static std::mutex m_;

    int server_fd_;                                  /// the fd of redis server to listen all requests
    int replica_fd_;                                 /// <replica only>: the fd in replica server connect to the master server
    std::vector<std::shared_ptr<Client>> clients_;   /// list of clients connect to this redis server

    std::mutex clients_mutex_;
    int port_;

    pid_t child_pid_;

    std::atomic_bool listening_;
    ReplicationInfo replication_info_;

    int child_info_pipe_[2];

private:
    Server() = default;

    int SetupReplica();

    int HandShake(int fd);

    int ProcessClientData(const fd_set &read_fds);

    void CheckChildrenDone();

    void OnSaveRdbBackgroundDone(const int exitcode);

    void FullSyncRdbToReplica(const std::shared_ptr<Client> &slave);

public:
    Server &operator=(const Server &sv) = delete;

    Server(const Server &rhs) = delete;

    ~Server();

    static Server *GetInstance();

    /// initialize the server from config, include sync data from master server (if this is a replica)
    int Setup();

    /// start the Redis server, already to listen all command after the preparing
    int Start();

    void SetConfig(const std::shared_ptr<RedisConfig> &cfg);

    /// send data over file descriptor fd. return < 0 while fail, otherwise return the total sent size
    static ssize_t SendData(const int fd, const char *data, ssize_t len);

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
};


#endif //REDIS_STARTER_CPP_SERVER_H
