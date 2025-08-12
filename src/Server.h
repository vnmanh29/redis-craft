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

/// States of the slave in master server
enum SlaveState {
    SlaveOnline = 0,
    WaitBGSaveStart = 1,
    WaitBGSaveEnd = 2,
};

#define BUF_SIZE 4096

typedef struct Client {
    int active;
    int fd;                        /// the socket connects to this client
    char out_buf[BUF_SIZE];
    size_t buf_pos;                /// total bytes in buffer
    size_t sent_len;               /// how many bytes already sent -> need to send the data in buffer range [sent_len -> buf_pos-1]

    int is_slave;
    int slave_state;

    CommandExecutor executor; /// the executor for this client

    explicit Client(int fd_) : fd(fd_), active(1), is_slave(0) {}

    ~Client() {
        close(fd);
    }

    size_t FillStringToOutBuffer(const std::string &s) {
        if (buf_pos + s.size() >= BUF_SIZE) {
            LOG_INFO(TAG, "Overflow, retry later");
            return 0;
        }
        memcpy(out_buf + buf_pos, s.c_str(), s.size());
        buf_pos += s.size();

        /// return the number of unsent bytes in buffer
        return buf_pos - sent_len + 1;
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

    std::unordered_map<std::string, RedisCmd *> global_commands_;

private:
    Server() = default;

    int SetupReplica();

    int SetupCommands();

    int HandShake(int fd);

    int ProcessClientData(const fd_set &read_fds);

    void CheckChildrenDone();

    void OnSaveRdbBackgroundDone(const int exitcode);

    ssize_t FullSyncRdbToReplica(const std::shared_ptr<Client> &slave);

    void AddCommand(const std::string& command, CommandType type, uint64_t flag);

    void AddCommand(const std::string& command, const std::string& subcmd, CommandType type, uint64_t flag);

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

    std::vector<std::shared_ptr<Client>> GetClients() const { return clients_; }

    RedisCmd *GetRedisCommand(const std::string& cmd_name);
};


#endif //REDIS_STARTER_CPP_SERVER_H
