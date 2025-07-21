//
// Created by Manh Nguyen Viet on 7/20/25.
//

#ifndef REDIS_STARTER_CPP_SERVER_H
#define REDIS_STARTER_CPP_SERVER_H

#include <mutex>
#include <vector>
#include <memory>

#include "RedisOption.h"

typedef struct ReplicationInfo
{
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


class Server {
private:
    static Server* instance_;
    static std::mutex m_;

    int server_fd_, replica_fd_;
    std::vector<int> client_fds_;
    int port_;

    ReplicationInfo replication_info_;

private:
    Server() = default;

    int SetupReplica();

    int HandShake(int fd);

public:
    Server& operator=(const Server& sv) = delete;
    Server(const Server& rhs) = delete;

    ~Server();

    static Server* GetInstance();

    /// initialize the server from config, include sync data from master server (if this is a replica)
    int Setup();

    /// start the Redis server, already to listen all command after the preparing
    int Start();

    void SetConfig(const std::shared_ptr<RedisConfig>& cfg);

    std::string ShowReplicationInfo() const;
};


#endif //REDIS_STARTER_CPP_SERVER_H
