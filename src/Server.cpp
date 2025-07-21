//
// Created by Manh Nguyen Viet on 7/20/25.
//

#include "Server.h"
#include "CommandExecutor.h"
#include "Utils.h"
#include "RedisError.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

Server *Server::instance_{nullptr};
std::mutex Server::m_;

Server *Server::GetInstance() {
    std::lock_guard lock(m_);
    if (instance_ == nullptr) {
        instance_ = new Server();
    }
    return instance_;
}

Server::~Server() {
    for (int &fd: client_fds_) {
        close(fd);
    }

    close(replica_fd_);

    close(server_fd_);
}

static std::string get_response2(const std::string &query) {
    CommandExecutor ce;
    ce.ReceiveRequest(query);

    return ce.Execute();
}

static void receive_and_send(int fd) {
    std::cout << "Client connected\n";

    fd_set readfds;
    FD_ZERO(&readfds);
    FD_SET(fd, &readfds);
    struct timeval timeout;
    timeout.tv_sec = 2;
    timeout.tv_usec = 0;

    char buffer[4096] = {0};
    while (true) {
        select(fd + 1, &readfds, NULL, NULL, &timeout);
        ssize_t recv_bytes = recv(fd, (void *) buffer, 4095, 0);
        if (recv_bytes < 0) {
            std::cerr << "Receive from client failed: " << errno << ", msg: " << strerror(errno) << std::endl;
            break;
        } else if (recv_bytes == 0) {
            continue;
        }

        buffer[recv_bytes] = '\0';
        std::string received_data(buffer);

        std::string response = get_response2(received_data);

        ssize_t sent_bytes = send(fd, response.c_str(), response.size(), 0);
    }

    close(fd);
}

int Server::HandShake(int fd)
{
    fd_set readfds;
    FD_ZERO(&readfds);
    FD_SET(fd, &readfds);
    struct timeval timeout;
    timeout.tv_sec = 2;
    timeout.tv_usec = 0;

    /// TODO: build the list message instead of hardcode
    std::vector<std::vector<std::string>> handshake_msgs = {
            {"ping"},
            {"replconf", "listening-port", std::to_string(port_)},
            {"replconf", "capa", "psync2"},
            {"psync", "?", "-1"},
            };

    char buffer[4096] = {0};
    for (int idx = 0;idx < handshake_msgs.size(); ++idx)
    {
        // build RESP message
        auto& arr = handshake_msgs[idx];

        auto request = EncodeArr2RespArr(arr);
        ssize_t sent_bytes = send(fd, request.c_str(), request.size(), 0);
        if (sent_bytes < 0)
        {
            std::cerr << "Make hand-shake fail at step " << idx <<std::endl;
            return HandShakeSendError;
        }
//        printf("Sent %lu bytes, request %s\n", sent_bytes, request.c_str());

        int ready = select(fd + 1, &readfds, NULL, NULL, &timeout);
        if (ready < 0)
        {
            fprintf(stderr, "select the fail, erro %s\n", strerror(errno));
            return HandShakeFdError;
        }
        ssize_t recv_bytes = recv(fd, (void *) buffer, 4095, 0);
        if (recv_bytes < 0) {
            std::cerr << "Receive from client failed: " << errno << ", msg: " << strerror(errno) << std::endl;
            return HandShakeRecvError;
        } else if (recv_bytes == 0) {
            continue;
        }

        buffer[recv_bytes] = '\0';
        std::string received_data(buffer);
//        printf("Received %lu bytes: %s\n", recv_bytes, buffer);
        /// TODO: handle received data

    }

    close(fd);

    return 0;
}

int Server::Start() {
    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ < 0) {
        std::cerr << "Failed to create server socket\n";
        return 1;
    }

    // Since the tester restarts your program quite often, setting SO_REUSEADDR
    // ensures that we don't run into 'Address already in use' errors
    int reuse = 1;
    if (setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        std::cerr << "setsockopt failed\n";
        return 1;
    }

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port_);

    if (bind(server_fd_, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) {
        std::cerr << "Failed to bind to port 6379\n";
        return 1;
    }

    int connection_backlog = 5;
    if (listen(server_fd_, connection_backlog) != 0) {
        std::cerr << "listen failed\n";
        return 1;
    }

    struct sockaddr_in client_addr;
    int client_addr_len = sizeof(client_addr);
    std::cout << "Waiting for a client to connect...\n";

    while (true) {
        int client_fd = accept(server_fd_, (struct sockaddr *) &client_addr, (socklen_t *) &client_addr_len);
        if (client_fd < 0) {
            std::cerr << "Accept failed\n";
            return -1;
        }

        std::thread t(receive_and_send, client_fd);

        client_fds_.push_back(client_fd);
        t.detach();
    }

}

void Server::SetConfig(const std::shared_ptr<RedisConfig> &cfg) {
    if (cfg) {
        /// TODO: add more config properties belong to network???
        port_ = cfg->port;

        if (cfg->is_replica)
        {
            replication_info_.role = ReplicationInfo::ReplicationRole::Slave;

            replication_info_.is_replica = cfg->is_replica;
            replication_info_.master_host = cfg->master_host;
            replication_info_.master_port = cfg->master_port;
        }
    }
}

std::string Server::ShowReplicationInfo() const {
    std::stringstream ss;

    ss << "role:";
    ss << ((replication_info_.role == ReplicationInfo::Master) ? "master" : "slave") << CRLF;

    ss << "connected_slave:" << replication_info_.connected_slaves << CRLF;
    ss << "master_replid:" << replication_info_.master_replid << CRLF;
    ss << "master_repl_offset:" << replication_info_.master_repl_offset << CRLF;

    if (replication_info_.is_replica)
    {
        ss << "master_host:" << replication_info_.master_host << CRLF;
        ss << "master_port:" << replication_info_.master_port << CRLF;
    }

    return ss.str();
}

int Server::Setup() {
    /// 1. setup the replica
    int ret = SetupReplica();

    return ret;
}

int Server::SetupReplica() {
    if (!replication_info_.is_replica)
        return 0;

    replica_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (replica_fd_ < 0)
    {
        std::cerr << "Fail to create replica fd" << std::endl;
        return CreateSocketError;
    }

    // Step 2: Resolve hostname to IP address
    struct hostent *master_server = gethostbyname(replication_info_.master_host.c_str());
    if (master_server == NULL) {
        fprintf(stderr, "No such host: %s\n", replication_info_.master_host.c_str());
        return GetHostNameError;
    }

    // Step 3: Set up the sockaddr_in struct
    struct sockaddr_in master_addr;
    memset(&master_addr, 0, sizeof(master_addr));
    master_addr.sin_family = AF_INET;
    master_addr.sin_port = htons(replication_info_.master_port);
//    master_addr.sin_addr.s_addr = INADDR_ANY;
    memcpy(&master_addr.sin_addr.s_addr, master_server->h_addr, master_server->h_length);

    // Step 4: Connect to the server
    if (connect(replica_fd_, (struct sockaddr*)&master_addr, sizeof(master_addr)) < 0) {
        perror("connect failed");
        return SocketConnectError;
    }

    int ret = HandShake(replica_fd_);

    return ret;
}
