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
#include <fcntl.h>
#include <netinet/tcp.h> // Required for TCP_NODELAY

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

    close(replica_fd_);

    close(server_fd_);
}

static std::string get_response2(const std::string &query) {
    CommandExecutor ce;
//    ce.ReceiveRequest(query);

//    return ce.Execute();
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
    listening_ = true;
//    std::thread t(&Server::ReceiveAndReply, this);

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

    // Set the server socket to non-blocking mode
    if (fcntl(server_fd_, F_SETFL, O_NONBLOCK) < 0) {
        perror("fcntl failed");
        close(server_fd_);
        exit(EXIT_FAILURE);
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
//            std::cerr << "Accept failed\n";
//            return -1;
        }
        else {
//            int flags = fcntl(client_fd, F_GETFL, 0);
//            if (flags == -1)
//            {
//                fprintf(stderr, "set client fd %d to non-blocking mode fail\n", client_fd);
//                fflush(stderr);
//                continue;
//            }
//            else
//            {
//                fprintf(stdout, "Connected to the client through fd %d\n", client_fd);
//                fflush(stdout);
//                fcntl(client_fd, F_SETFL, flags | O_NONBLOCK);
                int yes = 1;
                std::lock_guard lock(clients_mutex_);
                clients_.push_back(std::make_shared<Client>(client_fd));
//            }
        }

        ReceiveAndReply();
    }

//    t.join();

    return 0;
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
    if (ret < 0)
    {
        fprintf(stderr, "Handshake fail %d\n", ret);
        return ret;
    }

    return ret;
}

int Server::ReceiveAndReply() {
//    std::cout << "Client connected in fd " << fd << std::endl;

//    while (listening_.load())
//    {
        int max_fd = 0;
        fd_set readfds;

        struct timeval timeout;
        timeout.tv_sec = 2;
        timeout.tv_usec = 0;

        {
            std::lock_guard<std::mutex> lock(clients_mutex_);
            FD_ZERO(&readfds);
            for (const auto& client: clients_) {
                if (client->active)
                {
                    FD_SET(client->fd, &readfds);
                    max_fd = std::max(max_fd, client->fd);
                }
            }
        }

        char buffer[4096] = {0};

        int activity = select(max_fd + 1, &readfds, NULL, NULL, &timeout);
        if (activity <= 0)
        {
            return -1;
        }
        else
        {
//            std::cerr << "there are " << activity << std::endl;
        }

        std::lock_guard lock(clients_mutex_);
        for (auto &client : clients_)
        {
            if (client->active == 0)
                continue;

            int fd = client->fd;
            if (FD_ISSET(fd, &readfds))
            {
                ssize_t recv_bytes = recv(fd, (void *) buffer, 4095, 0);
                if (recv_bytes < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        // No data available yet, try again later or use select/poll/epoll
                        // to wait for data
                        continue;
                    } else {
                        // A real error occurred
                        fprintf(stderr, "Receive from client fd %d failed: %d, msg %s\n", fd, errno, strerror(errno));
                        continue;
                    }
                } else if (recv_bytes == 0) {
                    // Connection closed by peer
                    std::cerr << "EOF of file in fd " <<  fd << std::endl;
                    client->active = 0;
    //                continue;
                    return SocketConnectError;
                }

                buffer[recv_bytes] = '\0';
                std::string received_data(buffer);
//                std::cout << "received data " << received_data << " in " << fd << std::endl;

                /// handle the new data
                int ret = client->executor.ReceiveData(received_data);
                if (ret == 0)
                {
//                    std::cout << "execute data " << received_data << " and send through fd " << fd << std::endl;
                    /// execute the query and respond
                    client->executor.Execute(fd);
                }
                else
                {
                    std::cerr << "Received data " << received_data << " failed" << std::endl;
                }

                return 0;
            }
        }
//    }

    return 0;
}

ssize_t Server::SendData(const int fd, const std::string &response) {
//    std::cout << "Send through fd " << fd << " response: " << response << std::endl;
    size_t total_sent = 0, sent = 0;
    const char* data = response.data();
    size_t total_len = response.size();

    fd_set writefds;
    FD_ZERO(&writefds);
    FD_SET(fd, &writefds);

    struct timeval tv;
    tv.tv_sec = 2;
    tv.tv_usec = 0;

    int activity = select(fd + 1, NULL, &writefds, NULL, &tv);
    if (activity <= 0)
        return SentDataError;


    int nodelay = 1;
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (char *)&nodelay, sizeof(nodelay)) < 0)
    {
        std::cerr << "set fd " << fd << " opt NODELAY fail" << std::endl;
    }
    else
    {
//        std::cerr << "set fd " << fd << " opt NODELAY success" << std::endl;
    }

    if (FD_ISSET(fd, &writefds))
    {
        while (total_sent < total_len)
        {
            sent = send(fd, data + total_sent, total_len - total_sent, 0);
//            sent = write(fd, data, total_sent);
            if (sent == -1)
            {
                if (errno == EWOULDBLOCK || errno == EAGAIN) {
                    // Socket buffer is full, wait and retry later (e.g., using select/poll)
                    // In a real application, you'd use select/poll to wait for writability
                    // For simplicity here, we might just sleep briefly or yield control
//                    usleep(1000); // Sleep for 1ms, for example
                    continue; // Try sending again
                }
                std::cerr << "Sent " << total_sent << " bytes, error " << strerror(errno) << std::endl;
                return Error::SentDataError;
            }
            else if (sent == 0)
            {
                return Error::SentDataError;
            }
            total_sent += sent;
        }

//        std::cout << "Send through fd " << fd << " successfully " << total_sent << " bytes" << std::endl;

        usleep(10000);

        return total_sent;
    }

    return 0;
}
