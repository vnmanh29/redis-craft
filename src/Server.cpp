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
    LOG_INFO(TAG, "Client connected");

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

int Server::HandShake(int fd) {
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
            {"replconf", "capa",           "psync2"},
            {"psync",    "?",              "-1"},
    };

    char buffer[4096] = {0};
    for (int idx = 0; idx < handshake_msgs.size(); ++idx) {
        // build RESP message
        auto &arr = handshake_msgs[idx];

        auto request = EncodeArr2RespArr(arr);
        ssize_t sent_bytes = send(fd, request.c_str(), request.size(), 0);
        if (sent_bytes < 0) {
            LOG_ERROR(TAG, "Make hand-shake fail at step %d", idx)
            return HandShakeSendError;
        }

        int ready = select(fd + 1, &readfds, NULL, NULL, &timeout);
        if (ready < 0) {
            LOG_ERROR(TAG, "select the fail, errno %s", strerror(errno));
            return HandShakeFdError;
        }
        ssize_t recv_bytes = recv(fd, (void *) buffer, 4095, 0);
        if (recv_bytes < 0) {
            LOG_ERROR(TAG, "Receive from client failed: %d, msg: %s", errno, strerror(errno));
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
//    std::thread t(&Server::ProcessClientData, this);

    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ < 0) {
        LOG_ERROR(TAG, "Failed to create server socket");
        return 1;
    }

    // Since the tester restarts your program quite often, setting SO_REUSEADDR
    // ensures that we don't run into 'Address already in use' errors
    int reuse = 1;
    if (setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        LOG_ERROR(TAG, "setsockopt failed");
        return 1;
    }

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port_);

    if (bind(server_fd_, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) {
        LOG_ERROR(TAG, "Failed to bind to port %d", DEFAULT_REDIS_PORT)
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
    LOG_INFO(TAG, "Waiting for a client to connect...");

    while (true) {
        /// check the end of child proc
        CheckChildrenDone();

        int max_fd = server_fd_;

        /// initialize the read_fd
        fd_set read_fds;
        FD_ZERO(&read_fds);
        /// add server socket to monitor for new connection
        FD_SET(server_fd_, &read_fds);
        /// add all active clients to monitor for new connection
        {
            std::lock_guard lock(clients_mutex_);
            for (auto &client: clients_) {
                if (client->active) {
                    FD_SET(client->fd, &read_fds);
                    max_fd = std::max(max_fd, client->fd);
                }
            }
        }

        struct timeval timeout;
        timeout.tv_sec = 1;
        timeout.tv_usec = 0;

        // probe the activity
        int activity = select(max_fd + 1, &read_fds, NULL, NULL, &timeout);
        if (activity < 0) {
            /// FIXME: handle error
        } else if (activity == 0) {
            /// timeout, continue probe
            continue;
        }

        /// check for new connection on server socket
        if (FD_ISSET(server_fd_, &read_fds)) {
            int client_fd = accept(server_fd_, (struct sockaddr *) &client_addr, (socklen_t *) &client_addr_len);
            if (client_fd >= 0) {
                std::lock_guard lock(clients_mutex_);
                clients_.push_back(std::make_shared<Client>(client_fd));
                LOG_INFO(TAG, "New client connected on fd %d", client_fd);
            }
        }

        ProcessClientData(read_fds);
    }

    return 0;
}

void Server::SetConfig(const std::shared_ptr<RedisConfig> &cfg) {
    if (cfg) {
        /// TODO: add more config properties belong to network???
        port_ = cfg->port;

        if (cfg->is_replica) {
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

    if (replication_info_.is_replica) {
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
    if (replica_fd_ < 0) {
        LOG_ERROR(TAG, "Fail to create replica fd")
        return CreateSocketError;
    }

    // Step 2: Resolve hostname to IP address
    struct hostent *master_server = gethostbyname(replication_info_.master_host.c_str());
    if (master_server == NULL) {
        LOG_ERROR(TAG, "No such host: %s\n", replication_info_.master_host.c_str());
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
    if (connect(replica_fd_, (struct sockaddr *) &master_addr, sizeof(master_addr)) < 0) {
        perror("connect failed");
        return SocketConnectError;
    }

    int ret = HandShake(replica_fd_);
    if (ret < 0) {
        LOG_ERROR(TAG, "Handshake fail %d", ret);
        return ret;
    }

    return ret;
}

int Server::ProcessClientData(const fd_set &read_fds) {

    char buffer[4096] = {0};

    std::lock_guard lock(clients_mutex_);
    /// probe the data from the client sockets
    for (auto &client: clients_) {
        if (client->active == 0)
            continue;

        int fd = client->fd;
        if (FD_ISSET(fd, &read_fds)) {
            ssize_t recv_bytes = recv(fd, buffer, 4095, 0);
            if (recv_bytes < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    /// no data available yet, retry later
                    continue;
                } else {
                    /// a real error occurred
                    LOG_ERROR(TAG, "Received from fd %d, error %d, msg %s", fd, errno, strerror(errno));
                    continue;
                }
            } else if (recv_bytes == 0) {
                /// connection was closed by peer
                client->active = 0;
                LOG_ERROR(TAG, "closed connection on fd %d", fd);
                continue;
            }

            buffer[recv_bytes] = '\0';
            std::string received_data(buffer);
            /// handle the new data
            int ret = client->executor.ReceiveData(received_data);
            if (ret == 0) {
                LOG_DEBUG(TAG, "execute data %s and send through fd %d", buffer, fd);
                /// execute the query and respond
                client->executor.Execute(client);
            } else {
                LOG_DEBUG(TAG, "Parse received data %s failed", buffer)
            }
        }
    }

    return 0;
}

ssize_t Server::SendData(const int fd, const char *data, ssize_t len) {
    size_t total_sent = 0, sent = 0;
    size_t total_len = len;

    fd_set writefds;
    FD_ZERO(&writefds);
    FD_SET(fd, &writefds);

    struct timeval tv;
    tv.tv_sec = 2;
    tv.tv_usec = 0;

    int activity = select(fd + 1, NULL, &writefds, NULL, &tv);
    if (activity <= 0)
        return SentDataError;

//
//    int nodelay = 1;
//    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (char *)&nodelay, sizeof(nodelay)) < 0)
//    {
//        std::cerr << "set fd " << fd << " opt NODELAY fail" << std::endl;
//    }
//    else
//    {
////        std::cerr << "set fd " << fd << " opt NODELAY success" << std::endl;
//    }

    if (FD_ISSET(fd, &writefds)) {
        while (total_sent < total_len) {
            sent = send(fd, data + total_sent, total_len - total_sent, 0);
            if (sent == -1) {
                if (errno == EWOULDBLOCK || errno == EAGAIN) {
                    // Socket buffer is full, wait and retry later (e.g., using select/poll)
                    // In a real application, you'd use select/poll to wait for writability
                    // For simplicity here, we might just sleep briefly or yield control
//                    usleep(1000); // Sleep for 1ms, for example
                    continue; // Try sending again
                }
                LOG_ERROR(TAG, "Sent %ld bytes, error %s", total_sent, strerror(errno));
                return Error::SentDataError;
            } else if (sent == 0) {
                return Error::SentDataError;
            }
            total_sent += sent;
        }

//        std::cout << "Send through fd " << fd << " successfully " << total_sent << " bytes" << std::endl;

//        usleep(10000);

        return total_sent;
    }

    return 0;
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
    int status;
    pid_t pid;
    if ((pid = waitpid(-1, &status, WNOHANG)) != 0) {
        if (pid == -1) {
            /// return there is not any waiting child proc
            if (errno == ECHILD)
                return;

            LOG_ERROR(TAG, "waitpid() returned an error: %s, child proc %d", strerror(errno), child_pid_);

            /// handle error
        } else {
            /// handle by exit code, the child proc type
            int exitcode = WIFEXITED(status) ? WEXITSTATUS(status) : -1;

            if (pid == child_pid_) {
                LOG_DEBUG(TAG, "Handle the child proc %d exit with status %d, exitcode %d", pid, status, exitcode)
                /// TODO: handle the result of child proc in parent proc
                OnSaveRdbBackgroundDone(exitcode);
            } else {
                LOG_ERROR(TAG, "Conflict between child proc %d and finished pid %d", child_pid_, pid)
                /// TODO: handle conflict
            }
        }
    }
}

void Server::OnSaveRdbBackgroundDone(const int exitcode) {
    /// TODO: update state???

    /// possibly there are some slaves waiting for. Transfer dump file .rdb
    if (exitcode == 0) {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        for (auto &client: clients_) {
            if (client->is_slave && client->slave_state == SlaveState::WaitBGSaveEnd) {
                FullSyncRdbToReplica(client);
            }
        }
    }
}

void Server::FullSyncRdbToReplica(const std::shared_ptr<Client> &slave) {
    std::string rdb_file_path = Database::GetInstance()->GetRdbPath();
    char prefix_rdb[20] = {0};
    ssize_t rdb_size = 0;

    LOG_DEBUG(TAG, "start send replica prefix through fd %d", slave->fd);
    struct stat st;
    if (RdbStat(rdb_file_path, st) < 0) {
        std::cerr << "Stat() file " << rdb_file_path << "error " << strerror(errno) << std::endl;
        return;
    }
    else {
        rdb_size = st.st_size;
    }

    snprintf(prefix_rdb, 19, "$%lld\r\n", rdb_size);
    int slave_fd = slave->fd;

    /** send data */
    ///  First, send the length of rdb
    SendData(slave_fd, prefix_rdb, strlen(prefix_rdb));

    ///  Second, read and send binary rdb
    FILE *rdb_file = fopen(rdb_file_path.c_str(), "rb");
    if (rdb_file) {
        ssize_t offset = 0;
        char buf[16 * 1024] = {0};
        while (offset < rdb_size) {
            size_t read_bytes = fread(buf, sizeof(char), 16 * 1024, rdb_file);
            if (read_bytes > 0) {
                ssize_t sent_bytes = SendData(slave_fd, buf, read_bytes);
                if (sent_bytes < 0) {
                    /// sent fail
                }
                offset += read_bytes;
            } else {
                if (ferror(rdb_file)) {
                    LOG_DEBUG(TAG, "Sent %ld bytes, there are total %ld bytes", offset, rdb_size);
                }
                break;
            }
        }
        fclose(rdb_file);
    } else {
        std::cerr << "Open file " << rdb_file_path << " error: " << strerror(errno) << std::endl;
    }
}
