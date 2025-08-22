//
// Created by Manh Nguyen Viet on 8/18/25.
//

#ifndef REDIS_CRAFT_CLIENT_H
#define REDIS_CRAFT_CLIENT_H

#include "CommandExecutor.h"
#include "RedisDef.h"
#include "RedisError.h"
#include "asio.hpp"

#include <memory>
#include <string>
#include <vector>

using asio::ip::tcp;

/// States of the slave in master server
enum SlaveState {
    SlaveOnline = 0,
    WaitBGSaveStart = 1,
    WaitBGSaveEnd = 2,
};

enum ClientType {
    TypeRegular = 0,
    TypeMaster = 1,
    TypeSlave = 2,
};

class Client : public std::enable_shared_from_this<Client> {

public:
    typedef std::shared_ptr<Client> pClient;

    static pClient create(asio::io_context &io_ctx) {
        return pClient(new Client(io_ctx));
    }

    static pClient CreateBindSocket(asio::io_context &io_ctx, tcp::socket &&socket) {
        return pClient(new Client(io_ctx, socket));
    }

    tcp::socket &Socket() {
        return sock_;
    }

    void ReadAsync();

    void WriteAsync(const std::string &reply);

    /// read data from tcp::socket and write to stream @param pfile .
    /// @param total_size: maximum size need to read from
    /// @param current_read: current size that read
    void ReadBulkAsyncWriteFile(size_t total_size, size_t current_read, FILE *pfile);

    /// send data from @param pfile stream through tpc:socket
    void WriteStreamFileAsync(std::shared_ptr<std::ifstream> file);

    int ConnectSync(asio::io_context &io_ctx, const std::string &host, const std::string &port);

    int WriteSync(const std::string &s);

    int ReadSyncWithLength(std::string &s, size_t length);

    int ReadSyncNewLine(std::string &s);

    int ClientType() const { return client_type_; }

    void SetClientType(int type) { client_type_ = type; }

    int SlaveState() const { return slave_state_; }

    void SetSlaveState(const int state) { slave_state_ = state; }

    ~Client() {
        LOG_LINE();
    }

private:
    explicit Client(asio::io_context &io_ctx) : sock_(io_ctx), bulk_(), client_type_(TypeRegular),
                                                slave_state_(SlaveState::SlaveOnline) {
    }

    explicit Client(asio::io_context &io_ctx, tcp::socket &socket) : sock_(std::move(socket)), bulk_(),
                                                                     client_type_(TypeRegular),
                                                                     slave_state_(SlaveState::SlaveOnline) {}

private:
    tcp::socket sock_;

    std::array<char, BUFFER_SIZE> out_buf_;
    std::array<char, BUFFER_SIZE> in_buf_;
    std::vector<char> bulk_;

    CommandExecutor executor_; /// the executor for this client

    int client_type_;
    int slave_state_;


};


#endif //REDIS_CRAFT_CLIENT_H
