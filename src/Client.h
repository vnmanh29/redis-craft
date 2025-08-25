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

#define CLIENT_REPLY_SUPPORTED (1<<0)
#define MASTER_REPLY_SUPPORTED (1<<1)
#define SLAVE_REPLY_SUPPORTED (1<<2)

enum ClientType {
    TypeRegular = (1 << 0),
    TypeMaster = (1 << 1),
    TypeSlave = (1 << 2),
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

    ~Client() {
        LOG_LINE();
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
    void WriteStreamFileAsync();

    int ConnectAsync(asio::io_context &io_ctx, const std::string &host, const std::string &port);

    int ClientType() const { return client_type_; }

    void SetClientType(int type) { client_type_ = type; }

    int SlaveState() const { return slave_state_; }

    void SetSlaveState(const int state) { slave_state_ = state; }

    void PropagateRdb(const std::string &rdb_path);

private:
    explicit Client(asio::io_context &io_ctx) : io_context_(io_ctx), sock_(io_ctx), bulk_(), client_type_(TypeRegular),
                                                slave_state_(SlaveState::SlaveOnline), file_(io_ctx),
                                                received_fullresync_(false), start_pos_(0), rdb_file_size_(0),
                                                rdb_read_size_(0), rdb_written_size_(0) {
        filename_ = get_rdb_file_path();
    }

    explicit Client(asio::io_context &io_ctx, tcp::socket &socket) : io_context_(io_ctx), sock_(std::move(socket)),
                                                                     bulk_(),
                                                                     client_type_(TypeRegular),
                                                                     slave_state_(SlaveState::SlaveOnline),
                                                                     file_(io_ctx), file_opened_(0),
                                                                     received_fullresync_(false), start_pos_(0),
                                                                     rdb_file_size_(0), rdb_read_size_(0),
                                                                     rdb_written_size_(0) {
        filename_ = get_rdb_file_path();
    }

    /// Handshake methods
    void SendPingAsync();

    void ReceivePongAndSendReplConf();

    void PrepareAndSendReplConfCapa();

    void PrepareAndSendPsync();

    void ReceivePsyncReply();

    int GetFullResync();

    int GetRdbFileSize();

    int TryWriteRdb();

    /// I/O file APIs
    void OpenFile();

    void FlushBuffer2File(size_t bytes);

    void ReadFile2Buffer();

    void CloseFile();

private:
    asio::io_context &io_context_;
    tcp::socket sock_;

    /// beginning position of input buffer.
    /// With write method, it is the beginning writable position, with read method, it's the beginning readable position
    size_t in_pos_;
    std::array<char, BUFFER_SIZE> in_buf_;

    std::vector<char> internal_buffer_;
    size_t start_pos_; /// starting position of internal_buffer_

    bool received_fullresync_;
    uint64_t rdb_file_size_, rdb_read_size_, rdb_written_size_;

    std::string filename_;
    int file_opened_;
    asio::posix::stream_descriptor file_; /// using async read/write to regular file
    int fd_;

    std::array<char, BULK_SIZE> out_buf_;
    std::vector<char> bulk_;

    CommandExecutor executor_; /// the executor for this client

    int client_type_;
    int slave_state_;
};


#endif //REDIS_CRAFT_CLIENT_H
