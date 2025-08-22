//
// Created by Manh Nguyen Viet on 8/18/25.
//

#include "Client.h"
#include "Server.h"

void Client::ReadAsync() {
    auto self(shared_from_this());
    sock_.async_read_some(asio::buffer(in_buf_),
                          [this, self](const std::error_code &error, const size_t byte_transferred) {
                              if (!error) {
                                  LOG_DEBUG("Client", "read %s from sock %d",
                                            std::string(in_buf_.data(), byte_transferred).c_str(),
                                            sock_.native_handle());
                                  LOG_LINE();
                                  /// try to decode received data
                                  executor_.ReceiveDataAndExecute(std::string(in_buf_.data(), byte_transferred),
                                                                  shared_from_this());
                                  /// continue to receive data
                                  ReadAsync();
                              } else if (error == asio::error::eof) {
                                  /// FIXME: handle this case
                                  LOG_ERROR(TAG, "Peer was closed, socket %d", sock_.native_handle());
                              } else {
                                  /// FIXME: handle other error
                              }
                          });
}

void Client::WriteAsync(const std::string &reply) {
    LOG_DEBUG("Client", "write reply %s, sock %d, client type %d", reply.c_str(), sock_.native_handle(), client_type_);
    /// no need to reply to master server
    /// FIXME: more details
    if (client_type_ == ClientType::TypeMaster)
        return;

    sock_.async_write_some(asio::buffer(reply), [&](const std::error_code &error, const size_t byte_transferred) {
        if (error) {
            LOG_ERROR(TAG, "Send reply %s fail %d: %s", reply.c_str(), error.value(), error.message().c_str());
            return;
        } else {
            LOG_LINE();
        }
    });
}

void Client::ReadBulkAsyncWriteFile(const size_t total_size, size_t current_read, FILE *pfile) {
    LOG_DEBUG("Client", "current %lu, total %lu", current_read, total_size);
    auto self(shared_from_this());
    sock_.async_read_some(asio::buffer(in_buf_),
                          [total_size, current_read, this, self, pfile](const std::error_code &error,
                                                                        const size_t byte_transferred) {
                              LOG_LINE();
                              if (!error) {
                                  bulk_.insert(bulk_.end(), in_buf_.begin(), in_buf_.begin() + byte_transferred);
                                  size_t read_bytes = current_read + byte_transferred;
                                  LOG_DEBUG("Client", "bulk.size %d, read_bytes %d, total size %d", bulk_.size(),
                                            read_bytes, total_size);
                                  if (bulk_.size() >= BULK_SIZE || read_bytes >= total_size) {
                                      LOG_DEBUG(TAG, "write %lu bytes, current_read %lu", byte_transferred,
                                                read_bytes);
                                      LOG_LINE();
                                      fwrite(bulk_.data(), sizeof(char), bulk_.size(), pfile);
                                      LOG_LINE();
                                      bulk_.clear();
                                      /// write enough, skip other data
                                      if (read_bytes >= total_size) {
                                          LOG_LINE();
                                          fclose(pfile);
                                          LOG_LINE();
                                          Server::GetInstance()->SetReplicaState(
                                                  ReplicationState::ReplStateSynced);
                                          ReadAsync();
                                          LOG_LINE();
                                      }
                                  }

                                  /// only call it when expect more data
                                  if (read_bytes < total_size) {
                                      ReadBulkAsyncWriteFile(total_size, read_bytes, pfile);
                                  }
                                  LOG_LINE();
                              } else {
                                  /// FIXME: handle error???
                                  /// mark the server state: repl fail
                                  LOG_LINE();
                                  Server::GetInstance()->SetReplicaState(ReplicationState::ReplStateConnecting);
                              }
                          });
}

int Client::ConnectSync(asio::io_context &io_ctx, const std::string &host, const std::string &port) {
    try {
        LOG_DEBUG(TAG, "connect to host %s, port %s", host.c_str(), port.c_str());
        tcp::resolver resolver(io_ctx);
        tcp::resolver::results_type endpoints =
                resolver.resolve(host, port);
        asio::connect(sock_, endpoints);

        return RedisSuccess;
    }
    catch (std::exception &ex) {
        LOG_ERROR(TAG, "Connect to %s, port %s throw exception %s", host.c_str(),
                  port.c_str(), ex.what());
        return SocketConnectError;
    }
}

int Client::WriteSync(const std::string &s) {
    try {
        LOG_DEBUG("Client", "write to socket: %s", s.c_str());
        return sock_.write_some(asio::buffer(s));
    }
    catch (std::exception &ex) {
        LOG_ERROR("Asio", "write sync msg %s fail", s.c_str());
        return SyncWriteError;
    }
}

int Client::ReadSyncWithLength(std::string &s, const size_t length) {
    try {
//        asio::read(sock_, asio::buffer(in_buf_, length));
        sock_.read_some(asio::buffer(in_buf_, length));
        s = std::move(std::string(in_buf_.begin(), in_buf_.begin() + length));
        LOG_DEBUG("Client", "read from socket: %s", s.c_str());
        return RedisSuccess;
    }
    catch (std::exception &ex) {
        LOG_ERROR("Asio", "Read with max length %lu fail %s", length, ex.what());
        return SyncReadError;
    }
}

int Client::ReadSyncNewLine(std::string &response) {
    try {
        size_t read_bytes = 0;
        response = std::string(1, '\0');
        while (read_bytes < 256) {
            std::vector<char> c(1);
            int len = asio::read(sock_, asio::buffer(c));
            if (c[0] == '\n') {
                break;
            } else {
                response.back() = c[0];
                response.push_back('\0');
                read_bytes += len;
            }
        }

        return read_bytes;
    }
    catch (std::exception &ex) {
        LOG_ERROR(TAG, "throw ex %s", ex.what());
        return SyncReadError;
    }
}

void Client::WriteStreamFileAsync(std::shared_ptr<std::ifstream> file) {
    long read_size = file->read(&out_buf_[0], 4096).gcount();
    LOG_DEBUG(TAG, "Read %lu bytes from file, eof %d", read_size, file->eof());
    auto self(shared_from_this());
    sock_.async_write_some(asio::buffer(out_buf_, read_size),
                           [file, this, self](const std::error_code &error, const int transferred) {
                               if (!error) {
                                   LOG_DEBUG(TAG, "Write %d bytes to socket %d", transferred, sock_.native_handle());
                                   if (file->eof() || file->bad() || file->fail()) {
                                       LOG_ERROR("File", "End of reading file");
                                       slave_state_ = SlaveState::SlaveOnline;
                                       file->close();
                                   } else {
                                       WriteStreamFileAsync(file);
                                   }
                               } else {
                                   if (error == asio::error::eof) {
                                       LOG_INFO(TAG, "End of file");
                                       slave_state_ = SlaveState::SlaveOnline;
                                   }
                                   file->close();
                                   LOG_ERROR(TAG, "Error reading from file: %s", error.message().c_str());
                               }
                           });
}
