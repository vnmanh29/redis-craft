//
// Created by Manh Nguyen Viet on 8/18/25.
//

#include "Client.h"
#include "Server.h"

void Client::ReadAsync() {
    LOG_DEBUG("Client", "Wait read data from client sock %d, is open %d", sock_.native_handle(), sock_.is_open());
    auto self(shared_from_this());
    sock_.async_read_some(asio::buffer(in_buf_),
                          [this, self](const std::error_code &error, const size_t byte_transferred) {
                              if (!error) {
                                  LOG_ERROR("Client", "read %s from sock %d",
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
                                  LOG_ERROR(TAG, "error receive data %s on sock %d", error.message().c_str(),
                                            sock_.native_handle());
                                  /// FIXME: handle other error
                              }
                          });
}

void Client::WriteAsync(const std::string &reply) {
    LOG_INFO("Client", "write reply %s, sock %d, client type %d, executor flags %d", reply.c_str(),
             sock_.native_handle(), client_type_, executor_.Flags());
    /// no need to reply to master server
    /// FIXME: more details
    if (!(client_type_ & executor_.Flags()))
        return;

    LOG_DEBUG("Client", "write reply %s, sock %d, client type %d", reply.c_str(), sock_.native_handle(), client_type_);
    sock_.async_write_some(asio::buffer(reply), [&reply](const std::error_code &error, const size_t byte_transferred) {
        if (error) {
            LOG_ERROR(TAG, "Send reply %s fail %d: %s", reply.c_str(), error.value(), error.message().c_str());
            return;
        } else {
            LOG_LINE();
        }
    });
}

void Client::ReadBulkAsyncWriteFile(const size_t total_size, size_t current_read, FILE *pfile) {
    LOG_DEBUG("Client", "current %lu, total %lu from sock %d", current_read, total_size, sock_.native_handle());
    auto self(shared_from_this());
    sock_.async_read_some(asio::buffer(in_buf_),
                          [total_size, current_read, this, self, pfile](const std::error_code &error,
                                                                        const size_t byte_transferred) {
                              LOG_LINE();
                              if (!error) {
                                  bulk_.insert(bulk_.end(), in_buf_.begin(), in_buf_.begin() + byte_transferred);
                                  size_t read_bytes = current_read + byte_transferred;
                                  LOG_DEBUG("Client", "bulk.size %d, read_bytes %d, total size %d, sock %d",
                                            bulk_.size(),
                                            read_bytes, total_size, sock_.native_handle());
                                  if (bulk_.size() >= BULK_SIZE || read_bytes >= total_size) {
                                      LOG_DEBUG(TAG, "write %lu bytes, current_read %lu", byte_transferred,
                                                read_bytes);
                                      LOG_LINE();
                                      //   LOG_ERROR(TAG, "bulk %s", bulk_.data());
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

int Client::ConnectAsync(asio::io_context &io_ctx, const std::string &host, const std::string &port) {
    try {
        LOG_DEBUG(TAG, "connect to host %s, port %s", host.c_str(), port.c_str());
        tcp::resolver resolver(io_ctx);
        tcp::resolver::results_type endpoints =
                resolver.resolve(host, port);

        auto self(shared_from_this());
        asio::async_connect(sock_, endpoints, [this, self](const std::error_code &ec, const tcp::endpoint &endpoint) {
            if (!ec) {
                LOG_DEBUG("HandShake", "Connect to host %s, port %d success", endpoint.address().to_string().c_str(),
                          endpoint.port());
                /// ping to master
                SendPingAsync();
            }
        });

        return RedisSuccess;
    }
    catch (std::exception &ex) {
        LOG_ERROR(TAG, "Connect to %s, port %s throw exception %s", host.c_str(),
                  port.c_str(), ex.what());
        return SocketConnectError;
    }
}

void Client::WriteStreamFileAsync() {
    if (!file_opened_) {
        OpenFile();
    }

    size_t read_size = read(fd_, out_buf_.data(), BULK_SIZE);
    if (read_size <= 0) {
        LOG_INFO("Client", "End of sending file %s", filename_.c_str());
        slave_state_ = SlaveState::SlaveOnline;
        CloseFile();
        return;
    } else {
        LOG_DEBUG(TAG, "Read %lu bytes from file %s", read_size, filename_.c_str());
#if ZDEBUG
        LOG_DEBUG(TAG, "Hexdata:");
        showBinFile(filename_);
        printf("\n");
        fflush(stdout);
#endif // ZDEBUG
    }

    auto self(shared_from_this());
    sock_.async_write_some(asio::buffer(out_buf_, read_size),
                           [this, self](const std::error_code &error, const int transferred) {
                               if (!error) {
                                   LOG_DEBUG(TAG, "Write %d bytes to socket %d", transferred, sock_.native_handle());
                                   WriteStreamFileAsync();
                               } else {
                                   LOG_ERROR(TAG, "Error reading from file: %s", error.message().c_str());
                               }
                           });
}

void Client::SendPingAsync() {
    LOG_DEBUG("HandShake", "Start sending PING through socket %d", sock_.native_handle());
    auto self(shared_from_this());
    sock_.async_write_some(asio::buffer(EncodeArr2RespArr({"PING"})),
                           [self, this](const std::error_code &ec, const size_t &bytes) {
                               if (!ec) {
                                   LOG_DEBUG("HandShake", "Finish sending PING through socket %d, try to receive PONG",
                                             sock_.native_handle());
                                   ReceivePongAndSendReplConf();
                               } else {
                                   LOG_ERROR("HandShake", "Send PING fail %s", ec.message().c_str());
                               }
                           });

}

void Client::ReceivePongAndSendReplConf() {
    LOG_DEBUG("HandShake", "Start handshake replica config");
    auto self(shared_from_this());
    sock_.async_read_some(asio::buffer(in_buf_),
                          [this, self](const std::error_code &ec, const size_t byte_transferred) {
                              if (!ec) {
                                  LOG_DEBUG("HandShake", "Read %s from sock %d",
                                            std::string(in_buf_.data(), byte_transferred).c_str(),
                                            sock_.native_handle());
                                  if (std::string(in_buf_.data(), byte_transferred) != RESP_PONG) {
                                      /// FIXME: handle the received response
                                  } else {
                                      auto port = Server::GetInstance()->GetPort();
                                      LOG_DEBUG("HandShake", "Start send listening-port %d", port);
                                      sock_.async_write_some(
                                              asio::buffer(EncodeArr2RespArr({"replconf", "listening-port",
                                                                              std::to_string(port)})),
                                              [this, self](const std::error_code &e, const size_t byte_transferred) {
                                                  if (!e) {
                                                      LOG_DEBUG("HandShake",
                                                                "Finish sending replconf, listen port %d through socket %d",
                                                                sock_.local_endpoint().port(), sock_.native_handle());
                                                      PrepareAndSendReplConfCapa();
                                                  } else {
                                                      LOG_ERROR("HandShake", "error %s while send replconf",
                                                                e.message().c_str());
                                                      /// FIXME: handle the error
                                                  }
                                              });
                                  }
                              } else {
                                  LOG_ERROR("HandShake", "Error %s while receive PONG", ec.message().c_str());
                                  /// FIXME: handle the error
                              }
                          });
}

void Client::PrepareAndSendReplConfCapa() {
    auto self(shared_from_this());
    sock_.async_read_some(asio::buffer(in_buf_),
                          [self, this](const std::error_code &ec, const size_t byte_transferred) {
                              if (!ec) {
                                  LOG_DEBUG("HandShake", "Read %s from sock %d",
                                            std::string(in_buf_.data(), byte_transferred).c_str(),
                                            sock_.native_handle());
                                  if (std::string(in_buf_.data(), byte_transferred) != RESP_OK) {
                                      /// FIXME: handle the received response
                                  } else {
                                      sock_.async_write_some(
                                              asio::buffer(EncodeArr2RespArr({"replconf", "capa", "psync2"})),
                                              [self, this](const std::error_code &e, const size_t byte_transferred) {
                                                  if (!e) {
                                                      LOG_DEBUG("HandShake", "Finish sending replconf capa psync2");
                                                      PrepareAndSendPsync();
                                                  } else {
                                                      LOG_ERROR("HandShake", "error %s while send replconf",
                                                                e.message().c_str());
                                                      /// FIXME: handle the error
                                                  }
                                              });
                                  }
                              } else {
                                  LOG_ERROR("HandShake", "Error %s while receive response of replconf",
                                            ec.message().c_str());
                                  /// FIXME: handle the error
                              }
                          });

}

void Client::PrepareAndSendPsync() {
    auto self(shared_from_this());
    sock_.async_read_some(asio::buffer(in_buf_),
                          [self, this](const std::error_code &ec, const size_t byte_transferred) {
                              if (!ec) {
                                  LOG_DEBUG("HandShake", "Read %s from sock %d",
                                            std::string(in_buf_.data(), byte_transferred).c_str(),
                                            sock_.native_handle());
                                  if (std::string(in_buf_.data(), byte_transferred) != RESP_OK) {
                                      /// FIXME: handle the received response
                                  } else {
                                      sock_.async_write_some(
                                              /// FIXME: reuse this function with another parameters
                                              asio::buffer(EncodeArr2RespArr({"psync", "?", "-1"})),
                                              [self, this](const std::error_code &e, const size_t byte_transferred) {
                                                  if (!e) {
                                                      LOG_DEBUG("HandShake", "Finish sending command: psync ? -1");
                                                      ReceivePsyncReply();
                                                  } else {
                                                      LOG_ERROR("HandShake", "error %s while send command psync",
                                                                e.message().c_str());
                                                      /// FIXME: handle the error
                                                  }
                                              });
                                  }
                              } else {
                                  LOG_ERROR("HandShake", "Error %s while receive response of psync",
                                            ec.message().c_str());
                                  /// FIXME: handle the error
                              }
                          });
}

int Client::GetFullResync() {
    if (received_fullresync_)
        return 0;

    int pos = start_pos_;
    for (; pos < internal_buffer_.size(); ++pos) {
        if (internal_buffer_[pos] == '\n') {
            LOG_LINE();
            /// parse fullresync reply if found
            Server::GetInstance()->HandleFullResyncReply(
                    std::string(internal_buffer_.begin() + start_pos_, internal_buffer_.begin() + pos));
            received_fullresync_ = true;
            /// update start position of internal_buffer_
            start_pos_ = pos + 1;
            return 0;
        }
    }

    return -1;
}

int Client::GetRdbFileSize() {
    if (rdb_file_size_ > 0)
        return rdb_file_size_;

    if (start_pos_ >= internal_buffer_.size()) {
        LOG_LINE();
        return -1;
    }

    int pos = start_pos_;
    /// find the signature '$'
    while (pos < internal_buffer_.size() && internal_buffer_[pos] != '$') ++pos;
    /// move next character
    ++pos;
    start_pos_ = pos;

    LOG_LINE();
    for (; pos < internal_buffer_.size(); ++pos) {
        if (internal_buffer_[pos] == '\n') {
            LOG_LINE();
            LOG_LINE();
            for (int i = start_pos_; i < pos; ++i) {
                printf("%x", internal_buffer_[i]);
            }
            LOG_LINE();
            fflush(stdout);
            rdb_file_size_ = std::stoll(
                    std::string(internal_buffer_.begin() + start_pos_, internal_buffer_.begin() + pos));
            start_pos_ = pos + 1;
            LOG_DEBUG("Client", "start pos %zu, pos %d, rdb size %llu", start_pos_, pos, rdb_file_size_);
            return rdb_file_size_;
        }
    }
    LOG_LINE();

    return -1;
}

int Client::TryWriteRdb() {
    /// only called when know the rdb file size
    if (rdb_file_size_ <= 0)
        return -1;

    if (internal_buffer_.size() >= BULK_SIZE || rdb_read_size_ >= rdb_file_size_) {
        size_t need_to_write = rdb_file_size_ - rdb_written_size_;
        LOG_LINE();
        FlushBuffer2File(need_to_write);

        if (rdb_read_size_ >= rdb_file_size_) {

            CloseFile();
            LOG_INFO("Client", "Finish write RDB file");

            Database::GetInstance()->LoadPersistentDb();

            Server::GetInstance()->SetReplicaState(ReplicationState::ReplStateSynced);

            return 1;
        }
    }

    /// has not enough data to write
    return 0;
}

void Client::ReceivePsyncReply() {
    LOG_DEBUG("HandShake", "Prepare receive psync reply sock %d", sock_.native_handle());
    /// 1. read FULRESYNC
    auto self(shared_from_this());
    sock_.async_read_some(asio::buffer(in_buf_),
                          [self, this](const std::error_code &ec, const size_t byte_transferred) {
                              if (!ec) {
                                  LOG_DEBUG("HandShake", "Read %zu bytes from sock %d",
                                            byte_transferred,
                                            sock_.native_handle());

                                  /// append all new data to internal buffer
                                  for (int pos = 0; pos < byte_transferred; ++pos) {
                                      internal_buffer_.push_back(in_buf_[pos]);
                                  }

                                  /// find fullresync command
                                  if (!received_fullresync_) {
                                      if (GetFullResync() < 0) {
                                          /// haven't read fullresync yet, async read again and return
                                          ReceivePsyncReply();
                                          return;
                                      }
                                  }

                                  if (rdb_file_size_ <= 0) {
                                      /// NOT go to this bracket if know the file size
                                      if (GetRdbFileSize() < 0) {
                                          /// haven't read file size yet, async read agin and return
                                          ReceivePsyncReply();
                                          return;
                                      } else {
                                          rdb_read_size_ = internal_buffer_.size() - start_pos_;
                                      }
                                  } else {
                                      rdb_read_size_ += byte_transferred;
                                  }

                                  if (rdb_file_size_ > 0) {
                                      int ret = TryWriteRdb();
                                      if (ret == -1) {
                                          ReceivePsyncReply();
                                          return;
                                      } else if (ret == 0) {
                                          ReceivePsyncReply();
                                          return;
                                      } else {
                                          /// after that, if there are data in internal_buff_, trye to execute these command
                                          if (internal_buffer_.size() > start_pos_) {
                                              executor_.ReceiveDataAndExecute(
                                                      std::string(internal_buffer_.data() + start_pos_), self);
                                              /// reset internal buffer
                                              internal_buffer_.clear();
                                              start_pos_ = 0;
                                          }

                                          ReadAsync();
                                      }
                                  }
                              } else {
                                  LOG_ERROR("HandShake", "Error %s while receive response of psync",
                                            ec.message().c_str());
                                  /// FIXME: handle the error
                              }
                          });
}

void Client::PropagateRdb(const std::string &rdb_file_path) {
    char prefix_rdb[20] = {0};
    uint64_t rdb_size = 0, total_sent = 0;

    filename_ = rdb_file_path;
    LOG_DEBUG(TAG, "start sending replica prefix through fd %d", sock_.native_handle());
    struct stat st;
    if (RdbStat(rdb_file_path, st) < 0) {
        std::cerr << "Stat() file " << rdb_file_path << "error " << strerror(errno) << std::endl;
        return;
    } else {
        rdb_size = st.st_size;
    }

    snprintf(prefix_rdb, 19, "$%llu\r\n", rdb_size);

    LOG_DEBUG(TAG, "send prefix %s of file %s", prefix_rdb, filename_.c_str());
    /** send data */
    ///  First, send the length of rdb
    auto self(shared_from_this());
    sock_.async_write_some(asio::buffer(prefix_rdb, strlen(prefix_rdb)),
                           [self, this, rdb_file_path](const std::error_code &error, const size_t byte_transferred) {
                               if (error) {
                                   LOG_ERROR(TAG, "Send prefix rdb fail %d: %s", error.value(),
                                             error.message().c_str());
                               } else {
                                   /// Second, read and send binary rdb
                                   /// because MACOSX does not support ASIO_HAS_IO_URING, so we use normal blocking file read/write
                                   WriteStreamFileAsync();
                               }
                           });
}

void Client::OpenFile() {
#if defined(_WIN32)
    HANDLE h = ::CreateFileA(filename_.c_str(),
                             GENERIC_WRITE,
                             FILE_SHARE_READ,
                             NULL,
                             CREATE_ALWAYS,
                             FILE_ATTRIBUTE_NORMAL,
                             NULL);
    file_ = asio::windows::random_access_handle(io_, h);
#else
//    int fd = ::open(filename_.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0666);
//    file_ = asio::posix::stream_descriptor(io_context_, fd);
#endif

    fd_ = ::open(filename_.c_str(), O_CREAT | O_RDWR, 0666);
    file_opened_ = true;

    size_t file_size = 0;
#if ZDEBUG
    struct stat st;
    if (RdbStat(filename_, st) < 0) {
        std::cerr << "Stat() file " << filename_ << "error " << strerror(errno) << std::endl;
        return;
    } else {
        file_size = st.st_size;
    }
#endif // ZDEBUG

    LOG_DEBUG("Client", "file opened %s, size %lu", filename_.c_str(), file_size);
}

void Client::FlushBuffer2File(size_t bytes) {
    if (!file_opened_) {
        OpenFile();
        file_opened_ = 1;
    }

    ssize_t written = ::write(fd_, internal_buffer_.data() + start_pos_, bytes);
    LOG_DEBUG("Client", "need to write %zu, write %ld bytes from %zu in internal buffer size %zu to file %s",
              bytes, written, start_pos_, internal_buffer_.size(), filename_.c_str());

    start_pos_ += written;
    rdb_written_size_ += written;

    if (start_pos_ >= internal_buffer_.size()) { /// write full
        internal_buffer_.clear();
        start_pos_ = 0;
    } else {
        LOG_ERROR("Client", "File %s write error, written %ld", filename_.c_str(), written);
    }
}

void Client::CloseFile() {
    if (!file_opened_)
        return;

    ::close(fd_);
    file_opened_ = 0;
}

void Client::ReadFile2Buffer() {
    if (!file_opened_)
        return;

    ssize_t read_bytes = ::read(fd_, internal_buffer_.data(), internal_buffer_.size());
}

