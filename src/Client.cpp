//
// Created by Manh Nguyen Viet on 8/18/25.
//

#include "Client.h"
#include "Server.h"

void Client::ReadAsync() {
    LOG_LINE();
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
                                  LOG_ERROR(TAG, "error receive data %s on sock %d", error.message().c_str(), sock_.native_handle());
                                  /// FIXME: handle other error
                              }
                          });
}

void Client::WriteAsync(const std::string &reply) {
    LOG_INFO("Client", "write reply %s, sock %d, client type %d", reply.c_str(), sock_.native_handle(), client_type_);
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
    LOG_LINE();
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
            } else {
                Server::GetInstance()->OnReady();
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

//int Client::WriteSync(const std::string &s) {
//    try {
//        LOG_DEBUG("Client", "write to socket: %s", s.c_str());
//        return sock_.write_some(asio::buffer(s));
//    }
//    catch (std::exception &ex) {
//        LOG_ERROR("Asio", "write sync msg %s fail", s.c_str());
//        return SyncWriteError;
//    }
//}

//int Client::ReadSyncWithLength(std::string &s, const size_t length) {
//    try {
////        asio::read(sock_, asio::buffer(in_buf_, length));
//        sock_.read_some(asio::buffer(in_buf_, length));
//        s = std::move(std::string(in_buf_.begin(), in_buf_.begin() + length));
//        LOG_DEBUG("Client", "read from socket: %s", s.c_str());
//        return RedisSuccess;
//    }
//    catch (std::exception &ex) {
//        LOG_ERROR("Asio", "Read with max length %lu fail %s", length, ex.what());
//        return SyncReadError;
//    }
//}

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
    sock_.async_read_some(asio::buffer(in_buf_), [this, self](const std::error_code &ec, const size_t byte_transferred) {
        if (!ec) {
            LOG_DEBUG("HandShake", "Read %s from sock %d", std::string(in_buf_.data(), byte_transferred).c_str(),
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
                                LOG_DEBUG("HandShake", "Finish sending replconf, listen port %d through socket %d",
                                          sock_.local_endpoint().port(), sock_.native_handle());
                                PrepareAndSendReplConfCapa();
                            } else {
                                LOG_ERROR("HandShake", "error %s while send replconf", e.message().c_str());
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
    sock_.async_read_some(asio::buffer(in_buf_), [self, this](const std::error_code &ec, const size_t byte_transferred) {
        if (!ec) {
            LOG_DEBUG("HandShake", "Read %s from sock %d", std::string(in_buf_.data(), byte_transferred).c_str(),
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
                                LOG_ERROR("HandShake", "error %s while send replconf", e.message().c_str());
                                /// FIXME: handle the error
                            }
                        });
            }
        } else {
            LOG_ERROR("HandShake", "Error %s while receive response of replconf", ec.message().c_str());
            /// FIXME: handle the error
        }
    });

}

void Client::PrepareAndSendPsync() {
    auto self(shared_from_this());
    sock_.async_read_some(asio::buffer(in_buf_), [self, this](const std::error_code &ec, const size_t byte_transferred) {
        if (!ec) {
            LOG_DEBUG("HandShake", "Read %s from sock %d", std::string(in_buf_.data(), byte_transferred).c_str(),
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
                                LOG_ERROR("HandShake", "error %s while send command psync", e.message().c_str());
                                /// FIXME: handle the error
                            }
                        });
            }
        } else {
            LOG_ERROR("HandShake", "Error %s while receive response of psync", ec.message().c_str());
            /// FIXME: handle the error
        }
    });
}

void Client::ReceivePsyncReply() {
    /// 1. read FULRESYNC
    auto self(shared_from_this());
    sock_.async_read_some(asio::buffer(in_buf_), [self, this](const std::error_code &ec, const size_t byte_transferred) {
        if (!ec) {
            LOG_DEBUG("HandShake", "Read %s from sock %d", std::string(in_buf_.data(), byte_transferred).c_str(),
                      sock_.native_handle());
            /// parse received string
            /// find the new line '\n' to get FULLSYNC response
            std::string fullresync_reply;
            size_t pos = 0;
            for (; pos < byte_transferred; pos++) {
                if (in_buf_[pos] == '\n') {
                    for (int i = 0; i < pos; ++i) {
                        internal_buffer_.push_back(in_buf_[i]);
                    }

                    fullresync_reply = std::string(internal_buffer_.begin(), internal_buffer_.end());

                    /// store the remainder of received data
                    internal_buffer_.clear();
                    for (int i = 0; i < byte_transferred - pos - 1; ++i) {
                        internal_buffer_.emplace_back(in_buf_.at(pos + 1 + i));
                    }

                    break;
                }
            }

            if (fullresync_reply.empty()) { /// not found '\n' in the above loop
                /// append received data
                for (size_t i = 0; i < byte_transferred; ++i) {
                    internal_buffer_.push_back(in_buf_.at(i));
                }

                /// continue read to enough FULLRESYNC reply
                ReceivePsyncReply();
            } else {
                /// Handle FULLRESYNC reply, update info
                Server::GetInstance()->HandleFullResyncReply(fullresync_reply);

                /**Create a temporary file in disk */
                std::string tmp_file_path = Database::GetInstance()->GetRdbPath();
                FILE *ptmp_file = fopen(tmp_file_path.c_str(), "wb");
                if (!ptmp_file) {
                    LOG_ERROR(TAG, "open temp file %s fail %s", tmp_file_path.c_str(), strerror(errno));
                    return OpenRdbFileError;
                }

                /// Reading file .rdb with async method
                ReadRdbFileReply(ptmp_file);
            }
        } else {
            LOG_ERROR("HandShake", "Error %s while receive response of psync", ec.message().c_str());
            /// FIXME: handle the error
        }
    });
}

int Client::ReadRdbLengthAndData(FILE *file) {
    /// unknown file size. Try to get file size in the internal buffer
    if (rdb_file_size_ <= 0) {
        LOG_LINE();
        for (int i = 0; i < internal_buffer_.size(); ++i) {
            if (internal_buffer_[i] == '\n' && internal_buffer_[0] == '$') {
                rdb_file_size_ = std::stoll(
                        std::string(internal_buffer_.begin() + 1, internal_buffer_.begin() + i - 1));
                /// store remainder in temp buffer
                std::vector<char> tmp_buffer;
                for (int j = i + 1; j < internal_buffer_.size(); ++j) {
                    tmp_buffer.push_back(internal_buffer_[j]);
                }

                /// write to file if enough data
                if (tmp_buffer.size() >= rdb_file_size_) {
                    fwrite(tmp_buffer.data(), sizeof(char), tmp_buffer.size(), file);
                    fclose(file);
                    LOG_INFO("RDBSync", "Finish sync data from the master");
                    internal_buffer_.clear();

                    Database::GetInstance()->LoadPersistentDb();

                    Server::GetInstance()->SetReplicaState(ReplicationState::ReplStateSynced);

                    Server::GetInstance()->OnReady();

                    return 2;
                }

                /// restore to internal buffer
                internal_buffer_ = tmp_buffer;
                /// internal buffer contains only binary data. update rdb_read_size
                rdb_read_size_ = internal_buffer_.size();
                return 1;
            }
        }
    }

    return 0;
}

void Client::ReadRdbFileReply(FILE *file) {
    /// Format of this response: $<length_of_file>\r\n<binary_contents_of_file>
    /// try to get rdb file size from internal buffer. If it was known, redis skip this line
    if (ReadRdbLengthAndData(file) == 2) {
        LOG_LINE();
        ReadAsync();
        LOG_LINE();
        return;
    }

    /// read binary data from the master.
    /// Coming data was stored in in_buf_ -> copy to internal_buffer_.
    /// The internal_buffer_ must has enough size before writing to disk.
    auto self(shared_from_this());
    sock_.async_read_some(asio::buffer(in_buf_),
                          [this, self, file](const std::error_code &ec, const size_t received_bytes) {
                              if (!ec) {
                                  LOG_DEBUG("ReplicaSync", "Receive %zu bytes", received_bytes);
                                  /// append all data in buffer (include previous data)
                                  for (size_t i = 0; i < received_bytes; ++i) {
                                      internal_buffer_.push_back(in_buf_[i]);
                                  }

                                  /// try to determine the rdb file size with new data
                                  int ret = ReadRdbLengthAndData(file);
                                  /**
                                   * HACK:
                                   * ret == 2: read and write all file -> return
                                   * ret == 1: first time get the file size. the rdb_read_size was assign before, no need to plus
                                   * ret == 0: dont know the file size or knew it before. plus the received_bytes
                                   * */
                                  if (ret == 2) {
                                      ReadAsync();
                                      return;
                                  }
                                  else if (rdb_file_size_ <= 0) {
                                      /// still dont know rdb file size
                                      /// continue read to get the file size
                                      sock_.async_read_some(asio::buffer(in_buf_),
                                                            [self, this, file](const std::error_code &e,
                                                                               const size_t bytes) {
                                                                if (!e) {
                                                                    ReadRdbFileReply(file);
                                                                }
                                                            });
                                      return;
                                  }

                                  if (ret == 0) {
                                      rdb_read_size_ += received_bytes;
                                  }

                                  if (rdb_read_size_ >= rdb_file_size_ || internal_buffer_.size() >= BULK_SIZE) {
                                      /// known file before. after this read, receive enough file
                                      /// write all data to file and close
                                      fwrite(internal_buffer_.data(), sizeof(char), internal_buffer_.size(), file);
                                      internal_buffer_.clear();
                                      if (rdb_read_size_ >= rdb_file_size_) {
                                          LOG_INFO("RDBSync", "Finish sync data from the master");
                                          fclose(file);

                                          Database::GetInstance()->LoadPersistentDb();

                                          Server::GetInstance()->SetReplicaState(ReplicationState::ReplStateSynced);

                                          Server::GetInstance()->OnReady();

                                          ReadAsync();
                                      }
                                  }

                                  /// only call it when expect more data
                                  if (rdb_read_size_ < rdb_file_size_) {
                                      /// continue reading
                                      ReadRdbFileReply(file);
                                  }
                              } else {
                                  /// FIXME: handle error???
                                  /// mark the server state: repl fail
                                  LOG_ERROR("ReplicaSync", "Error %s", ec.message().c_str());
                              }
                          });
}

void Client::PropagateRdb(const std::string &rdb_file_path) {
    char prefix_rdb[20] = {0};
    uint64_t rdb_size = 0, total_sent = 0;

    LOG_DEBUG(TAG, "start sending replica prefix through fd %d", sock_.native_handle());
    struct stat st;
    if (RdbStat(rdb_file_path, st) < 0) {
        std::cerr << "Stat() file " << rdb_file_path << "error " << strerror(errno) << std::endl;
        return;
    } else {
        rdb_size = st.st_size;
    }

    snprintf(prefix_rdb, 19, "$%llu\r\n", rdb_size);

    /** send data */
    ///  First, send the length of rdb
    auto self(shared_from_this());
    sock_.async_write_some(asio::buffer(prefix_rdb),
                           [self, this, rdb_file_path](const std::error_code &error, const size_t byte_transferred) {
                               if (error) {
                                   LOG_ERROR(TAG, "Send prefix rdb fail %d: %s", error.value(),
                                             error.message().c_str());
                               } else {
                                   /// Second, read and send binary rdb
                                   /// because MACOSX does not support ASIO_HAS_IO_URING, so we use normal blocking file read/write
                                   std::shared_ptr<std::ifstream> pfile = std::make_shared<std::ifstream>(
                                           rdb_file_path);
                                   WriteStreamFileAsync(pfile);
                               }
                           });
}