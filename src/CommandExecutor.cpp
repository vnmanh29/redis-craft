//
// Created by Manh Nguyen Viet on 7/10/25.
//

#include "CommandExecutor.h"
#include "RedisError.h"
#include "Server.h"

int CommandExecutor::ReceiveDataAndExecute(const std::string &buffer, std::shared_ptr<Client> client) {
    /// append new data to the last. Useful in case that remain data in the previous request
    data_ = data_ + buffer;
    LOG_DEBUG(TAG, "Received buffer %s, new data %s", buffer.c_str(), data_.c_str());

    size_t offset = 0;
    while (offset < data_.size()) {
        /// FIXME: avoid to call strdup() this char array
        const char *remain_buf = strdup(data_.substr(offset).c_str());
        size_t remain_len = data_.size() - offset;
        resp::result res = decoder_.decode(remain_buf, remain_len);
        if (res == resp::incompleted) {
            LOG_DEBUG(TAG, "Incompleted RESP with remainder buf %s", remain_buf);
            data_ = data_.substr(offset);
            return IncompletedCommand;
        } else if (res == resp::error) {
            LOG_ERROR(TAG, "Invalid RESP with remainder buf %s", remain_buf);
            return Error::InvalidCommandError;
        }

        delete remain_buf;

        /// build the command
        ResetQuery(query_);

        resp::unique_value rep = res.value();
        /// create query and executor of this command
        int ret = BuildRedisCommand(rep);
        if (ret < 0) {
            LOG_ERROR(TAG, "NOT found the suitable cmd, err %d", ret);
#if HARDCODE
            query_.cmd = new RedisCmd(UnknownCmd, READ_CMD | APP_RECV);
#else // HARDCODE
            return ret;
#endif // HARDCODE
        } else {
            LOG_INFO(TAG, "build cmd %d success flag %llu", query_.cmd->cmd_type, query_.cmd->flags);
        }

        if (client->ClientType() == ClientType::TypeMaster) {
            /// this command was propagated from its master, so it is replicated command
            query_.flags |= REPL_CMD;
        }

        ret = BuildExecutor();
        if (ret < 0) {
            LOG_ERROR(TAG, "Build executor fail, error %d", ret);
            data_ = data_.substr(offset);
            return ret;
        }

        /// execute the current command, fill the response to the output buffer of client
        internal_executor_->execute(query_, client);
        /// increase the offset in the next decoding
        offset += res.size();
        LOG_DEBUG("Executor", "remain data %s", data_.c_str());

        /// propagate this command to the slaves if need to propagate this command
        int need_propagate = ((query_.flags & WRITE_CMD) | (query_.flags & REPL_CMD)) ? 1 : 0;

        /// first, encode the command to a list of RESP string
        std::vector<std::string> resp_arr = encoder_.encode_arr(query_.cmd_args);

        /// second, concat all argv to get the entire resp_data
        std::string resp_data;
        for (auto &argv: resp_arr) {
            resp_data += argv;
        }

        /// update offset
        if (need_propagate || client->ClientType() == TypeMaster)
            Server::GetInstance()->AddBackLogBuffer(resp_data);

        /// move next loop if this cmd no need to propagate
        if (!need_propagate)
            continue;

        /// FIXME: handle case replica server received the command from its master
        if (client->ClientType() == TypeMaster)
            continue;

        /// third, loop and fill entire resp_data to output buffer of slaves
        auto clients = Server::GetInstance()->GetClients();
        LOG_DEBUG(TAG, "There are %lu clients of this server", clients.size());
        for (const auto &cli: clients) {
            if (cli->ClientType() == ClientType::TypeSlave) {
                /// FIXME: handle case copy the resp_data to output buffer fail
                LOG_DEBUG(TAG, "Propagate command %s through sock %d", resp_data.c_str(),
                          cli->Socket().native_handle());
                cli->WriteAsync(resp_data, MASTER_SEND | SLAVE_RECV);
            }
        }
    }

    data_ = data_.substr(offset);

    return 0;
}

int CommandExecutor::BuildRedisCommand(const resp::unique_value &rep) {
    /// clean all argv of previous cmd
    ResetQuery(query_);

    resp::unique_array<resp::unique_value> arr = rep.array();
    if (arr.size() < 1) {
        LOG_ERROR(TAG, "less than 1 element in arr");
        return InvalidCommandError;
    }

    /// fill the cmd argv
    for (int i = 0; i < arr.size(); ++i) {
        query_.cmd_args.emplace_back(arr[i].bulkstr().data(), arr[i].bulkstr().size());
    }

    /// mapping with the declared cmds, find the cmd type
    std::string &cmd_name = query_.cmd_args[0];
    std::transform(cmd_name.begin(), cmd_name.end(), cmd_name.begin(), ::tolower);
    auto rcmd = Server::GetInstance()->GetRedisCommand(cmd_name);
    if (rcmd == nullptr) {
        LOG_ERROR(TAG, "Command %s is not supported nowadays", cmd_name.c_str());
        return InvalidCommandError;
    }

    /// update the cmd type
    int has_subcmd = (rcmd->subcmd_dict.empty()) ? 0 : 1;
    if (has_subcmd) {
        if (query_.cmd_args.size() <= 1) {
            return InvalidCommandError;
        } else {
            std::string &subcmd = query_.cmd_args[1];
            std::transform(subcmd.begin(), subcmd.end(), subcmd.begin(), ::tolower);
            auto it = rcmd->subcmd_dict.find(subcmd);
            if (it == rcmd->subcmd_dict.end()) {
                return InvalidCommandError;
            } else {
                query_.cmd = it->second;
                query_.flags = it->second->flags;
            }
        }
    } else {
        query_.cmd = rcmd;
        query_.flags = rcmd->flags;
    }

    return 0;
}

int CommandExecutor::BuildExecutor() {
    if (!query_.cmd)
        return BuildExecutorError;

    /// create the executor by cmd_type
    internal_executor_ = AbstractInternalCommandExecutor::createCommandExecutor(query_.cmd->cmd_type);
    if (!internal_executor_)
        return BuildExecutorError;

    return 0;
}