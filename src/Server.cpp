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

#include "all.hpp"
#include "CommandExecutor.h"
#include "RedisOption.h"

#define CRLF "\r\n"

static std::string get_response2(const std::string &query)
{
    CommandExecutor ce;
    ce.ReceiveRequest(query);

    return ce.Execute();
}

static void receive_and_send(int fd)
{
    std::cout << "Client connected\n";

    fd_set readfds;
    FD_ZERO(&readfds);
    FD_SET(fd, &readfds);
    struct timeval timeout;
    timeout.tv_sec = 2;
    timeout.tv_usec = 0;

    char buffer[4096] = {0};
    while (true)
    {
        select(fd + 1, &readfds, NULL, NULL, &timeout);
        ssize_t recv_bytes = recv(fd, (void*)buffer, 4095, 0);
        if (recv_bytes < 0)
        {
            std::cerr << "Receive from client failed: " << errno << ", msg: " << strerror(errno) << std::endl;
            break;
        }
        else if (recv_bytes == 0)
        {
            continue;
        }
        
        buffer[recv_bytes] = '\0';
        std::string received_data(buffer);

        std::string response = get_response2(received_data);
        ssize_t sent_bytes = send(fd, response.c_str(), response.size(), 0);
    }

    close(fd);
}

static int redis_parse_options(int argc, char** argv, std::shared_ptr<RedisConfig> cfg)
{
    int idx = 1;
    while (idx < argc)
    {
        char* arg = argv[idx];
        if (arg[0] == '-' && arg[1] == '-' && (&arg[2]))
        {
            /// get the name of option, find this option and set the value
            std::string name = static_cast<std::string>(&arg[2]);
            if (idx + 1 >= argc)
            {
                printf("%s, %d\n", __func__, __LINE__);
                return -1;
            }
            
            const char* val = argv[++idx];
            const RedisOptionDef* opt = find_redis_option(redis_options, name.c_str());
            if (opt)
            {
                opt->func_arg(cfg.get(), val);
            }
            else
            {
                fprintf(stderr, "%s, %d, idx %d, name %s\n", __func__, __LINE__, idx, name.c_str());
                return -1;
            }
        }
        else
        {
            
            /// TODO: handle another format
            fprintf(stderr, "%s, %d, idx %d\n", __func__, __LINE__, idx);
            return -2;
        }

        ++idx;
    }

    return 0;
}

static void redis_set_global_config(const std::shared_ptr<RedisConfig>& cfg)
{
    Database::GetInstance()->SetConfig(cfg);
}

int main(int argc, char **argv) {
    // Flush after every std::cout / std::cerr
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    /// parse the argv

    std::shared_ptr<RedisConfig> cfg = std::make_shared<RedisConfig>();
    int ret = redis_parse_options(argc, argv, cfg);
    if (ret < 0)
    {
        fprintf(stderr, "Invalid parameters, total %d ret %d\n", argc, ret);
        for (int i = 0;i < argc; ++i)
        {
            fprintf(stderr, "arg %d: %s\n", i, argv[i]);
        }
        return -1;
    }

    redis_set_global_config(cfg);

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "Failed to create server socket\n";
        return 1;
    }

    // Since the tester restarts your program quite often, setting SO_REUSEADDR
    // ensures that we don't run into 'Address already in use' errors
    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        std::cerr << "setsockopt failed\n";
        return 1;
    }

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(6379);

    if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) != 0) {
        std::cerr << "Failed to bind to port 6379\n";
        return 1;
    }

    int connection_backlog = 5;
    if (listen(server_fd, connection_backlog) != 0) {
        std::cerr << "listen failed\n";
        return 1;
    }

    struct sockaddr_in client_addr;
    int client_addr_len = sizeof(client_addr);
    std::cout << "Waiting for a client to connect...\n";

    std::vector<int> list_client_fd;

    while (true)
    {
        int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, (socklen_t *)&client_addr_len);
        if (client_fd < 0)
        {
            std::cerr << "Accept failed\n";
            return -1;
        }

        std::thread t(receive_and_send, client_fd);
        
        list_client_fd.push_back(client_fd);
        t.detach();        
    }
    
    for (int& fd : list_client_fd)
    {
        close(fd);
    }

    close(server_fd);

    return 0;
}

/**
 * TODO: 
 * - Implement the RESP parser
 * 
 */
