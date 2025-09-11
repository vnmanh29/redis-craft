//
// Created by Manh Nguyen Viet on 8/8/25.
//

#ifndef REDIS_STARTER_CPP_REDISDEF_H
#define REDIS_STARTER_CPP_REDISDEF_H

#define ASIO_LIB 1
#define HARDCODE 1

#define ZDEBUG 0

#define EXECUTOR "Executor"

enum struct LogLevel {
    Silent = 0,
    Error = 1,
    Info = 2,
    Debug = 3,
};

#define BUFFER_SIZE 4096
#define BULK_SIZE 1<<20

#define RESP_PONG "+PONG\r\n"
#define RESP_OK "+OK\r\n"
#define RESP_NIL "$-1\r\n"

#define RESP_NONE "+none\r\n"
#define RESP_STRING "+string\r\n"
#define RESP_STREAM "+stream\r\n"

#define RESP_FULLRESYNC "+FULLRESYNC"

extern LogLevel global_log_level;
extern const char TAG[];

#define KNRM  "\x1B[0m"
#define KRED  "\x1B[31m"
#define KGRN  "\x1B[32m"
#define KYEL  "\x1B[33m"
#define KBLU  "\x1B[34m"
#define KMAG  "\x1B[35m"
#define KCYN  "\x1B[36m"
#define KWHT  "\x1B[37m"

#define LOG_ERROR(LABEL, fmt, ...) {                        \
    if (global_log_level >= LogLevel::Error) {              \
        printf("\033[0;31m[%s]: " fmt "\033[0m\n", LABEL, ##__VA_ARGS__);    \
        fflush(stdout);                                     \
    }                                                       \
}

#define LOG_INFO(LABEL, fmt, ...) {                     \
    if (global_log_level >= LogLevel::Info) {           \
        printf("\033[32m[%s]:%d " fmt "\033[0m\n", LABEL, __LINE__, ##__VA_ARGS__); \
        fflush(stdout);                                 \
    }                                                   \
}

#define LOG_DEBUG(LABEL, fmt, ...) {                                                                \
    if (global_log_level >= LogLevel::Debug) {                                                      \
        printf("\033[93m[%s]:%d:%s " fmt "\033[0m\n", LABEL, __LINE__, __FUNCTION__, ##__VA_ARGS__);  \
        fflush(stdout);                                                                             \
    }                                                                                               \
}

#if ZDEBUG
#define LOG_LINE() { \
    printf("%s:%d:%s\n", __FILE__, __LINE__, __FUNCTION__); \
    fflush(stdout);      \
}
#else
#define LOG_LINE()
#endif // DEBUG

#endif //REDIS_STARTER_CPP_REDISDEF_H
