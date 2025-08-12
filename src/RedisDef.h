//
// Created by Manh Nguyen Viet on 8/8/25.
//

#ifndef REDIS_STARTER_CPP_REDISDEF_H
#define REDIS_STARTER_CPP_REDISDEF_H

enum struct LogLevel {
    Silent = 0,
    Error = 1,
    Info = 2,
    Debug = 3,
};

extern LogLevel global_log_level;
extern const char TAG[];

#define LOG_ERROR(LABEL, fmt, ...) {                        \
    if (global_log_level >= LogLevel::Error) {              \
        printf("[%s]: " fmt "\n", LABEL, ##__VA_ARGS__);    \
        fflush(stdout);                                     \
    }                                                       \
}

#define LOG_INFO(LABEL, fmt, ...) {                     \
    if (global_log_level >= LogLevel::Info) {           \
        printf("[%s] " fmt "\n", LABEL, ##__VA_ARGS__); \
        fflush(stdout);                                 \
    }                                                   \
}

#define LOG_DEBUG(LABEL, fmt, ...) {                                                                \
    if (global_log_level >= LogLevel::Debug) {                                                      \
        printf("[%s]:%s:%d:%s " fmt "\n", LABEL, __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__);  \
        fflush(stdout);                                                                             \
    }                                                                                               \
}

#define LOG_LINE() { \
    printf("%s:%d:%s\n", __FILE__, __LINE__, __FUNCTION__); \
    fflush(stdout);      \
}\

#endif //REDIS_STARTER_CPP_REDISDEF_H
