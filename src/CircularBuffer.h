//
// Created by Manh Nguyen Viet on 8/26/25.
//

#ifndef REDIS_CRAFT_CIRCULARBUFFER_H
#define REDIS_CRAFT_CIRCULARBUFFER_H

#include <string>

typedef struct CircularBuffer {
    size_t capacity;
    size_t size;
    /// FIXME: implement real circular buffer
    std::string data;
} CircularBuffer;

void AppendDataBuffer(CircularBuffer *buffer, const std::string &s);

#endif //REDIS_CRAFT_CIRCULARBUFFER_H
