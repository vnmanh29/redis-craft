//
// Created by Manh Nguyen Viet on 8/26/25.
//

#include "CircularBuffer.h"

void AppendDataBuffer(CircularBuffer *buffer, const std::string &s) {
    buffer->data += s;
}
