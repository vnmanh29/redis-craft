//
// Created by Manh Nguyen Viet on 7/22/25.
//

#ifndef REDIS_STARTER_CPP_REDISERROR_H
#define REDIS_STARTER_CPP_REDISERROR_H

enum Error {
    CreateSocketError = -1,
    GetHostNameError = -2,
    SocketConnectError = -3,
    HandShakeSendError = -4,
    HandShakeRecvError = -5,
    HandShakeFdError = -6,
};

#endif //REDIS_STARTER_CPP_REDISERROR_H
