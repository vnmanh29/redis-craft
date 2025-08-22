//
// Created by Manh Nguyen Viet on 7/22/25.
//

#ifndef REDIS_STARTER_CPP_REDISERROR_H
#define REDIS_STARTER_CPP_REDISERROR_H

enum Error {
    RedisSuccess = 0,

    /// critical errors
    CreateSocketError = -1,
    GetHostNameError = -2,
    SocketConnectError = -3,
    HandShakeSendError = -4,
    HandShakeRecvError = -5,
    HandShakeFdError = -6,
    InvalidCommandError = -7,
    InvalidResponseError = -8,
    InvalidSocketError = -9,
    SentDataError = -10,
    BuildExecutorError = -11,
    OpenRdbFileError = -12,
    ReceiveRdbFileError = -13,
    SyncWriteError = -14,
    SyncReadError = -15,


    /// retriable errors
    IncompletedCommand = -30,

    UnknownError = -101,
};

#endif //REDIS_STARTER_CPP_REDISERROR_H
