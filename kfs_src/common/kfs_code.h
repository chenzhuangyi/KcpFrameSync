//=====================================================================
//
// kfs_code.h — KCP 帧同步库的结果码
//
// 职责：用于返回给前端。
//
//
//=====================================================================


#ifndef KFS_CODE_H
#define KFS_CODE_H


#ifdef __cplusplus
extern "C" {
#endif

//---------------------------------------------------------------------
// 结果码
//---------------------------------------------------------------------
typedef enum {
    KFS_CODE_SUCCESS = 0,
    KFS_CODE_ERROR_ROOM_NULL = 1,       // 房间null
    KFS_CODE_ERROR_NOT_IN_ROOM = 2,     // 不在房间中
    KFS_CODE_ERROR_JOIN_TOKEN = 3,      // token鉴权失败
    KFS_CODE_ERROR_ALREADY_ONLINE = 4,  // 玩家已连接
    KFS_CODE_ERROR_CREATE_KCPCB = 5,  // create a new kcpcb
} kfs_code;


#ifdef __cplusplus
}
#endif

#endif /* KFS_CODE_H */

