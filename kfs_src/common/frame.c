//=====================================================================
//
// frame.c — KCP 帧同步库的帧同步层（纯 C，不依赖 Lua / KCP / 网络）
//
// 实现帧信封的编解码：
//   [ frame_id(4B) ][ data_len(2B) ][ data... ]
//
// 所有函数只操作内存缓冲，无 IO、无线程、无外部依赖，可独立单元测试。
//
//=====================================================================
#include "frame.h"

#include <string.h>


// 协议包


// 编码：把帧号+负载打包进 out。
// 失败条件：out 为空、out_cap 不足以容纳帧头、len 超上限。
int frame_encode(char *out, int out_cap, uint32_t fid,
                 const char *data, int len)
{
    uint16_t dlen;

    if (out == NULL || out_cap < KFS_FRAME_HEADER_SIZE)
        return -1;
    if (len < 0 || len > KFS_FRAME_MAX_DATA)
        return -1;
    if (out_cap < KFS_FRAME_HEADER_SIZE + len)
        return -1;

    dlen = (uint16_t)len;
    memcpy(out, &fid, 4);
    memcpy(out + 4, &dlen, 2);
    if (len > 0 && data != NULL)
        memcpy(out + KFS_FRAME_HEADER_SIZE, data, (size_t)len);
    return KFS_FRAME_HEADER_SIZE + len;
}

// 解码：从整帧数据中取出帧号与负载。
// 失败条件：pkt 为空、pktlen 小于帧头、负载长度字段与 pktlen 不匹配。
int frame_decode(const char *pkt, int pktlen,
                 uint32_t *fid, const char **data, int *dlen)
{
    uint16_t dlen_raw;

    if (pkt == NULL || pktlen < KFS_FRAME_HEADER_SIZE)
        return -1;
    if (fid == NULL || data == NULL || dlen == NULL)
        return -1;

    memcpy(fid, pkt, 4);
    memcpy(&dlen_raw, pkt + 4, 2);

    // 长度字段必须与整包长度严格一致（防截断/粘包）
    if ((int)dlen_raw != pktlen - KFS_FRAME_HEADER_SIZE)
        return -1;
    if (dlen_raw > KFS_FRAME_MAX_DATA)
        return -1;

    *dlen = (int)dlen_raw;
    *data = (dlen_raw > 0) ? (pkt + KFS_FRAME_HEADER_SIZE) : pkt;
    return 0;
}

// 带消息类型的帧打包：[ type 1B ][ frame_id 4B ][ data_len 2B ][ data... ]
int frame_encode2(char *out, int out_cap, uint8_t type, uint32_t fid,
                  const char *data, int len)
{
    uint16_t dlen;

    if (out == NULL || out_cap < KFS_FRAME2_HEADER_SIZE)
        return -1;
    if (len < 0 || len > KFS_FRAME_MAX_DATA)
        return -1;
    if (out_cap < KFS_FRAME2_HEADER_SIZE + len)
        return -1;

    dlen = (uint16_t)len;
    out[0] = type;
    memcpy(out + 1, &fid, 4);
    memcpy(out + 5, &dlen, 2);
    if (len > 0 && data != NULL)
        memcpy(out + KFS_FRAME2_HEADER_SIZE, data, (size_t)len);
    return KFS_FRAME2_HEADER_SIZE + len;
}

// 带消息类型的帧解包
int frame_decode2(const char *pkt, int pktlen,
                  uint8_t *type, uint32_t *fid,
                  const char **data, int *dlen)
{
    uint16_t dlen_raw;

    if (pkt == NULL || pktlen < KFS_FRAME2_HEADER_SIZE)
        return -1;
    if (type == NULL || fid == NULL || data == NULL || dlen == NULL)
        return -1;

    *type = (uint8_t)pkt[0];
    memcpy(fid, pkt + 1, 4);
    memcpy(&dlen_raw, pkt + 5, 2);

    // 长度字段必须与整包长度严格一致（防截断/粘包）
    if ((int)dlen_raw != pktlen - KFS_FRAME2_HEADER_SIZE)
        return -1;
    if (dlen_raw > KFS_FRAME_MAX_DATA)
        return -1;

    *dlen = (int)dlen_raw;
    *data = (dlen_raw > 0) ? (pkt + KFS_FRAME2_HEADER_SIZE) : pkt;
    return 0;
}

// FNV-1a 32 位哈希：用于帧对账 / 完整性校验。
//   服务器对广播帧负载（不含末尾 4B 哈希本身）计算，客户端重算比对。
uint32_t frame_fnv1a(const void *data, int len)
{
    const unsigned char *p = (const unsigned char *)data;
    uint32_t h = 2166136261u;                 // FNV-1a 32 offset basis
    int i;
    for (i = 0; i < len; i++) {
        h ^= p[i];
        h *= 16777619u;                       // FNV 32 prime
    }
    return h;
}
