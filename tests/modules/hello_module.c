/* Minimal test module that registers a HELLO command */
#include <stdlib.h>
#include <string.h>

/* We declare only the function pointers we need, matching the Redis Module ABI */
typedef struct RedisModuleCtx RedisModuleCtx;
typedef struct RedisModuleString RedisModuleString;

typedef int (*CmdFunc)(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

/* ABI function declarations - these are resolved from the host binary */
extern int RedisModule_Init(RedisModuleCtx *ctx, const char *name, int ver, int apiver);
extern int RedisModule_CreateCommand(RedisModuleCtx *ctx, const char *name, CmdFunc cmdfunc,
                                     const char *strflags, int firstkey, int lastkey, int keystep);
extern int RedisModule_ReplyWithBulkString(RedisModuleCtx *ctx, const char *buf, size_t len);
extern int RedisModule_ReplyWithLongLong(RedisModuleCtx *ctx, long long ll);
extern void RedisModule_AutoMemory(RedisModuleCtx *ctx);

#define REDISMODULE_OK 0
#define REDISMODULE_ERR 1
#define REDISMODULE_APIVER_1 1

int HelloCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    const char *msg = "Hello from module!";
    return RedisModule_ReplyWithBulkString(ctx, msg, strlen(msg));
}

int HelloCountCmd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    return RedisModule_ReplyWithLongLong(ctx, (long long)argc);
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx, "hello", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "hello", HelloCmd, "readonly", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "hello.count", HelloCountCmd, "readonly", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;
}
