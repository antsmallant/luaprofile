// This file is modified version from https://github.com/JieTrancender/game-server/tree/main/3rd/luaprofile

#include "profile.h"
#include "imap.h"
#include "smap.h"
#include "icallpath.h"
#include "lobject.h"
#include "lstate.h"
#include "lua.h"
#include "lauxlib.h"
#include <pthread.h>
#include <stdint.h>
#include <string.h>
#include <math.h>

/*
callpath node 构成一棵树，每个frame可以在这个树中找到一个 node。framestack 从 root frame 到 cur frame, 对应这棵树的某条路径。  

prototype 相当于一个 lua closure/c closure/c function 在 lua vm 中的唯一指针。它是 callpath node 的 key，一个新的 frame 要找到对应的 callpath node，就用 prototype 从父frame 的 node 里面查找 child node，找不到则创建。  

相同 prototype 的闭包或函数，可能会在 callpath tree 对应多个 node，这取决于它的父亲是谁。   

node1
  node21
    node31
对应着 frame1 -> frame2 -> frame3 的调用关系。 
*/

#define MAX_CALL_SIZE               1024
#define MAX_CO_SIZE                 1024
#define NANOSEC                     1000000000
#define MICROSEC                    1000000
#define MAX_SAMPLE_DEPTH            256

// 模式定义（统一用于 CPU/Memory）
#define MODE_OFF                    0
#define MODE_PROFILE                1
#define MODE_SAMPLE                 2

#define DEFAULT_SAMPLE_PERIOD       10000

static char profile_context_key = 'x';

static inline uint64_t
gettime() {
    struct timespec ti;
    clock_gettime(CLOCK_REALTIME, &ti);  // would be faster
    long sec = ti.tv_sec & 0xffff;
    long nsec = ti.tv_nsec;
    return sec * NANOSEC + nsec;
}

static inline double
realtime_sec(uint64_t t) {
    return (double)t / NANOSEC;
}

static inline double
realtime_ms(uint64_t t) {
    return (double)t / NANOSEC * MICROSEC;
}

// 读取启动参数：{ cpu = "off|profile|sample", mem = "off|profile|sample", sample_period = int }
static bool
read_arg(lua_State* L, int* out_cpu_mode, int* out_mem_mode, int* out_sample_period) {
    if (!out_cpu_mode || !out_mem_mode || !out_sample_period) return false;
    *out_cpu_mode = MODE_PROFILE;
    *out_mem_mode = MODE_PROFILE;
    *out_sample_period = DEFAULT_SAMPLE_PERIOD;
    if (lua_gettop(L) < 1 || !lua_istable(L, 1)) return true;

    lua_getfield(L, 1, "cpu");
    if (lua_isstring(L, -1)) {
        const char* s = lua_tostring(L, -1);
        if (strcmp(s, "off") == 0) *out_cpu_mode = MODE_OFF;
        else if (strcmp(s, "profile") == 0) *out_cpu_mode = MODE_PROFILE;
        else if (strcmp(s, "sample") == 0) *out_cpu_mode = MODE_SAMPLE;
        else {printf("invalid cpu mode: %s\n", s); return false;}
    }
    lua_pop(L, 1);

    lua_getfield(L, 1, "mem");
    if (lua_isstring(L, -1)) {
        const char* s = lua_tostring(L, -1);
        if (strcmp(s, "off") == 0) *out_mem_mode = MODE_OFF;
        else if (strcmp(s, "profile") == 0) *out_mem_mode = MODE_PROFILE;
        else if (strcmp(s, "sample") == 0) *out_mem_mode = MODE_SAMPLE;
        else {printf("invalid mem mode: %s\n", s); return false;}
    }
    lua_pop(L, 1);

    lua_getfield(L, 1, "sample_period");
    if (lua_isinteger(L, -1)) {
        int sp = (int)lua_tointeger(L, -1);
        if (sp > 0) *out_sample_period = sp;
    }
    lua_pop(L, 1);
    return true;
}

struct call_frame {
    const void* prototype;
    struct icallpath_context*   path;
    bool  tail;
    uint64_t call_time;
    uint64_t co_cost;     // co yield cost 
};

struct call_state {
    lua_State*  co;
    uint64_t    leave_time; // co yield begin time
    int         top;
    struct call_frame call_list[0];
};

struct profile_context {
    uint64_t    start_time;
    bool        is_ready;
    bool        running_in_hook;
    lua_Alloc   last_alloc_f;
    void*       last_alloc_ud;
    struct imap_context*        cs_map;
    struct imap_context*        alloc_map;
    struct imap_context*        symbol_map;
    smap_t*                     sample_map;   // folded stacks for cpu sampling
    struct icallpath_context*   callpath;
    struct call_state*          cur_cs;
    int         cpu_mode;       // MODE_*
    int         mem_mode;       // MODE_*
    int         sample_period;  // instruction count for LUA_MASKCOUNT
    uint64_t    rng_state;      // RNG state for sampling gaps
};

struct callpath_node {
    struct callpath_node*   parent;
    const char* source;
    const char* name;
    int     line;
    int     depth;
    uint64_t last_ret_time;
    uint64_t call_count;
    uint64_t real_cost;
    uint64_t cpu_samples;    // sampling count (leaf samples), aggregated at dump
    uint64_t alloc_bytes;
    uint64_t free_bytes;
    uint64_t alloc_times;
    uint64_t free_times;
    uint64_t realloc_times;
};

struct alloc_node {
    size_t live_bytes;                // 当前存活字节
    struct callpath_node* path;       // 当前所有权路径
};

struct symbol_info {
    char* name;
    char* source;
    int line;
};

// 简单的字符串 HashMap（链式散列），用于 CPU 抽样折叠栈
// use external smap

static struct callpath_node*
callpath_node_create() {
    struct callpath_node* node = (struct callpath_node*)pmalloc(sizeof(*node));
    node->parent = NULL;
    node->source = NULL;
    node->name = NULL;
    node->line = 0;
    node->depth = 0;
    node->last_ret_time = 0;
    node->call_count = 0;
    node->real_cost = 0;
    node->cpu_samples = 0;
    node->alloc_bytes = 0;
    node->free_bytes = 0;
    node->alloc_times = 0;
    node->free_times = 0;
    node->realloc_times = 0;
    return node;
}

static struct alloc_node*
alloc_node_create() {
    struct alloc_node* node = (struct alloc_node*)pmalloc(sizeof(*node));
    node->live_bytes = 0;
    node->path = NULL;
    return node;
}

static inline char*
pstrdup(const char* s) {
    if (!s) return NULL;
    size_t n = strlen(s);
    char* d = (char*)pmalloc(n + 1);
    memcpy(d, s, n);
    d[n] = '\0';
    return d;
}

// free counters stored as smap values (uint64_t*)
static void _free_counter_cb(const char* key, void* value, void* ud) {
    (void)key; (void)ud;
    if (value) pfree(value);
}

struct smap_dump_ctx {
    lua_State* L;
    size_t idx;
};

struct fg_dump_ctx {
    luaL_Buffer* buf;
    struct imap_context* symbol_map;
};

static void _fg_dump_cb(const char* key, void* value, void* ud) {
    struct fg_dump_ctx* ctx = (struct fg_dump_ctx*)ud;
    luaL_Buffer* b = ctx->buf;
    uint64_t samples = value ? *(uint64_t*)value : 0;
    if (samples == 0) return;
    const char* p = key;
    char token[64];
    size_t tlen = 0;
    int first = 1;
    while (1) {
        char c = *p++;
        if (c == ';' || c == '\0') {
            token[tlen] = '\0';
            void* fptr = NULL;
            sscanf(token, "%p", &fptr);
            uint64_t sym_key = (uint64_t)((uintptr_t)fptr);
            struct symbol_info* si = (struct symbol_info*)imap_query(ctx->symbol_map, sym_key);
            if (!first) luaL_addchar(b, ';');
            if (si) {
                char namebuf[512];
                const char* nm = (si->name && si->name[0]) ? si->name : "anonymous";
                const char* src = (si->source && si->source[0]) ? si->source : "(source)";
                int ln = si->line;
                int n = snprintf(namebuf, sizeof(namebuf)-1, "%s %s:%d", nm, src, ln);
                if (n > 0) luaL_addlstring(b, namebuf, (size_t)n);
            } else {
                luaL_addlstring(b, token, tlen);
            }
            tlen = 0;
            first = 0;
            if (c == '\0') break;
        } else {
            if (tlen + 1 < sizeof(token)) token[tlen++] = c;
        }
    }
    char tail[64];
    int m = snprintf(tail, sizeof(tail)-1, " %llu\n", (unsigned long long)samples);
    if (m > 0) luaL_addlstring(b, tail, (size_t)m);
}

static struct profile_context *
profile_create() {
    struct profile_context* context = (struct profile_context*)pmalloc(sizeof(*context));
    
    context->start_time = 0;
    context->is_ready = false;
    context->cs_map = imap_create();
    context->alloc_map = imap_create();
    context->symbol_map = imap_create();
    context->sample_map = smap_create(2048);
    context->callpath = NULL;
    context->cur_cs = NULL;
    context->running_in_hook = false;
    context->last_alloc_f = NULL;
    context->last_alloc_ud = NULL;
    context->cpu_mode = MODE_PROFILE;       // default: profile
    context->mem_mode = MODE_PROFILE;       // default: profile
    context->sample_period = DEFAULT_SAMPLE_PERIOD; // default instruction period for sampling
    context->rng_state = 0;
    return context;
}

static void
_ob_free_call_state(uint64_t key, void* value, void* ud) {
    pfree(value);
}

static void
_ob_free_symbol(uint64_t key, void* value, void* ud) {
    (void)key; (void)ud;
    struct symbol_info* si = (struct symbol_info*)value;
    if (si) {
        if (si->name) pfree(si->name);
        if (si->source) pfree(si->source);
        pfree(si);
    }
}

static void
_ob_free_alloc_node(uint64_t key, void* value, void* ud) {
    (void)key; (void)ud;
    struct alloc_node* an = (struct alloc_node*)value;
    if (an) {
        pfree(an);
    }
}

static void
profile_free(struct profile_context* context) {
    if (context->callpath) {
        icallpath_free(context->callpath);
        context->callpath = NULL;
    }

    imap_dump(context->cs_map, _ob_free_call_state, NULL);
    imap_free(context->cs_map);
    imap_dump(context->symbol_map, _ob_free_symbol, NULL);
    imap_free(context->symbol_map);
    if (context->sample_map) {
        // free smap entries' values (uint64_t*)
        smap_iterate(context->sample_map, _free_counter_cb, NULL);
        smap_free(context->sample_map);
    }
    imap_dump(context->alloc_map, _ob_free_alloc_node, NULL);
    imap_free(context->alloc_map);
    pfree(context);
}

static inline struct call_frame *
push_callframe(struct call_state* cs) {
    if(cs->top >= MAX_CALL_SIZE) {
        assert(false);
    }
    return &cs->call_list[cs->top++];
}

static inline struct call_frame *
pop_callframe(struct call_state* cs) {
    if(cs->top<=0) {
        assert(false);
    }
    return &cs->call_list[--cs->top];
}

static inline struct call_frame *
cur_callframe(struct call_state* cs) {
    if(cs->top<=0) {
        return NULL;
    }

    uint64_t idx = cs->top-1;
    return &cs->call_list[idx];
}

static inline struct profile_context *
get_profile_context(lua_State* L) {
    struct profile_context* ctx = NULL;
    lua_pushlightuserdata(L, &profile_context_key);
    lua_rawget(L, LUA_REGISTRYINDEX);
    ctx = (struct profile_context*)lua_touserdata(L, -1);
        lua_pop(L, 1);
    return ctx;
}

static void set_profile_context(lua_State* L, struct profile_context* ctx) {
    lua_pushlightuserdata(L, &profile_context_key);
    lua_pushlightuserdata(L, (void*)ctx);
    lua_rawset(L, LUA_REGISTRYINDEX);
}

static void unset_profile_context(lua_State* L) {
    lua_pushlightuserdata(L, &profile_context_key);
    lua_pushnil(L);
    lua_rawset(L, LUA_REGISTRYINDEX);
}

// --- Random gap generator (exponential/geometric) ---
static inline uint64_t xorshift64(uint64_t* s) {
    uint64_t x = (*s) ? *s : 88172645463393265ULL;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *s = x;
    return x;
}

static inline int next_exponential_gap(struct profile_context* ctx) {
    // mean = ctx->sample_period (instructions)
    // u in (0,1], avoid 0
    uint64_t r = xorshift64(&ctx->rng_state);
    double u = ( (r >> 11) * (1.0 / 9007199254740992.0) ); // 53-bit to [0,1)
    if (u <= 0.0) u = 1e-12;
    int gap = (int)floor(-log(u) * (double)ctx->sample_period);
    if (gap < 1) gap = 1;
    return gap;
}

static struct icallpath_context*
get_frame_path(struct profile_context* context, lua_State* co, lua_Debug* far, struct icallpath_context* pre_path, struct call_frame* frame) {
    if (!context->callpath) {
        struct callpath_node* node = callpath_node_create();
        node->name = "root";
        node->source = "root";
        node->call_count = 1;
        context->callpath = icallpath_create(0, node);
    }
    if (!pre_path) {
        pre_path = context->callpath;
    }

    struct call_frame* cur_cf = frame;
    uint64_t k = (uint64_t)((uintptr_t)cur_cf->prototype);
    struct icallpath_context* cur_path = icallpath_get_child(pre_path, k);
    if (!cur_path) {
        struct callpath_node* path_parent = (struct callpath_node*)icallpath_getvalue(pre_path);
        struct callpath_node* node = callpath_node_create();

        node->parent = path_parent;
        node->depth = path_parent->depth + 1;
        node->last_ret_time = 0;
        node->real_cost = 0;
        node->call_count = 0;
        cur_path = icallpath_add_child(pre_path, k, node);
    }

    struct callpath_node* cur_node = (struct callpath_node*)icallpath_getvalue(cur_path);
    if (cur_node->name == NULL) {
        uint64_t sym_key = (uint64_t)((uintptr_t)cur_cf->prototype);
        struct symbol_info* si = (struct symbol_info*)imap_query(context->symbol_map, sym_key);
        if (!si) {
            lua_getinfo(co, "nSl", far);
            const char* name = far->name;
            int line = far->linedefined;
            const char* source = far->source;
            char flag = far->what[0];
            if (flag == 'C') {
                lua_Debug ar2;
                int i=0;
                int ret = 0;
                do {
                    i++;
                    ret = lua_getstack(co, i, &ar2);
                    if(ret) {
                        lua_getinfo(co, "Sl", &ar2);
                        if(ar2.what[0] != 'C') {
                            line = ar2.currentline;
                            source = ar2.source;
                            break;
                        }
                    }
                } while(ret);
            }
            si = (struct symbol_info*)pmalloc(sizeof(struct symbol_info));
            si->name = pstrdup(name ? name : "null");
            si->source = pstrdup(source ? source : "null");
            si->line = line;
            imap_set(context->symbol_map, sym_key, si);
        }
        cur_node->name = si->name;
        cur_node->source = si->source;
        cur_node->line = si->line;
    }
    
    return cur_path;
}

// 按路径更新节点（仅更新当前节点的 self 计数，父链累计推迟到 dump 聚合）
static inline void _mem_update_on_path(struct callpath_node* node,
    size_t alloc_bytes, uint64_t alloc_times, size_t free_bytes, uint64_t free_times, uint64_t realloc_times) {
    if (!node) return;
    if (alloc_bytes) node->alloc_bytes += alloc_bytes;
    if (alloc_times) node->alloc_times += alloc_times;
    if (free_bytes) node->free_bytes += free_bytes;
    if (free_times) node->free_times += free_times;
    if (realloc_times) node->realloc_times += realloc_times;
}

// 取当前栈的叶子节点
static inline struct callpath_node* _current_leaf_node(struct profile_context* context) {
    struct call_state* cs = context->cur_cs;
    if (!cs) return NULL;
    struct call_frame* leaf = cur_callframe(cs);
    if (!leaf || !leaf->path) return NULL;
    return (struct callpath_node*)icallpath_getvalue(leaf->path);
}

/*
获取所有类型的 prototype，包括 LUA_VLCL、LUA_VCCL、LUA_VLCF。   
如果没有正确获取 prototype，那么像 tonumber 和 print 这类 LUA_VLCF 使用栈上的函数指针来充当 prototype,
会导致同一层的这类函数被合并为同一个节点，比如下面的代码，最终 print 会错误的合并到 tonumber 的节点中，显示为
2 次 tonumber 调用。 

验证代码:
local function test1()
    local profile = require "profile"
    local json = require "cjson"
    profile.start()
    tonumber("123")    
    print("111")
    local result = profile.stop()
    skynet.error("test1:", json.encode(result))
end
test1()

结果可用 https://jsongrid.com/ 查看
*/
static const void* _get_prototype(lua_State* L, lua_Debug* ar) {
    const void* proto = NULL;
    if (ar->i_ci && ar->i_ci->func.p) {
        const TValue* tv = s2v(ar->i_ci->func.p);
        if (ttislcf(tv)) {
            // LUA_VLCF：轻量 C 函数，直接取 c 函数指针
            proto = (const void*)fvalue(tv);
        } else if (ttisclosure(tv)) {
            const Closure* cl = clvalue(tv);
            if (cl->c.tt == LUA_VLCL) {
                proto = (const void*)cl->l.p;   // Lua 函数 → Proto*
            } else if (cl->c.tt == LUA_VCCL) {
                proto = (const void*)cl->c.f; // C 闭包 → lua_CFunction
            }
        }
    }
    if (!proto) {
        // 兜底：仍可能遇到少数拿不到 TValue 的情况
        lua_getinfo(L, "f", ar);
        proto = lua_topointer(L, -1);
        lua_pop(L, 1);
        printf("get prototype by getinfo: %p\n", proto);
    }
    return proto;
}

// Ensure symbol_map has an entry for the given prototype
static inline void _ensure_symbol(struct profile_context* context, lua_State* L, lua_Debug* ar, const void* proto) {
    uint64_t sym_key = (uint64_t)((uintptr_t)proto);
    if (imap_query(context->symbol_map, sym_key) != NULL) return;

    const char* name = ar->name;
    int line = ar->linedefined;
    const char* source = ar->source;
    char flag = ar->what[0];

    struct symbol_info* si = (struct symbol_info*)pmalloc(sizeof(struct symbol_info));
    if (flag == 'C') {
        // C 帧：不要绑定到 Lua 行号；优先使用 name，缺失则用指针占位
        char buf[64];
        const char* nm = name;
        if (!nm) {
            snprintf(buf, sizeof(buf)-1, "cfunc@%p", proto);
            nm = buf;
        }
        si->name = pstrdup(nm);
        si->source = pstrdup("=[C]");
        si->line = 0;
    } else {
        const char* nm = name;
        if (!nm) {
            nm = (line == 0) ? "chunk" : "anonymous";
        }
        si->name = pstrdup(nm);
        si->source = pstrdup(source ? source : "");
        si->line = line;
    }
    imap_set(context->symbol_map, sym_key, si);
}

// hook alloc/free/realloc 事件
static void*
_hook_alloc(void *ud, void *ptr, size_t _osize, size_t _nsize) {   
    struct profile_context* context = (struct profile_context*)ud;
    void* alloc_ret = context->last_alloc_f(context->last_alloc_ud, ptr, _osize, _nsize);
    if (context->running_in_hook || !context->is_ready) {
        return alloc_ret;
    }

    size_t oldsize = (ptr == NULL) ? 0 : _osize;
    size_t newsize = _nsize;

    if (oldsize == 0 && newsize > 0) {
        // alloc

        // 更新节点
        struct callpath_node* leaf = _current_leaf_node(context);
        if (leaf) _mem_update_on_path(leaf, newsize, 1, 0, 0, 0);

        // 创建映射
        struct alloc_node* an = (struct alloc_node*)imap_query(context->alloc_map, (uint64_t)(uintptr_t)alloc_ret);
        if (an == NULL) an = alloc_node_create();
        an->live_bytes = newsize;
        an->path = _current_leaf_node(context);
        imap_set(context->alloc_map, (uint64_t)(uintptr_t)alloc_ret, an);

    } else if (oldsize > 0 && newsize == 0) {
        // free
        
        struct alloc_node* an = (struct alloc_node*)imap_remove(context->alloc_map, (uint64_t)(uintptr_t)ptr);
        if (an) {
            // 更新节点
            size_t sub_bytes = an->live_bytes; 
            uint64_t sub_times = (an->live_bytes ? 1 : 0);
            if (an->path && an->live_bytes > 0) {
                _mem_update_on_path(an->path, 0, 0, sub_bytes, sub_times, 0);
            }
            pfree(an);
            an = NULL;
        }

    } else if (oldsize > 0 && newsize > 0) {
        // realloc
        
        // 参照 gperftools 的逻辑，realloc 拆分为 free 和 alloc 两个事件，但此处为了反映 gc 的压力，不增加 alloc_times 和 free_times。
        // 1、旧 node，free_bytes 加上 oldsizesize；
        // 2、新 node，alloc_bytes 加上 newsize，realloc_times 加 1；

        // 旧路径
        struct alloc_node* old_an = (struct alloc_node*)imap_query(context->alloc_map, (uint64_t)(uintptr_t)ptr);
        if (old_an && old_an->path) {
            _mem_update_on_path(old_an->path, 0, 0, oldsize, 0, 0);
        }

        // 新路径
        struct callpath_node* leaf = _current_leaf_node(context);
        if (leaf) _mem_update_on_path(leaf, newsize, 0, 0, 0, 1);

        // 更新映射（搬移或原地）
        if (alloc_ret != ptr && alloc_ret != NULL) {
            struct alloc_node* an = (struct alloc_node*)imap_remove(context->alloc_map, (uint64_t)(uintptr_t)ptr);
            if (!an) an = alloc_node_create();
            an->live_bytes = newsize;
            an->path = _current_leaf_node(context);
            imap_set(context->alloc_map, (uint64_t)(uintptr_t)alloc_ret, an);
        } else {
            struct alloc_node* an = (struct alloc_node*)imap_query(context->alloc_map, (uint64_t)(uintptr_t)ptr);
            bool exists = (an != NULL);
            if (!exists) an = alloc_node_create();
            an->live_bytes = newsize;
            an->path = _current_leaf_node(context);
            if (!exists) imap_set(context->alloc_map, (uint64_t)(uintptr_t)ptr, an);
        }
    }

    return alloc_ret;
}

// hook call/ret 事件
static void
_hook_call(lua_State* L, lua_Debug* far) {
    struct profile_context* context = get_profile_context(L);
    if (context == NULL) {
        printf("resolve hook fail, profile not started\n");
        return;
    }
    if(!context->is_ready) {
        return;
    }

    context->running_in_hook = true;

    uint64_t cur_time = gettime();
    int event = far->event;

    // COUNT 事件优先处理：采样只抓栈
    if (event == LUA_HOOKCOUNT) {
        // 抓栈 → 生成折叠 key（prototype 地址串）→ 在 sample_map 中累加计数
        void* frames[MAX_SAMPLE_DEPTH];
        int depth = 0;
        lua_Debug ar;
        while (depth < MAX_SAMPLE_DEPTH && lua_getstack(L, depth, &ar)) {
            lua_getinfo(L, "nSl", &ar);
            const void* proto = _get_prototype(L, &ar);
            _ensure_symbol(context, L, &ar, proto);
            frames[depth] = (void*)proto;
            depth++;
        }
        if (depth > 0) {
            // 估算 key 长度：每帧 2+16 字符 + 分隔符（root->leaf 顺序）
            size_t bufcap = (size_t)depth * 20 + 1;
            char* buf = (char*)pmalloc(bufcap);
            size_t off = 0;
            for (int i = depth - 1; i >= 0; i--) {
                int n = snprintf(buf + off, bufcap - off, "%p%s", frames[i], (i == 0 ? "" : ";"));
                if (n < 0) { off = 0; break; }
                off += (size_t)n;
                if (off >= bufcap) { off = bufcap - 1; break; }
            }
            if (off > 0) {
                void* pv = smap_get(context->sample_map, buf);
                uint64_t* counter = (uint64_t*)pv;
                if (!counter) {
                    counter = (uint64_t*)pmalloc(sizeof(uint64_t));
                    *counter = 0;
                    smap_set(context->sample_map, buf, counter);
                }
                *counter += 1;
            }
            pfree(buf);
        }
        if (context->cpu_mode == MODE_SAMPLE) {
            int gap = next_exponential_gap(context);
            printf("set hook with gap %d\n", gap);
            lua_sethook(L, _hook_call, LUA_MASKCOUNT, gap);
        }
        context->running_in_hook = false;
        return;
    }

    struct call_state* cs = context->cur_cs;
    if (!context->cur_cs || context->cur_cs->co != L) {
        uint64_t key = (uint64_t)((uintptr_t)L);
        cs = imap_query(context->cs_map, key);
        if (cs == NULL) {
            cs = (struct call_state*)pmalloc(sizeof(struct call_state) + sizeof(struct call_frame)*MAX_CALL_SIZE);
            cs->co = L; 
            cs->top = 0;
            cs->leave_time = 0;
            imap_set(context->cs_map, key, cs);
        }

        if (context->cur_cs) {
            context->cur_cs->leave_time = cur_time;
        }
        context->cur_cs = cs;
    }
    if (cs->leave_time > 0) {
        assert(cur_time >= cs->leave_time);
        uint64_t co_cost = cur_time - cs->leave_time;

        for (int i = 0; i < cs->top; i++) {
            cs->call_list[i].co_cost += co_cost;
        }
        cs->leave_time = 0;
    }
    assert(cs->co == L);

    if (event == LUA_HOOKCALL || event == LUA_HOOKTAILCALL) {
        struct icallpath_context* pre_callpath = NULL;
        struct call_frame* pre_frame = cur_callframe(cs);
        if (pre_frame) {
            pre_callpath = pre_frame->path;
        }
        struct call_frame* frame = push_callframe(cs);
        frame->tail = (event == LUA_HOOKTAILCALL);
        frame->co_cost = 0;
        frame->call_time = cur_time;
        frame->prototype = _get_prototype(L, far);    
        frame->path = get_frame_path(context, L, far, pre_callpath, frame);
        if (frame->path) {
            struct callpath_node* node = (struct callpath_node*)icallpath_getvalue(frame->path);
            ++node->call_count;
        }

    } else if (event == LUA_HOOKRET) {
        if (cs->top <= 0) {
            context->running_in_hook = false;
            return;
        }
        bool tail_call = false;
        do {
            struct call_frame* cur_frame = pop_callframe(cs);
            struct callpath_node* cur_path = (struct callpath_node*)icallpath_getvalue(cur_frame->path);
            uint64_t total_cost = cur_time - cur_frame->call_time;
            uint64_t real_cost = total_cost - cur_frame->co_cost;
            assert(cur_time >= cur_frame->call_time && total_cost >= cur_frame->co_cost);
            cur_path->last_ret_time = cur_time;
            cur_path->real_cost += real_cost;

            struct call_frame* pre_frame = cur_callframe(cs);
            tail_call = pre_frame ? cur_frame->tail : false;
        } while(tail_call);
    }

    context->running_in_hook = false;
}


struct dump_call_path_arg {
    lua_State* L;
    uint64_t real_cost;
    uint64_t index;
    uint64_t cpu_samples_sum;
    uint64_t alloc_bytes_sum;
    uint64_t free_bytes_sum;
    uint64_t alloc_times_sum;
    uint64_t free_times_sum;
    uint64_t realloc_times_sum;
};

static void _init_dump_call_path_arg(struct dump_call_path_arg* arg, lua_State* L) {
    arg->L = L;
    arg->real_cost = 0;
    arg->index = 0;
    arg->cpu_samples_sum = 0;
    arg->alloc_bytes_sum = 0;
    arg->free_bytes_sum = 0;
    arg->alloc_times_sum = 0;
    arg->free_times_sum = 0;
    arg->realloc_times_sum = 0;
}

static void _dump_call_path(struct icallpath_context* path, struct dump_call_path_arg* arg);

static void _dump_call_path_child(uint64_t key, void* value, void* ud) {
    struct dump_call_path_arg* arg = (struct dump_call_path_arg*)ud;
    _dump_call_path((struct icallpath_context*)value, arg);
    lua_seti(arg->L, -2, ++arg->index);
}

static void _dump_call_path(struct icallpath_context* path, struct dump_call_path_arg* arg) {
    lua_checkstack(arg->L, 3);
    lua_newtable(arg->L);

    // 递归获取所有子结点的指标和
    struct dump_call_path_arg child_arg;
    _init_dump_call_path_arg(&child_arg, arg->L);
    if (icallpath_children_size(path) > 0) {
        lua_newtable(arg->L);
        icallpath_dump_children(path, _dump_call_path_child, &child_arg);
        lua_setfield(arg->L, -2, "children");
    }

    struct callpath_node* node = (struct callpath_node*)icallpath_getvalue(path);

    // 本节点的聚合指标=本节点指标+所有子结点的指标
    uint64_t alloc_bytes_incl = node->alloc_bytes + child_arg.alloc_bytes_sum;
    uint64_t free_bytes_incl = node->free_bytes + child_arg.free_bytes_sum;
    uint64_t alloc_times_incl = node->alloc_times + child_arg.alloc_times_sum;
    uint64_t free_times_incl = node->free_times + child_arg.free_times_sum;
    uint64_t realloc_times_incl = node->realloc_times + child_arg.realloc_times_sum;
    uint64_t real_cost = node->real_cost + child_arg.real_cost;
    uint64_t cpu_samples_incl = node->cpu_samples + child_arg.cpu_samples_sum;
    // 本节点的其他指标
    uint64_t call_count = node->call_count;
    uint64_t inuse_bytes = alloc_bytes_incl >= free_bytes_incl ? alloc_bytes_incl - free_bytes_incl : 9999999999;

    // 累加到父节点
    arg->real_cost += real_cost;
    arg->cpu_samples_sum += cpu_samples_incl;
    arg->alloc_bytes_sum += alloc_bytes_incl;
    arg->free_bytes_sum += free_bytes_incl;
    arg->alloc_times_sum += alloc_times_incl;
    arg->free_times_sum += free_times_incl;
    arg->realloc_times_sum += realloc_times_incl;

    // 导出本节点的聚合指标
    char name[512] = {0};
    snprintf(name, sizeof(name)-1, "%s %s:%d", node->name ? node->name : "", node->source ? node->source : "", node->line);
    lua_pushstring(arg->L, name);
    lua_setfield(arg->L, -2, "name");

    lua_pushinteger(arg->L, call_count);
    lua_setfield(arg->L, -2, "call_count");

    lua_pushinteger(arg->L, real_cost);
    lua_setfield(arg->L, -2, "cpu_cost_ns");

    lua_pushinteger(arg->L, node->last_ret_time);
    lua_setfield(arg->L, -2, "last_ret_time");

    lua_pushinteger(arg->L, (lua_Integer)cpu_samples_incl);
    lua_setfield(arg->L, -2, "cpu_samples");

    lua_pushinteger(arg->L, (lua_Integer)alloc_bytes_incl);
    lua_setfield(arg->L, -2, "alloc_bytes");

    lua_pushinteger(arg->L, (lua_Integer)free_bytes_incl);
    lua_setfield(arg->L, -2, "free_bytes");

    lua_pushinteger(arg->L, (lua_Integer)alloc_times_incl);
    lua_setfield(arg->L, -2, "alloc_times");

    lua_pushinteger(arg->L, (lua_Integer)free_times_incl);
    lua_setfield(arg->L, -2, "free_times");

    lua_pushinteger(arg->L, (lua_Integer)realloc_times_incl);
    lua_setfield(arg->L, -2, "realloc_times");

    lua_pushinteger(arg->L, (lua_Integer)inuse_bytes);
    lua_setfield(arg->L, -2, "inuse_bytes");
}

static void dump_call_path(lua_State* L, struct icallpath_context* path) {
    struct dump_call_path_arg arg;
    _init_dump_call_path_arg(&arg, L);
    _dump_call_path(path, &arg);
}

static int 
get_all_coroutines(lua_State* L, lua_State** result, int maxsize) {
    int i = 0;
    struct global_State* lG = L->l_G;
    result[i++] = lG->mainthread;

    struct GCObject* obj = lG->allgc;
    while (obj && i < maxsize) {
        if (obj->tt == LUA_TTHREAD) {
            result[i++] = gco2th(obj);
        }
        obj = obj->next;
    }
    return i;
}

static void
_hook_all_co(lua_State* L) {
    // stop gc before set hook
    int gc_was_running = lua_gc(L, LUA_GCISRUNNING, 0);
    if (gc_was_running) { lua_gc(L, LUA_GCSTOP, 0); }
    lua_State* states[MAX_CO_SIZE] = {0};
    int i = get_all_coroutines(L, states, MAX_CO_SIZE);
    struct profile_context* ctx = get_profile_context(L);
    for (i = i - 1; i >= 0; i--) {
        if (ctx && ctx->cpu_mode == MODE_SAMPLE) {
            // sampling: instruction-count based with exponential gaps
            int gap = next_exponential_gap(ctx);
            printf("set hook with gap %d\n", gap);
            lua_sethook(states[i], _hook_call, LUA_MASKCOUNT, gap);
        } else if (ctx && ctx->cpu_mode == MODE_PROFILE) {
            // profiling (full call/ret)
            lua_sethook(states[i], _hook_call, LUA_MASKCALL | LUA_MASKRET, 0);
        } else {
            // off
            lua_sethook(states[i], NULL, 0, 0);
        }
    }
    if (gc_was_running) { lua_gc(L, LUA_GCRESTART, 0); }
}

static void 
_unhook_all_co(lua_State* L) {
    // stop gc before unset hook
    int gc_was_running = lua_gc(L, LUA_GCISRUNNING, 0);
    if (gc_was_running) { lua_gc(L, LUA_GCSTOP, 0); }     
    lua_State* states[MAX_CO_SIZE] = {0};
    int sz = get_all_coroutines(L, states, MAX_CO_SIZE);
    int i;
    for (i = sz - 1; i >= 0; i--) {
        lua_sethook(states[i], NULL, 0, 0);
    }
    if (gc_was_running) { lua_gc(L, LUA_GCRESTART, 0); }
}

static int
_lstart(lua_State* L) {
    struct profile_context* context = get_profile_context(L);
    if (context != NULL) {
        printf("start fail, profile already started\n");
        return 0;
    }

    // parse options: start([opts]), opts is a table
    int cpu_mode = MODE_PROFILE; // default profile
    int mem_mode = MODE_PROFILE; // default profile
    int sample_period = DEFAULT_SAMPLE_PERIOD;
    bool read_ok = read_arg(L, &cpu_mode, &mem_mode, &sample_period);
    if (!read_ok) {
        printf("start fail, invalid options\n");
        return 0;
    }

    lua_gc(L, LUA_GCCOLLECT, 0);  // full gc

    context = profile_create();
    context->running_in_hook = true;
    context->start_time = gettime();
    context->is_ready = true;
    context->cpu_mode = cpu_mode;
    context->mem_mode = mem_mode;
    context->sample_period = sample_period;
    // seed rng with time xor state pointer
    context->rng_state = gettime() ^ (uint64_t)(uintptr_t)context;
    context->last_alloc_f = lua_getallocf(L, &context->last_alloc_ud);
    if (mem_mode != MODE_OFF) {
        lua_setallocf(L, _hook_alloc, context);
    }
    set_profile_context(L, context);
    _hook_all_co(L);
    context->running_in_hook = false;
    printf("luaprofile started, cpu_mode = %d, mem_mode = %d, sample_period = %d, last_alloc_ud = %p\n", context->cpu_mode, context->mem_mode, context->sample_period, context->last_alloc_ud);    
    return 0;
}

static int
_lstop(lua_State* L) {
    struct profile_context* context = get_profile_context(L);
    if (context == NULL) {
        printf("stop fail, profile not started\n");
        return 0;
    }
    
    context->running_in_hook = true;
    context->is_ready = false;
    lua_setallocf(L, context->last_alloc_f, context->last_alloc_ud);
    _unhook_all_co(L);
    unset_profile_context(L);
    profile_free(context);
    context = NULL;
    printf("luaprofile stopped\n");
    return 0;
}

static int
_lmark(lua_State* L) {
    struct profile_context* context = get_profile_context(L);
    if (context == NULL) {
        printf("mark fail, profile not started\n");
        return 0;
    }
    lua_State* co = lua_tothread(L, 1);
    if(co == NULL) {
        co = L;
    }
    if(context->is_ready) {
        lua_sethook(co, _hook_call, LUA_MASKCALL | LUA_MASKRET, 0);
    }
    lua_pushboolean(L, context->is_ready);
    return 1;
}

static int
_lunmark(lua_State* L) {
    struct profile_context* context = get_profile_context(L);
    if (context == NULL) {
        printf("unmark fail, profile not started\n");
        return 0;
    }
    lua_State* co = lua_tothread(L, 1);
    if(co == NULL) {
        co = L;
    }
    lua_sethook(co, NULL, 0, 0);
    return 0;
}

static int
_ldump(lua_State* L) {
    struct profile_context* context = get_profile_context(L);
    if (context) {
        // full gc
        lua_gc(L, LUA_GCCOLLECT, 0);

        // stop gc before dump
        int gc_was_running = lua_gc(L, LUA_GCISRUNNING, 0);
        if (gc_was_running) { lua_gc(L, LUA_GCSTOP, 0); }
        context->running_in_hook = true;
        uint64_t cur_time = gettime();
        uint64_t profile_time = cur_time - context->start_time;
        lua_pushinteger(L, profile_time);

        if (context->cpu_mode == MODE_SAMPLE) {
            // dump folded stacks in FlameGraph format (lines of: path1;path2 count)
            luaL_Buffer b;
            luaL_buffinit(L, &b);
            struct fg_dump_ctx fctx = { .buf = &b, .symbol_map = context->symbol_map };
            smap_iterate(context->sample_map, _fg_dump_cb, &fctx);
            luaL_pushresult(&b);
        } else {
            // tracing dump
            if (context->callpath) {
                struct callpath_node* root = (struct callpath_node*)icallpath_getvalue(context->callpath);
                root->last_ret_time = cur_time;
                dump_call_path(L, context->callpath);
            } else {
                lua_newtable(L);
            }
        }
        context->running_in_hook = false;
        if (gc_was_running) { lua_gc(L, LUA_GCRESTART, 0); }
        return 2;
    }
    return 0;
}

int
luaopen_luaprofilec(lua_State* L) {
    luaL_checkversion(L);
     luaL_Reg l[] = {
        {"start", _lstart},
        {"stop", _lstop},
        {"mark", _lmark},
        {"unmark", _lunmark},
        {"dump", _ldump},
        {NULL, NULL},
    };
    luaL_newlib(L, l);
    return 1;
}