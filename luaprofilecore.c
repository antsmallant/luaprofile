/*
Modify from https://github.com/lsg2020/luaprofile .
Original version is https://github.com/lvzixun/luaprofile .

Check https://github.com/antsmallant/luaprofile for more.
*/

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <time.h> 
#include <assert.h>
#include <stdbool.h>
#include <string.h>
#include <stdint.h>
#include <math.h>
#include <errno.h>
#include "lobject.h"
#include "lfunc.h"
#include "lstate.h"
#include "lua.h"
#include "lauxlib.h"


#define prealloc  realloc
#define pmalloc   malloc
#define pfree     free
#define pcalloc   calloc

#define MAX_CALL_SIZE               1024
#define MAX_CO_SIZE                 1024
#define NANOSEC                     1000000000
#define MICROSEC                    1000000

#define MODE_OFF                    0
#define MODE_ON                     1

#define DEFAULT_IMAP_SLOT_SIZE      1024

static char profile_context_key = 'x';

struct icallpath_context {
    uint64_t key;
    void* value;
    struct imap_context* children;
};

enum imap_status {
    IS_NONE,
    IS_EXIST,
    IS_REMOVE,
};

struct imap_slot {
    uint64_t key;
    void* value;
    enum imap_status status;
    struct imap_slot* next;
};

struct imap_context {
    struct imap_slot* slots;
    size_t size;
    size_t count;
    struct imap_slot* lastfree;
};

typedef void(*observer)(uint64_t key, void* value, void* ud);
static void imap_set(struct imap_context* imap, uint64_t key, void* value);
static void icallpath_free(struct icallpath_context* icallpath);

static struct imap_context *
imap_create(void) {
    struct imap_context* imap = (struct imap_context*)pmalloc(sizeof(*imap));
    imap->slots = (struct imap_slot*)pcalloc(DEFAULT_IMAP_SLOT_SIZE, sizeof(struct imap_slot));
    imap->size = DEFAULT_IMAP_SLOT_SIZE;
    imap->count = 0;
    imap->lastfree = imap->slots + imap->size;
    return imap;
}

static void
imap_free(struct imap_context* imap) {
    pfree(imap->slots);
    pfree(imap);
}

static inline uint64_t
_imap_hash(struct imap_context* imap, uint64_t key) {
    uint64_t hash = key % (uint64_t)(imap->size);
    return hash;
}

static void
_imap_rehash(struct imap_context* imap) {
    size_t new_sz = DEFAULT_IMAP_SLOT_SIZE;
    struct imap_slot* old_slots = imap->slots;
    size_t old_count = imap->count;
    size_t old_size = imap->size;
    while(new_sz <= imap->count) {
        new_sz *= 2;
    }

    struct imap_slot* new_slots = (struct imap_slot*)pcalloc(new_sz, sizeof(struct imap_slot));
    imap->lastfree = new_slots + new_sz;
    imap->size = new_sz;
    imap->slots = new_slots;
    imap->count = 0;

    size_t i=0;
    for(i=0; i<old_size; i++) {
        struct imap_slot* p = &(old_slots[i]);
        enum imap_status status = p->status;
        if(status == IS_EXIST) {
            imap_set(imap, p->key, p->value);
        }
    }

    assert(old_count == imap->count);
    pfree(old_slots);
}

static struct imap_slot *
_imap_query(struct imap_context* imap, uint64_t key) {
    uint64_t hash = _imap_hash(imap, key);
    struct imap_slot* p = &(imap->slots[hash]);
    if(p->status != IS_NONE) {
        while(p) {
            if(p->key == key && p->status == IS_EXIST) {
                return p;
            }
            p = p->next;
        }
    }
    return NULL;
}

static void *
imap_query(struct imap_context* imap, uint64_t key) {
    struct imap_slot* p = _imap_query(imap, key);
    if(p) {
        return p->value;
    }
    return NULL;
}

static struct imap_slot *
_imap_getfree(struct imap_context* imap) {
    while(imap->lastfree > imap->slots) {
        imap->lastfree--;
        if(imap->lastfree->status == IS_NONE) {
            return imap->lastfree;
        }
    }
    return NULL;
}

static void
imap_set(struct imap_context* imap, uint64_t key, void* value) {
    assert(value);
    uint64_t hash = _imap_hash(imap, key);
    struct imap_slot* p = &(imap->slots[hash]);
    if(p->status == IS_EXIST) {
        struct imap_slot* np = p;
        while(np) {
            if(np->key == key && np->status == IS_EXIST) {
                np->value = value;
                return;
            }
            np = np->next;
        }

        np = _imap_getfree(imap);
        if(np == NULL) {
            _imap_rehash(imap);
            imap_set(imap, key, value);
            return;
        }

        uint64_t main_hash = _imap_hash(imap, p->key);
        np->next = p->next;
        p->next = np;
        if(main_hash == hash) {
            p = np;
        }else {
            np->key = p->key;
            np->value = p->value;
            np->status = IS_EXIST;
        }
    }

    imap->count++;
    p->status = IS_EXIST;
    p->key = key;
    p->value = value;
}

static void *
imap_remove(struct imap_context* imap, uint64_t key) {
    struct imap_slot* p = _imap_query(imap, key);
    if(p) {
        imap->count--;
        p->status = IS_REMOVE;
        return p->value;
    }
    return NULL;
}

static void
imap_dump(struct imap_context* imap, observer observer_cb, void* ud) {
    size_t i=0;
    for(i=0; i<imap->size; i++) {
        struct imap_slot* v = &imap->slots[i];
        if(v->status == IS_EXIST) {
            observer_cb(v->key, v->value, ud);
        }
    }
}

static size_t
imap_size(struct imap_context* imap) {
    return imap->count;
}

static struct icallpath_context* icallpath_create(uint64_t key, void* value) {
    struct icallpath_context* icallpath = (struct icallpath_context*)pmalloc(sizeof(*icallpath));
    icallpath->key = key;
    icallpath->value = value;
    icallpath->children = imap_create();

    return icallpath;
}

static void icallpath_free_child(uint64_t key, void* value, void* ud) {
    icallpath_free((struct icallpath_context*)value);
}

static void icallpath_free(struct icallpath_context* icallpath) {
    if (icallpath->value) {
        pfree(icallpath->value);
        icallpath->value = NULL;
    }
    imap_dump(icallpath->children, icallpath_free_child, NULL);
    imap_free(icallpath->children);
    pfree(icallpath);
}

static struct icallpath_context* icallpath_get_child(struct icallpath_context* icallpath, uint64_t key) {
    void* child_path = imap_query(icallpath->children, key);
    return (struct icallpath_context*)child_path;
}

static struct icallpath_context* icallpath_add_child(struct icallpath_context* icallpath, uint64_t key, void* value) {
    struct icallpath_context* child_path = icallpath_create(key, value);
    imap_set(icallpath->children, key, child_path);
    return child_path;
}

static void* icallpath_getvalue(struct icallpath_context* icallpath) {
    return icallpath->value;
}

static void icallpath_dump_children(struct icallpath_context* icallpath, observer observer_cb, void* ud) {
    imap_dump(icallpath->children, observer_cb, ud);
}

static size_t icallpath_children_size(struct icallpath_context* icallpath) {
    return imap_size(icallpath->children);
}

// 获取单调递增的时间戳（纳秒），不会被 NTP 调整。
// 只用于计算时间差，不能当成绝对时间戳用于获取当前的年月日。
// 如果需要获取绝对时间戳，请使用 get_realtime_ns。    
static inline uint64_t
get_mono_ns() {
    struct timespec ti;
    clock_gettime(CLOCK_MONOTONIC, &ti);
    uint64_t sec = (uint64_t)ti.tv_sec;
    uint64_t nsec = (uint64_t)ti.tv_nsec;
    return sec * (uint64_t)NANOSEC + nsec;
}

#ifdef GET_REALTIME
// 获取绝对时间戳（纳秒），会受 NTP 调整。
// 可以用于获取当前的年月日。
static inline uint64_t
get_realtime_ns() {
    struct timespec ti;
    clock_gettime(CLOCK_REALTIME, &ti);
    uint64_t sec = (uint64_t)ti.tv_sec;
    uint64_t nsec = (uint64_t)ti.tv_nsec;
    return sec * (uint64_t)NANOSEC + nsec;
}
#endif

// 读取启动参数：{ mem_profile = "off|on" }
static bool
read_arg(lua_State* L, int* out_mem_profile) {
    if (!out_mem_profile) return false;
    if (lua_gettop(L) < 1 || !lua_istable(L, 1)) return true;

    // 是否启用 内存 profile
    lua_getfield(L, 1, "mem_profile");
    if (lua_isstring(L, -1)) {
        const char* s = lua_tostring(L, -1);
        if (strcmp(s, "off") == 0) *out_mem_profile = MODE_OFF;
        else if (strcmp(s, "on") == 0) *out_mem_profile = MODE_ON;
        else {printf("invalid mem_profile mode: %s\n", s); return false;}
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
    struct icallpath_context*   callpath;
    struct call_state*          cur_cs;
    int         mem_profile_mode;       // MODE_*
    uint64_t    profile_cost_ns;
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

struct dump_call_path_arg {
    struct profile_context* pcontext;
    lua_State* L;
    uint64_t index;
    uint64_t alloc_bytes_sum;
    uint64_t free_bytes_sum;
    uint64_t alloc_times_sum;
    uint64_t free_times_sum;
    uint64_t realloc_times_sum;
};

static void _init_dump_call_path_arg(struct dump_call_path_arg* arg, struct profile_context* pcontext, lua_State* L) {
    arg->pcontext = pcontext;
    arg->L = L;
    arg->index = 0;
    arg->alloc_bytes_sum = 0;
    arg->free_bytes_sum = 0;
    arg->alloc_times_sum = 0;
    arg->free_times_sum = 0;
    arg->realloc_times_sum = 0;
}

struct sum_root_stat_arg {
    uint64_t real_cost_sum;
};

static void _init_sum_root_stat_arg(struct sum_root_stat_arg* arg) {
    arg->real_cost_sum = 0;
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

static struct profile_context *
profile_create() {
    struct profile_context* context = (struct profile_context*)pmalloc(sizeof(*context));
    
    context->start_time = 0;
    context->is_ready = false;
    context->cs_map = imap_create();
    context->alloc_map = imap_create();
    context->symbol_map = imap_create();
    context->callpath = NULL;
    context->cur_cs = NULL;
    context->running_in_hook = false;
    context->last_alloc_f = NULL;
    context->last_alloc_ud = NULL;
    context->mem_profile_mode = MODE_OFF;
    context->profile_cost_ns = 0;
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
获取各种类型函数的 prototype，包括 LUA_VLCL、LUA_VCCL、LUA_VLCF。   
如果没有正确获取 prototype，那么像 tonumber 和 print 这类 LUA_VLCF 使用栈上的函数指针来充当 prototype,
会导致同一层的这类函数被合并为同一个节点，比如下面的代码，最终 print 会错误的合并到 tonumber 的节点中，显示为
2 次 tonumber 调用。 

local function test1()
    tonumber("123")    
    print("111")
end
*/
static const void* _get_prototype(lua_State* L, lua_Debug* ar) {
    const void* proto = NULL;
    if (ar->i_ci && ar->i_ci->func.p) {
        const TValue* tv = s2v(ar->i_ci->func.p);
        if (ttislcf(tv)) {
            proto = (const void*)fvalue(tv);   // LUA_VLCF：轻量 C 函数，直接取 c 函数指针
        } else if (ttisclosure(tv)) {
            const Closure* cl = clvalue(tv);
            if (cl->c.tt == LUA_VLCL) {
                proto = (const void*)cl->l.p;  // LUA_VLCL：Lua 闭包
            } else if (cl->c.tt == LUA_VCCL) {
                proto = (const void*)cl->c.f;  // LUA_VCCL：C 闭包
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
        // 1、旧 node，free_bytes 加上 oldsize；
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
    uint64_t begin_time = get_mono_ns();

    struct profile_context* context = get_profile_context(L);
    if (context == NULL) {
        printf("resolve hook fail, profile not started\n");
        return;
    }
    if(!context->is_ready) {
        return;
    }

    context->running_in_hook = true;

    int event = far->event;

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
            context->cur_cs->leave_time = begin_time;
        }
        context->cur_cs = cs;
    }
    if (cs->leave_time > 0) {
        assert(begin_time >= cs->leave_time);
        uint64_t co_cost = begin_time - cs->leave_time;

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
        frame->prototype = _get_prototype(L, far);    
        frame->path = get_frame_path(context, L, far, pre_callpath, frame);
        if (frame->path) {
            struct callpath_node* node = (struct callpath_node*)icallpath_getvalue(frame->path);
            ++node->call_count;
        }
        frame->call_time = get_mono_ns();

    } else if (event == LUA_HOOKRET) {
        if (cs->top <= 0) {
            context->running_in_hook = false;
            return;
        }
        bool tail_call = false;
        do {
            struct call_frame* cur_frame = pop_callframe(cs);
            struct callpath_node* cur_path = (struct callpath_node*)icallpath_getvalue(cur_frame->path);
            uint64_t total_cost = begin_time - cur_frame->call_time;
            uint64_t real_cost = total_cost - cur_frame->co_cost;
            assert(begin_time >= cur_frame->call_time && total_cost >= cur_frame->co_cost);
            cur_path->last_ret_time = begin_time;
            cur_path->real_cost += real_cost;

            struct call_frame* pre_frame = cur_callframe(cs);
            tail_call = pre_frame ? cur_frame->tail : false;
        } while(tail_call);
    }

    context->profile_cost_ns += (get_mono_ns() - begin_time);
    context->running_in_hook = false;
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

    // 递归获取所有子节点的指标和
    struct dump_call_path_arg child_arg;
    _init_dump_call_path_arg(&child_arg, arg->pcontext, arg->L);
    if (icallpath_children_size(path) > 0) {
        lua_newtable(arg->L);
        icallpath_dump_children(path, _dump_call_path_child, &child_arg);
        lua_setfield(arg->L, -2, "children");
    }

    struct callpath_node* node = (struct callpath_node*)icallpath_getvalue(path);

    // 本节点的聚合指标=本节点指标+所有子节点的指标
    uint64_t alloc_bytes_incl = node->alloc_bytes + child_arg.alloc_bytes_sum;
    uint64_t free_bytes_incl = node->free_bytes + child_arg.free_bytes_sum;
    uint64_t alloc_times_incl = node->alloc_times + child_arg.alloc_times_sum;
    uint64_t free_times_incl = node->free_times + child_arg.free_times_sum;
    uint64_t realloc_times_incl = node->realloc_times + child_arg.realloc_times_sum;

    // 本节点的其他指标
    uint64_t real_cost = node->real_cost;
    uint64_t call_count = node->call_count;
    uint64_t inuse_bytes = (alloc_bytes_incl >= free_bytes_incl ? alloc_bytes_incl - free_bytes_incl : 9999999999);

    // 累加到父节点
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

    lua_pushinteger(arg->L, node->last_ret_time);
    lua_setfield(arg->L, -2, "last_ret_time");
    
    lua_pushinteger(arg->L, call_count);
    lua_setfield(arg->L, -2, "call_count");

    lua_pushinteger(arg->L, real_cost);
    lua_setfield(arg->L, -2, "cpu_cost_ns");

    uint64_t parent_real_cost = 0;
    if (node->parent) {
        parent_real_cost = node->parent->real_cost;
    }
    double percent = parent_real_cost > 0 ? ((double)real_cost / parent_real_cost * 100.0) : 100;
    char percent_str[32] = {0};
    snprintf(percent_str, sizeof(percent_str)-1, "%.2f", percent);
    lua_pushstring(arg->L, percent_str);
    lua_setfield(arg->L, -2, "cpu_cost_percent");

    if (MODE_ON == arg->pcontext->mem_profile_mode) {
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

    if (path == arg->pcontext->callpath) {
        lua_pushinteger(arg->L, arg->pcontext->profile_cost_ns);
        lua_setfield(arg->L, -2, "profile_cost_ns");
    }
}

static void sum_root_stat(uint64_t key, void* value, void* ud) {
    struct sum_root_stat_arg* arg = (struct sum_root_stat_arg*)ud;
    struct icallpath_context* path = (struct icallpath_context*)value;
    struct callpath_node* node = (struct callpath_node*)icallpath_getvalue(path);
    arg->real_cost_sum += node->real_cost;
}

static void update_root_stat(struct profile_context* pcontext, lua_State* L) {
    struct icallpath_context* path = pcontext->callpath;
    if (!path) return;
    struct callpath_node* root = (struct callpath_node*)icallpath_getvalue(path);
    if (!root) return;

    root->last_ret_time = get_mono_ns();
    if (icallpath_children_size(path) > 0) {
        struct sum_root_stat_arg arg;
        _init_sum_root_stat_arg(&arg);
        icallpath_dump_children(path, sum_root_stat, &arg);
        root->real_cost = arg.real_cost_sum;
    }
}

static void dump_call_path(struct profile_context* pcontext, lua_State* L) {
    struct dump_call_path_arg arg;
    _init_dump_call_path_arg(&arg, pcontext, L);
    _dump_call_path(pcontext->callpath, &arg);
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

static int _stop_gc_if_need(lua_State* L) {
    // stop gc before set hook
    int gc_was_running = lua_gc(L, LUA_GCISRUNNING, 0);
    if (gc_was_running) { lua_gc(L, LUA_GCSTOP, 0); }
    return gc_was_running;
}

static void _restart_gc_if_need(lua_State* L, int gc_was_running) {
    if (gc_was_running) { lua_gc(L, LUA_GCRESTART, 0); }
}

static int
_lmark_all(lua_State* L) {
    struct profile_context* ctx = get_profile_context(L);
    if (!ctx) {
        printf("mark all co fail, profile not started\n");
        lua_pushboolean(L, false);
        lua_pushstring(L, "profile not started");
        return 2;
    }

    int gc_was_running = _stop_gc_if_need(L);

    lua_State* states[MAX_CO_SIZE] = {0};
    int i = get_all_coroutines(L, states, MAX_CO_SIZE);
    for (i = i - 1; i >= 0; i--) {
        lua_sethook(states[i], _hook_call, LUA_MASKCALL | LUA_MASKRET, 0);
    }
    _restart_gc_if_need(L, gc_was_running);
    lua_pushboolean(L, true);
    return 1;
}

static int 
_lunmark_all(lua_State* L) {
    struct profile_context* ctx = get_profile_context(L);
    if (!ctx) {
        printf("unhook all co fail, profile not started\n");
        lua_pushboolean(L, false);
        lua_pushstring(L, "profile not started");
        return 2;
    }
    // stop gc before unset hook
    int gc_was_running = _stop_gc_if_need(L); 
    lua_State* states[MAX_CO_SIZE] = {0};
    int sz = get_all_coroutines(L, states, MAX_CO_SIZE);
    int i;
    for (i = sz - 1; i >= 0; i--) {
        lua_sethook(states[i], NULL, 0, 0);
    }
    _restart_gc_if_need(L, gc_was_running);
    lua_pushboolean(L, true);
    return 1;
}

static int
_lstart(lua_State* L) {
    struct profile_context* context = get_profile_context(L);
    if (context != NULL) {
        printf("start fail, profile already started\n");
        return 0;
    }

    // parse options: start([opts]), opts is a table
    int mem_profile_mode = MODE_OFF;  // default
    bool read_ok = read_arg(L, &mem_profile_mode);
    if (!read_ok) {
        printf("start fail, invalid options\n");
        return 0;
    }

    // full gc before start
    lua_gc(L, LUA_GCCOLLECT, 0);  

    context = profile_create();
    context->running_in_hook = true;
    context->start_time = get_mono_ns();
    context->is_ready = true;
    context->mem_profile_mode = mem_profile_mode;
    context->last_alloc_f = lua_getallocf(L, &context->last_alloc_ud);
    if (MODE_ON == mem_profile_mode) {
        lua_setallocf(L, _hook_alloc, context);
    }
    set_profile_context(L, context);
    
    context->running_in_hook = false;
    printf("luaprofile started, mem_profile_mode = %d, last_alloc_ud = %p\n", context->mem_profile_mode, context->last_alloc_ud);    
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
        // full gc to free objects, make mem profile more accurate
        lua_gc(L, LUA_GCCOLLECT, 0);

        // stop gc before dump
        int gc_was_running = _stop_gc_if_need(L); 
        context->running_in_hook = true;

        uint64_t cur_time = get_mono_ns();
        uint64_t profile_time = cur_time - context->start_time;
        lua_pushinteger(L, profile_time);

        // tracing dump
        if (context->callpath) {
            update_root_stat(context, L);
            dump_call_path(context, L);
        } else {
            lua_newtable(L);
        }

        context->running_in_hook = false;
        _restart_gc_if_need(L, gc_was_running);
        return 2;
    }
    return 0;
}

static int _lget_mono_ns(lua_State* L) {
    lua_pushinteger(L, get_mono_ns());
    return 1;
}

// sleep(seconds): 使用 POSIX nanosleep，支持小数秒，自动处理被信号打断
static int _lsleep(lua_State* L) {
    lua_Number sec = luaL_checknumber(L, 1);
    if (sec < 0) sec = 0;

    lua_Number integral = 0;
    lua_Number frac = modf(sec, &integral);

    struct timespec req;
    req.tv_sec = (time_t)integral;
    long nsec = (long)(frac * 1000000000.0);
    if (nsec < 0) nsec = 0;
    if (nsec >= 1000000000L) {
        req.tv_sec += 1;
        nsec -= 1000000000L;
    }
    req.tv_nsec = nsec;

    while (nanosleep(&req, &req) == -1 && errno == EINTR) {
        // 被信号中断则继续睡剩余时间
    }
    return 0;
}

int
luaopen_luaprofilecore(lua_State* L) {
    luaL_checkversion(L);
     luaL_Reg l[] = {
        {"start", _lstart},
        {"stop", _lstop},
        {"mark", _lmark},
        {"unmark", _lunmark},
        {"mark_all", _lmark_all},
        {"unmark_all", _lunmark_all},
        {"dump", _ldump},
        {"getnanosec", _lget_mono_ns},
        {"sleep", _lsleep},
        {NULL, NULL},
    };
    luaL_newlib(L, l);
    return 1;
}