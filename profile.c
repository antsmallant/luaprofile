// This file is modified version from https://github.com/JieTrancender/game-server/tree/main/3rd/luaprofile

#include "profile.h"
#include "imap.h"
#include "icallpath.h"
#include "lobject.h"
#include "lstate.h"
#include <pthread.h>
#include <stdint.h>
#include <string.h>

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

static char profile_context_key = 'x';

static inline uint64_t
gettime() {
    struct timespec ti;
    // clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ti);
    // clock_gettime(CLOCK_MONOTONIC, &ti);  
    clock_gettime(CLOCK_REALTIME, &ti);  // would be faster
    long sec = ti.tv_sec & 0xffff;
    long nsec = ti.tv_nsec;
    return sec * NANOSEC + nsec;
}

static inline double
realtime(uint64_t t) {
    return (double)t / NANOSEC;
}

struct call_frame {
    const void* prototype;
    struct icallpath_context*   path;
    bool  tail;
    uint64_t call_time;
    uint64_t ret_time;
    uint64_t sub_cost;
    uint64_t real_cost;
};

struct call_state {
    lua_State*  co;
    uint64_t    leave_time;
    int         top;
    struct call_frame   call_list[0];
};

struct profile_context {
    uint64_t    start_time;
    bool        is_ready; // 是否就绪
    bool        running_in_hook;  // 是否正在运行 hook 逻辑

    // 全局内存统计（仅按 alloc/free 维度聚合；realloc 记为增量/减量）
    uint64_t    alloc_bytes_total;
    uint64_t    free_bytes_total;
    uint64_t    alloc_times_total;
    uint64_t    free_times_total;
    uint64_t    realloc_times_total;
    lua_Alloc   last_alloc_f;
    void*       last_alloc_ud;
    struct imap_context*        cs_map;
    struct imap_context*        alloc_map;
    struct imap_context*        symbol_map;
    struct icallpath_context*   callpath;
    struct call_state*          cur_cs;
};

struct callpath_node {
    struct callpath_node*   parent;
    const char* source;
    const char* name;
    int     line;
    int     depth;
    uint64_t ret_time;
    uint64_t count;
    uint64_t record_time;
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
    node->ret_time = 0;
    node->count = 0;
    node->record_time = 0;
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
    context->alloc_bytes_total = 0;
    context->free_bytes_total = 0;
    context->alloc_times_total = 0;
    context->free_times_total = 0;
    context->realloc_times_total = 0;
    context->last_alloc_f = NULL;
    context->last_alloc_ud = NULL;
    return context;
}

static void
_ob_free_call_state(uint64_t key, void* value, void* ud) {
    pfree(value);
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
        node->name = "total";
        node->source = node->name;
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
        node->ret_time = 0;
        node->record_time = 0;
        node->count = 0;
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

// 按路径更新节点
static inline void _mem_accumulate_on_path(struct callpath_node* node,
    size_t add_bytes, uint64_t add_times, size_t sub_bytes, uint64_t sub_times, uint64_t realloc_times) {
    for (struct callpath_node* n = node; n != NULL; n = n->parent) {
        if (add_bytes) n->alloc_bytes += add_bytes;
        if (add_times) n->alloc_times += add_times;
        if (sub_bytes) n->free_bytes += sub_bytes;
        if (sub_times) n->free_times += sub_times;
        if (realloc_times) n->realloc_times += realloc_times;
    }
}

// 按栈更新节点
static inline void _mem_accumulate_on_stack(struct profile_context* context,
    size_t add_bytes, uint64_t add_times, size_t sub_bytes, uint64_t sub_times, uint64_t realloc_times) {
    struct call_state* cs = context->cur_cs;
    if (!cs || cs->top <= 0) return;
    for (int i = 0; i < cs->top; i++) {
        struct call_frame* f = &cs->call_list[i];
        if (!f->path) continue;
        struct callpath_node* n = (struct callpath_node*)icallpath_getvalue(f->path);
        if (!n) continue;
        if (add_bytes) n->alloc_bytes += add_bytes;
        if (add_times) n->alloc_times += add_times;
        if (sub_bytes) n->free_bytes += sub_bytes;
        if (sub_times) n->free_times += sub_times;
        if (realloc_times) n->realloc_times += realloc_times;
    }
}

// 取当前栈的叶子节点（用于 last-alloc-wins 路径归因）
static inline struct callpath_node* _current_leaf_node(struct profile_context* context) {
    struct call_state* cs = context->cur_cs;
    if (!cs) return NULL;
    struct call_frame* leaf = cur_callframe(cs);
    if (!leaf || !leaf->path) return NULL;
    return (struct callpath_node*)icallpath_getvalue(leaf->path);
}

static void*
_resolve_alloc(void *ud, void *ptr, size_t _osize, size_t _nsize) {   
    struct profile_context* context = (struct profile_context*)ud;
    void* alloc_ret = context->last_alloc_f(context->last_alloc_ud, ptr, _osize, _nsize);
    if (context->running_in_hook || !context->is_ready) {
        return alloc_ret;
    }

    size_t oldsize = (ptr == NULL) ? 0 : _osize;
    size_t newsize = _nsize;

    // 本次事件的增量/减量
    size_t add_bytes = 0;
    size_t sub_bytes = 0;
    uint64_t add_times = 0;
    uint64_t sub_times = 0;
    uint64_t realloc_times = 0;

    if (oldsize == 0 && newsize > 0) {
        // alloc

        add_bytes = newsize;
        add_times = 1;

        // 按栈更新节点
        _mem_accumulate_on_stack(context, add_bytes, add_times, 0, 0, 0);

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
            sub_bytes = an->live_bytes; 
            sub_times = (an->live_bytes ? 1 : 0);
            // 按路径更新节点
            if (an->path && an->live_bytes > 0) {
                _mem_accumulate_on_path(an->path, 0, 0, sub_bytes, sub_times, 0);
            }
            pfree(an);
        }

    } else if (oldsize > 0 && newsize > 0) {
        // realloc
        // 参照 gperftools 的逻辑，realloc 拆分为 free 和 alloc 两个事件，但此处为了反映 gc 的压力，不增加 alloc_times 和 free_times。
        // 1、旧 node，free_bytes 加上 oldsizesize，free_times 不加，by_path 传播给父节点；
        // 2、新 node，alloc_bytes 加上 newsize，realloc_times 加 1，by_stack 传播给父节点；

        add_bytes = newsize;
        sub_bytes = oldsize;
        realloc_times = 1;

        // 旧路径：free_bytes += oldsize
        if (sub_bytes) {
            struct alloc_node* anq = (struct alloc_node*)imap_query(context->alloc_map, (uint64_t)(uintptr_t)ptr);
            if (anq && anq->path) {
                _mem_accumulate_on_path(anq->path, 0, 0, sub_bytes, 0, 0);
            }
        }

        // 新路径：alloc_bytes += newsize；realloc_times += 1
        if (add_bytes) {
            _mem_accumulate_on_stack(context, add_bytes, 0, 0, 0, 1);
        }

        // 更新映射（搬移或原地）
        struct callpath_node* leaf_node = _current_leaf_node(context);   
        if (alloc_ret != ptr && alloc_ret != NULL) {
            struct alloc_node* an = (struct alloc_node*)imap_remove(context->alloc_map, (uint64_t)(uintptr_t)ptr);
            if (!an) an = alloc_node_create();
            an->live_bytes = newsize;
            an->path = leaf_node;
            imap_set(context->alloc_map, (uint64_t)(uintptr_t)alloc_ret, an);
        } else {
            struct alloc_node* an = (struct alloc_node*)imap_query(context->alloc_map, (uint64_t)(uintptr_t)ptr);
            bool exists = (an != NULL);
            if (!an) an = alloc_node_create();
            an->live_bytes = newsize;
            an->path = leaf_node;
            if (!exists) imap_set(context->alloc_map, (uint64_t)(uintptr_t)ptr, an);
        }

        printf("realloc: %p -> %p, %zu -> %zu\n", ptr, alloc_ret, oldsize, newsize);
    }

    // 全局累计
    if (add_bytes) context->alloc_bytes_total += add_bytes;
    if (add_times) context->alloc_times_total += add_times;
    if (sub_bytes) context->free_bytes_total += sub_bytes;
    if (sub_times) context->free_times_total += sub_times;
    if (realloc_times) context->realloc_times_total += realloc_times;

    return alloc_ret;
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
static const void* get_prototype(lua_State* L, lua_Debug* ar) {
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

static void
_resolve_hook(lua_State* L, lua_Debug* far) {
    struct profile_context* context = get_profile_context(L);
    if (context == NULL) {
        printf("resolve hook fail, profile not started\n");
        return;
    }
    if(!context->is_ready) {
        return;
    }

    uint64_t cur_time = gettime();
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
            context->cur_cs->leave_time = cur_time;
        }
        context->cur_cs = cs;
    }
    if (cs->leave_time > 0) {
        assert(cur_time >= cs->leave_time);
        uint64_t co_cost = cur_time - cs->leave_time;

        int i = 0;
        for (; i < cs->top; i++) {
            cs->call_list[i].sub_cost += co_cost;
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
        frame->sub_cost = 0;
        frame->call_time = cur_time;
        frame->prototype = get_prototype(L, far);    
        frame->path = get_frame_path(context, L, far, pre_callpath, frame);
    } else if (event == LUA_HOOKRET) {
        int len = cs->top;
        if (len <= 0) {
            context->running_in_hook = false;
            return;
        }
        bool tail_call = false;
        do {
            struct call_frame* cur_frame = pop_callframe(cs);
            struct callpath_node* cur_path = (struct callpath_node*)icallpath_getvalue(cur_frame->path);
            uint64_t total_cost = cur_time - cur_frame->call_time;
            uint64_t real_cost = total_cost - cur_frame->sub_cost;
            assert(cur_time >= cur_frame->call_time && total_cost >= cur_frame->sub_cost);
            cur_frame->ret_time = cur_time;
            cur_frame->real_cost = real_cost;
            cur_path->ret_time = cur_path->ret_time == 0 ? cur_time : cur_path->ret_time;
            cur_path->record_time += real_cost;
            cur_path->count++;

            struct call_frame* pre_frame = cur_callframe(cs);
            tail_call = pre_frame ? cur_frame->tail : false;
        }while(tail_call);
    }

    context->running_in_hook = false;
}


struct dump_call_path_arg {
    lua_State* L;
    uint64_t record_time;
    uint64_t count;
    uint64_t index;
    // 兼容旧字段移除：不再统计 alloc_count
};

static void _dump_call_path(struct icallpath_context* path, struct dump_call_path_arg* arg);

static void _dump_call_path_child(uint64_t key, void* value, void* ud) {
    struct dump_call_path_arg* arg = (struct dump_call_path_arg*)ud;
    _dump_call_path((struct icallpath_context*)value, arg);
    lua_seti(arg->L, -2, ++arg->index);
}

static void _dump_call_path(struct icallpath_context* path, struct dump_call_path_arg* arg) {
    lua_checkstack(arg->L, 3);
    lua_newtable(arg->L);

    struct dump_call_path_arg child_arg;
    child_arg.L = arg->L;
    child_arg.record_time = 0;
    child_arg.count = 0;
    child_arg.index = 0;

    if (icallpath_children_size(path) > 0) {
        lua_newtable(arg->L);
        icallpath_dump_children(path, _dump_call_path_child, &child_arg);
        lua_setfield(arg->L, -2, "children");
    }

    struct callpath_node* node = (struct callpath_node*)icallpath_getvalue(path);
    uint64_t count = node->count > child_arg.count ? node->count : child_arg.count;
    uint64_t rt = realtime(node->record_time) * MICROSEC;
    uint64_t record_time = rt > child_arg.record_time ? rt : child_arg.record_time;
    uint64_t inuse_bytes = node->alloc_bytes >= node->free_bytes ? node->alloc_bytes-node->free_bytes : 9999999999;

    arg->record_time += record_time;
    arg->count += count;

    char name[512] = {0};
    snprintf(name, sizeof(name)-1, "%s %s:%d", node->name ? node->name : "", node->source ? node->source : "", node->line);
    lua_pushstring(arg->L, name);
    lua_setfield(arg->L, -2, "name");

    lua_pushinteger(arg->L, count);
    lua_setfield(arg->L, -2, "count");

    lua_pushinteger(arg->L, record_time);
    lua_setfield(arg->L, -2, "value");

    lua_pushinteger(arg->L, node->ret_time);
    lua_setfield(arg->L, -2, "rettime");

    lua_pushinteger(arg->L, (lua_Integer)node->alloc_bytes);
    lua_setfield(arg->L, -2, "alloc_bytes");
    lua_pushinteger(arg->L, (lua_Integer)node->free_bytes);
    lua_setfield(arg->L, -2, "free_bytes");
    lua_pushinteger(arg->L, (lua_Integer)node->alloc_times);
    lua_setfield(arg->L, -2, "alloc_times");
    lua_pushinteger(arg->L, (lua_Integer)node->free_times);
    lua_setfield(arg->L, -2, "free_times");
    lua_pushinteger(arg->L, (lua_Integer)node->realloc_times);
    lua_setfield(arg->L, -2, "realloc_times");
    lua_pushinteger(arg->L, (lua_Integer)inuse_bytes);
    lua_setfield(arg->L, -2, "inuse_bytes");
}

static void dump_call_path(lua_State* L, struct icallpath_context* path) {
    struct dump_call_path_arg arg;
    arg.L = L;
    arg.record_time = 0;
    arg.count = 0;
    arg.index = 0;
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

static int
_lstart(lua_State* L) {
    struct profile_context* context = get_profile_context(L);
    if (context != NULL) {
        printf("start fail, profile already started\n");
        return 0;
    }
    context = profile_create();
    context->start_time = gettime();
    context->is_ready = true;
    context->running_in_hook = true;
    context->last_alloc_f = lua_getallocf(L, &context->last_alloc_ud);
    lua_setallocf(L, _resolve_alloc, context);
    set_profile_context(L, context);

    // stop gc before set hook
    int gc_was_running = lua_gc(L, LUA_GCISRUNNING, 0);
    if (gc_was_running) { lua_gc(L, LUA_GCSTOP, 0); }
    lua_State* states[MAX_CO_SIZE] = {0};
    int i = get_all_coroutines(L, states, MAX_CO_SIZE);
    for (i = i - 1; i >= 0; i--) {
        lua_sethook(states[i], _resolve_hook, LUA_MASKCALL | LUA_MASKRET, 0);
    }
    if (gc_was_running) { lua_gc(L, LUA_GCRESTART, 0); }

    printf("luaprofile started, last_alloc_ud = %p\n", context->last_alloc_ud);
    context->running_in_hook = false;
    return 0;
}

static int
_lstop(lua_State* L) {
    struct profile_context* context = get_profile_context(L);
    if (context == NULL) {
        printf("stop fail, profile not started\n");
        return 0;
    }
    context->is_ready = false;
    context->running_in_hook = true;
    lua_setallocf(L, context->last_alloc_f, context->last_alloc_ud);

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
        lua_sethook(co, _resolve_hook, LUA_MASKCALL | LUA_MASKRET, 0);
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
    if (context && context->callpath) {
        context->running_in_hook = true;
        // stop gc before dump
        int gc_was_running = lua_gc(L, LUA_GCISRUNNING, 0);
        if (gc_was_running) { lua_gc(L, LUA_GCSTOP, 0); }        
        struct callpath_node* node = icallpath_getvalue(context->callpath);
        node->alloc_bytes = context->alloc_bytes_total;
        node->free_bytes = context->free_bytes_total;
        node->alloc_times = context->alloc_times_total;
        node->free_times = context->free_times_total;
        node->realloc_times = context->realloc_times_total;
        uint64_t record_time = realtime(gettime() - context->start_time) * MICROSEC;
        lua_pushinteger(L, record_time);
        dump_call_path(L, context->callpath);
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