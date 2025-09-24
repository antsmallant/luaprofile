// This file is modified version from https://github.com/JieTrancender/game-server/tree/main/3rd/luaprofile

#include "profile.h"
#include "imap.h"
#include "icallpath.h"
#include "lobject.h"
#include "lstate.h"
#include <pthread.h>

// #include <google/profiler.h>

#define MAX_CALL_SIZE               1024
#define MAX_CO_SIZE                 1024
#define NANOSEC                     1000000000
#define MICROSEC                    1000000
#define USE_EXPORT_NAME             1
#define GET_ALL_KIND_PROTOTYPE      1

static char profile_started_key = 'x';

struct profile_context;


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


struct callpath_node;
struct call_frame {
    const void* point;
    const void* prototype;
    struct icallpath_context*   path;
    bool  tail;
    uint64_t call_time;
    uint64_t ret_time;
    uint64_t sub_cost;
    uint64_t real_cost;
    uint64_t alloc_co_cost;
    uint64_t alloc_start;
};

struct call_state {
    lua_State*  co;
    uint64_t    leave_time;
    uint64_t    leave_alloc;
    int         top;
    struct call_frame   call_list[0];
};

struct profile_context {
    uint64_t    start;
    bool        increment_alloc_count;
    uint64_t    alloc_count;
    lua_Alloc   last_alloc_f;
    void*       last_alloc_ud;
    struct imap_context*        cs_map;
    struct icallpath_context*   callpath;
    struct call_state*          cur_cs;
};

struct callpath_node {
    struct callpath_node*   parent;
    const void* point;
    const char* source;
    const char* name;
    int     line;
    int     depth;
    uint64_t ret_time;
    uint64_t count;
    uint64_t record_time;
    uint64_t alloc_count;
};

static struct callpath_node*
callpath_node_create() {
    struct callpath_node* node = (struct callpath_node*)pmalloc(sizeof(*node));
    node->parent = NULL;
    node->point = NULL;
    node->source = NULL;
    node->name = NULL;
    node->line = 0;
    node->depth = 0;
    node->ret_time = 0;
    node->count = 0;
    node->record_time = 0;
    node->alloc_count = 0;
    return node;
}

static struct profile_context *
profile_create() {
    struct profile_context* context = (struct profile_context*)pmalloc(sizeof(*context));
    
    context->start = 0;
    context->cs_map = imap_create();
    context->callpath = NULL;
    context->cur_cs = NULL;
    context->increment_alloc_count = false;
    context->alloc_count = 0;
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
_get_profile(lua_State* L) {
    void *ud = NULL;
    lua_getallocf(L, &ud);
    return ud;
}

static struct icallpath_context*
get_frame_path(struct profile_context* context, lua_State* co, lua_Debug* far, struct icallpath_context* pre_callpath, struct call_frame* frame) {
    if (!context->callpath) {
        struct callpath_node* node = callpath_node_create();
        node->name = "total";
        node->source = node->name;
        context->callpath = icallpath_create(0, node);
    }
    struct icallpath_context* path = pre_callpath;
    if (!path) {
        path = context->callpath;
    }

    struct call_frame* cur_cf = frame;
    uint64_t k = (uint64_t)((uintptr_t)cur_cf->prototype);
    struct icallpath_context* child_path = icallpath_get_child(path, k);
    if (!child_path) {
        struct callpath_node* path_parent = (struct callpath_node*)icallpath_getvalue(path);
        struct callpath_node* node = callpath_node_create();

        node->parent = path_parent;
        node->point = cur_cf->prototype;
        node->depth = path_parent->depth + 1;
        node->ret_time = 0;
        node->record_time = 0;
        node->count = 0;
        node->alloc_count = 0;
        child_path = icallpath_add_child(path, k, node);
    }
    path = child_path;

    struct callpath_node* cur_node = (struct callpath_node*)icallpath_getvalue(path);
    if (cur_node->name == NULL) {
        const char* name = NULL;
        #ifdef USE_EXPORT_NAME
            lua_getinfo(co, "nSl", far);
            name = far->name;
        #else
            lua_getinfo(co, "Sl", far);
        #endif
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
                flag = 'C';
                if(ret) {
                    lua_getinfo(co, "Sl", &ar2);
                    if(ar2.what[0] != 'C') {
                        line = ar2.currentline;
                        source = ar2.source;
                        break;
                    }
                }
            }while(ret);
        }

        cur_node->name = name ? name : "null";
        cur_node->source = source ? source : "null";
        cur_node->line = line;
    }
    
    return path;
}

static void*
_resolve_alloc(void *ud, void *ptr, size_t osize, size_t nsize) {
    struct profile_context* context = (struct profile_context*)ud;
    size_t old = ptr == NULL ? 0 : osize;
    if (nsize > 0 && nsize > old && context->increment_alloc_count) {
        context->alloc_count += (nsize - old);
    }

    void* p = context->last_alloc_f(context->last_alloc_ud, ptr, osize, nsize);
    return p;
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
static const void*
_get_all_kind_prototype(lua_State* L, lua_Debug* far) {
    const void* prototype = NULL;
    if (far->i_ci && far->i_ci->func.p && ttisclosure(s2v(far->i_ci->func.p))) {
        Closure *cl = clvalue(s2v(far->i_ci->func.p));
        if (cl) {
            if (cl->c.tt == LUA_VLCL) {
                prototype = (const void*)cl->l.p;
            } else if (cl->c.tt == LUA_VCCL) {
                prototype = (const void*)cl->c.f;
            }
        }
    } 
    // 通常 LUA_VLCF 可以用下面方式成功获取
    if (!prototype) {
        lua_getinfo(L, "f", far); 
        prototype = lua_topointer(L, -1); 
        //printf("get prototype by getinfo, prototype = %p\n", prototype);
        if (!prototype) {
            printf("get prototype fail at last\n");
        }        
    }
    return prototype;
}

#ifndef GET_ALL_KIND_PROTOTYPE
static const void*
_only_get_vlcl_prototype(lua_State* L, lua_Debug* far) {
    if (far->i_ci && far->i_ci->func.p && ttisclosure(s2v(far->i_ci->func.p))) {
        Closure *cl = clvalue(s2v(far->i_ci->func.p));
        if (cl && cl->c.tt == LUA_VLCL) {
            return (const void*)cl->l.p;
        }
    } 
    return NULL;
}
#endif

static void
_resolve_hook(lua_State* L, lua_Debug* far) {
    struct profile_context* context = _get_profile(L);
    if(context->start == 0) {
        return;
    }

    uint64_t cur_time = gettime();
    context->increment_alloc_count = false;
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
            cs->leave_alloc = 0;
            imap_set(context->cs_map, key, cs);
        }

        if (context->cur_cs) {
            context->cur_cs->leave_time = cur_time;
            context->cur_cs->leave_alloc = context->alloc_count;
        }
        context->cur_cs = cs;
    }
    if (cs->leave_time > 0) {
        assert(cur_time >= cs->leave_time);
        uint64_t co_cost = cur_time - cs->leave_time;
        uint64_t co_alloc = context->alloc_count - cs->leave_alloc;

        int i = 0;
        for (; i < cs->top; i++) {
            cs->call_list[i].sub_cost += co_cost;
            cs->call_list[i].alloc_co_cost += co_alloc;
        }
        cs->leave_time = 0;
        cs->leave_alloc = 0;
    }
    assert(cs->co == L);

    if (event == LUA_HOOKCALL || event == LUA_HOOKTAILCALL) {
        const void* point = NULL;
        if (far->i_ci && far->i_ci->func.p) {
            point = far->i_ci->func.p;
        } else {
            lua_getinfo(L, "f", far);
            point = lua_topointer(L, -1);
        }

        struct icallpath_context* pre_callpath = NULL;
        struct call_frame* pre_frame = cur_callframe(cs);
        if (pre_frame) {
            pre_callpath = pre_frame->path;
        }

        struct call_frame* frame = push_callframe(cs);
        frame->point = point;
        frame->tail = event == LUA_HOOKTAILCALL;
        frame->sub_cost = 0;
        frame->call_time = cur_time;
        frame->alloc_co_cost = 0;
        frame->alloc_start = context->alloc_count;
        const void* prototype = NULL;
#ifdef GET_ALL_KIND_PROTOTYPE
        prototype = _get_all_kind_prototype(L, far);
#else   
        prototype = _only_get_vlcl_prototype(L, far);
#endif
        if (prototype) { 
            frame->prototype = prototype;
        } else {
            frame->prototype = point;
        }        
        frame->path = get_frame_path(context, L, far, pre_callpath, frame);
    } else if (event == LUA_HOOKRET) {
        int len = cs->top;
        if (len <= 0) {
            context->increment_alloc_count = true;
            return;
        }
        bool tail_call = false;
        do {
            struct call_frame* cur_frame = pop_callframe(cs);
            struct callpath_node* cur_path = (struct callpath_node*)icallpath_getvalue(cur_frame->path);
            uint64_t total_cost = cur_time - cur_frame->call_time;
            uint64_t real_cost = total_cost - cur_frame->sub_cost;
            uint64_t alloc_count = context->alloc_count - cur_frame->alloc_start - cur_frame->alloc_co_cost;
            assert(context->alloc_count >= (cur_frame->alloc_start + cur_frame->alloc_co_cost));
            assert(cur_time >= cur_frame->call_time && total_cost >= cur_frame->sub_cost);
            cur_frame->ret_time = cur_time;
            cur_frame->real_cost = real_cost;

            cur_path->ret_time = cur_path->ret_time == 0 ? cur_time : cur_path->ret_time;
            cur_path->record_time += real_cost;
            cur_path->count++;
            cur_path->alloc_count += alloc_count;

            struct call_frame* pre_frame = cur_callframe(cs);
            tail_call = pre_frame ? cur_frame->tail : false;
        }while(tail_call);
    }

    context->increment_alloc_count = true;
}


struct dump_call_path_arg {
    lua_State* L;
    uint64_t record_time;
    uint64_t count;
    uint64_t index;
    uint64_t alloc_count;
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
    child_arg.alloc_count = 0;

    if (icallpath_children_size(path) > 0) {
        lua_newtable(arg->L);
        icallpath_dump_children(path, _dump_call_path_child, &child_arg);
        lua_setfield(arg->L, -2, "children");
    }

    struct callpath_node* node = (struct callpath_node*)icallpath_getvalue(path);
    uint64_t alloc_count = node->alloc_count > child_arg.alloc_count ? node->alloc_count : child_arg.alloc_count;
    uint64_t count = node->count > child_arg.count ? node->count : child_arg.count;
    uint64_t rt = realtime(node->record_time) * MICROSEC;
    uint64_t record_time = rt > child_arg.record_time ? rt : child_arg.record_time;

    arg->record_time += record_time;
    arg->count += count;
    arg->alloc_count += alloc_count;

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

    lua_pushinteger(arg->L, alloc_count);
    lua_setfield(arg->L, -2, "alloc_count");
}
static void dump_call_path(lua_State* L, struct icallpath_context* path) {
    struct dump_call_path_arg arg;
    arg.L = L;
    arg.record_time = 0;
    arg.count = 0;
    arg.index = 0;
    arg.alloc_count = 0;
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

static bool chk_profile_started(lua_State* L) {
    // 检查是否已经启动
    lua_pushlightuserdata(L, &profile_started_key);
    lua_rawget(L, LUA_REGISTRYINDEX);
    if (lua_toboolean(L, -1)) {
        lua_pop(L, 1);
        return true;
    }
    lua_pop(L, 1);  
    return false;  
}

static void set_profile_started(lua_State* L) {
    lua_pushlightuserdata(L, &profile_started_key);
    lua_pushboolean(L, 1);
    lua_rawset(L, LUA_REGISTRYINDEX);
}

static void unset_profile_started(lua_State* L) {
    lua_pushlightuserdata(L, &profile_started_key);
    lua_pushnil(L);
    lua_rawset(L, LUA_REGISTRYINDEX);
}

static int
_lstart(lua_State* L) {
    if (chk_profile_started(L)) {
        printf("start fail, profile already started\n");
        return 0;
    }
    set_profile_started(L);
    struct profile_context* context = profile_create();
    context->start = gettime();
    context->last_alloc_f = lua_getallocf(L, &context->last_alloc_ud);
    lua_setallocf(L, _resolve_alloc, context);
    lua_State* states[MAX_CO_SIZE] = {0};
    int i = get_all_coroutines(L, states, MAX_CO_SIZE);
    for (i = i - 1; i >= 0; i--) {
        lua_sethook(states[i], _resolve_hook, LUA_MASKCALL | LUA_MASKRET, 0);
    }
    context->increment_alloc_count = true;
    return 0;
}

static int
_lstop(lua_State* L) {
    if (!chk_profile_started(L)) {
        printf("stop fail, profile not started\n");
        return 0;
    }
    struct profile_context* context = _get_profile(L);
    if (!context) {
        return 0;
    }
    context->increment_alloc_count = false;
    lua_setallocf(L, context->last_alloc_f, context->last_alloc_ud);
    lua_State* states[MAX_CO_SIZE] = {0};
    int i = get_all_coroutines(L, states, MAX_CO_SIZE);
    for (i = i - 1; i >= 0; i--) {
        lua_sethook(states[i], NULL, 0, 0);
    }
    profile_free(context);
    context = NULL;
    unset_profile_started(L);
    return 0;
}

static int
_lmark(lua_State* L) {
    struct profile_context* context = _get_profile(L);
    if (!context) {
        return 0;
    }
    lua_State* co = lua_tothread(L, 1);
    if(co == NULL) {
        co = L;
    }
    if(context->start != 0) {
        lua_sethook(co, _resolve_hook, LUA_MASKCALL | LUA_MASKRET, 0);
    }
    lua_pushboolean(L, context->start != 0);
    return 1;
}

static int
_lunmark(lua_State* L) {
    struct profile_context* context = _get_profile(L);
    if (!context) {
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
    struct profile_context* context = _get_profile(L);
    if (context && context->callpath) {
        context->increment_alloc_count = false;
        uint64_t record_time = realtime(gettime() - context->start) * MICROSEC;
        lua_pushinteger(L, record_time);
        dump_call_path(L, context->callpath);
        context->increment_alloc_count = true;
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