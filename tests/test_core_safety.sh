#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BUILD_DIR="$ROOT_DIR/tests/.build-core-safety"
LUA_SRC_DIR="$ROOT_DIR/3rd/lua-5.4.8/src"
LUA_BIN="$LUA_SRC_DIR/lua"

mkdir -p "$BUILD_DIR"

make -C "$ROOT_DIR/3rd/lua-5.4.8" linux >/dev/null

gcc -DNDEBUG -shared -fPIC -Wall -Wextra -g -O2 \
    -I"$LUA_SRC_DIR" \
    -o "$BUILD_DIR/luaprofilecore.so" \
    "$ROOT_DIR/luaprofilecore.c"

cat > "$BUILD_DIR/core_safety_unit.c" <<'C_EOF'
#define NDEBUG 1
#include "../../luaprofilecore.c"

static int failures = 0;

static void check(int condition, const char* message) {
    if (!condition) {
        fprintf(stderr, "FAIL: %s\n", message);
        failures++;
    }
}

static void* failing_alloc(void* ud, void* ptr, size_t osize, size_t nsize) {
    (void)ud;
    (void)ptr;
    (void)osize;
    (void)nsize;
    return NULL;
}

static void* successful_alloc(void* ud, void* ptr, size_t osize, size_t nsize) {
    (void)ud;
    (void)osize;
    if (nsize == 0) {
        pfree(ptr);
        return NULL;
    }
    return prealloc(ptr, nsize);
}

static void test_push_callframe_normal(void) {
    struct call_state* cs = (struct call_state*)pmalloc(sizeof(*cs) + sizeof(struct call_frame) * MAX_CALL_SIZE);
    memset(cs, 0, sizeof(*cs) + sizeof(struct call_frame) * MAX_CALL_SIZE);

    struct call_frame* frame = push_callframe(cs);

    check(frame == &cs->call_list[0], "push_callframe returns next slot when capacity is available");
    check(cs->top == 1, "push_callframe increments top on success");
    pfree(cs);
}

static void test_push_callframe_full(void) {
    struct call_state* cs = (struct call_state*)pmalloc(sizeof(*cs) + sizeof(struct call_frame) * MAX_CALL_SIZE);
    memset(cs, 0, sizeof(*cs) + sizeof(struct call_frame) * MAX_CALL_SIZE);
    cs->top = MAX_CALL_SIZE;

    struct call_frame* frame = push_callframe(cs);

    check(frame == NULL, "push_callframe returns NULL when call stack is full");
    check(cs->top == MAX_CALL_SIZE, "push_callframe does not increment top on overflow");
    pfree(cs);
}

static void test_alloc_success_counted(void) {
    struct profile_context context;
    memset(&context, 0, sizeof(context));
    context.is_ready = true;
    context.running_in_hook = false;
    context.last_alloc_f = successful_alloc;
    context.alloc_map = imap_create();

    struct call_state* cs = (struct call_state*)pmalloc(sizeof(*cs) + sizeof(struct call_frame));
    memset(cs, 0, sizeof(*cs) + sizeof(struct call_frame));
    struct callpath_node* node = callpath_node_create();
    struct icallpath_context* path = icallpath_create(1, node);
    cs->top = 1;
    cs->call_list[0].path = path;
    context.cur_cs = cs;

    void* ret = _hook_alloc(&context, NULL, 0, 128);
    struct alloc_node* an = (struct alloc_node*)imap_query(context.alloc_map, (uint64_t)(uintptr_t)ret);

    check(ret != NULL, "_hook_alloc returns allocated pointer when allocator succeeds");
    check(node->alloc_bytes == 128, "successful alloc adds alloc_bytes");
    check(node->alloc_times == 1, "successful alloc adds alloc_times");
    check(an != NULL, "successful alloc inserts allocation into alloc_map");
    check(an && an->live_bytes == 128, "alloc_map records live bytes for successful alloc");
    check(an && an->path == node, "alloc_map records owner path for successful alloc");

    an = (struct alloc_node*)imap_remove(context.alloc_map, (uint64_t)(uintptr_t)ret);
    if (an) pfree(an);
    pfree(ret);
    icallpath_free(path);
    imap_free(context.alloc_map);
    pfree(cs);
}

static void test_alloc_failure_not_counted(void) {
    struct profile_context context;
    memset(&context, 0, sizeof(context));
    context.is_ready = true;
    context.running_in_hook = false;
    context.last_alloc_f = failing_alloc;
    context.alloc_map = imap_create();

    struct call_state* cs = (struct call_state*)pmalloc(sizeof(*cs) + sizeof(struct call_frame));
    memset(cs, 0, sizeof(*cs) + sizeof(struct call_frame));
    struct callpath_node* node = callpath_node_create();
    struct icallpath_context* path = icallpath_create(1, node);
    cs->top = 1;
    cs->call_list[0].path = path;
    context.cur_cs = cs;

    void* ret = _hook_alloc(&context, NULL, 0, 128);

    check(ret == NULL, "_hook_alloc returns NULL when allocator fails");
    check(node->alloc_bytes == 0, "failed alloc does not add alloc_bytes");
    check(node->alloc_times == 0, "failed alloc does not add alloc_times");
    check(imap_query(context.alloc_map, 0) == NULL, "failed alloc does not insert NULL key into alloc_map");

    icallpath_free(path);
    imap_free(context.alloc_map);
    pfree(cs);
}

int main(void) {
    test_push_callframe_normal();
    test_push_callframe_full();
    test_alloc_success_counted();
    test_alloc_failure_not_counted();
    return failures == 0 ? 0 : 1;
}
C_EOF

gcc -DNDEBUG -Wall -Wextra -g -O2 \
    -I"$LUA_SRC_DIR" \
    -o "$BUILD_DIR/core_safety_unit" \
    "$BUILD_DIR/core_safety_unit.c" \
    "$LUA_SRC_DIR/liblua.a" -lm -ldl

"$BUILD_DIR/core_safety_unit"
"$LUA_BIN" "$ROOT_DIR/tests/deep_stack_overflow.lua" "$BUILD_DIR"

echo "core safety tests passed"
