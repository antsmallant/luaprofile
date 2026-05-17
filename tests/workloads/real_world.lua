local M = {}

local function update_position(entity, dt)
    entity.x = entity.x + entity.vx * dt
    entity.y = entity.y + entity.vy * dt
    return entity.x + entity.y
end

local function update_ai(entity, tick)
    local state = entity.state
    if tick % 7 == 0 then
        state.intent = (state.intent + entity.id) % 11
    end
    state.score = state.score + (state.intent * 3 + tick) % 17
    return state.score
end

local function tick_loop_step(entities, tick)
    local total = 0
    local dt = 0.016
    for i = 1, #entities do
        local entity = entities[i]
        total = total + update_position(entity, dt)
        total = total + update_ai(entity, tick)
    end
    return total
end

local function make_entities(count)
    local entities = {}
    for i = 1, count do
        entities[i] = {
            id = i,
            x = i % 31,
            y = i % 17,
            vx = (i % 5) - 2,
            vy = (i % 7) - 3,
            hp = 100 + (i % 23),
            state = {
                intent = i % 9,
                score = 0,
            },
            components = {
                transform = { dirty = false },
                combat = { cooldown = i % 13 },
                inventory = { slots = { i, i + 1, i + 2 } },
            },
        }
    end
    return entities
end

local function workload_tick_loop(scale)
    local entities = make_entities(scale.entities)
    local total = 0
    for tick = 1, scale.ticks do
        total = total + tick_loop_step(entities, tick)
    end
    return total
end

local function component_transform(entity, tick)
    entity.components.transform.dirty = tick % 2 == 0
    entity.x = entity.x + (tick % 3) - 1
    entity.y = entity.y + (tick % 5) - 2
    return entity.x - entity.y
end

local function component_combat(entity, tick)
    local combat = entity.components.combat
    combat.cooldown = (combat.cooldown + tick + entity.id) % 17
    entity.hp = entity.hp - (combat.cooldown % 3)
    return entity.hp
end

local function component_inventory(entity)
    local sum = 0
    local slots = entity.components.inventory.slots
    for i = 1, #slots do
        slots[i] = slots[i] + entity.id % 4
        sum = sum + slots[i]
    end
    return sum
end

local function entity_component_update(scale)
    local entities = make_entities(scale.entities)
    local total = 0
    for tick = 1, scale.ticks do
        for i = 1, #entities do
            local entity = entities[i]
            total = total + component_transform(entity, tick)
            total = total + component_combat(entity, tick)
            total = total + component_inventory(entity)
        end
    end
    return total
end

local function table_hot_path_lookup(buckets, id)
    local bucket = buckets[(id % #buckets) + 1]
    local item = bucket[id]
    if item then
        item.hits = item.hits + 1
        return item.value + item.hits
    end
    bucket[id] = { value = id % 97, hits = 1 }
    return bucket[id].value
end

local function table_hot_path(scale)
    local buckets = {}
    for i = 1, scale.buckets do
        buckets[i] = {}
    end
    local total = 0
    for i = 1, scale.operations do
        total = total + table_hot_path_lookup(buckets, (i * 37) % scale.keyspace)
    end
    return total
end

local function scheduler_task(task)
    local total = task.seed
    for i = 1, task.steps do
        total = total + (task.id * i) % 13
        if i % task.yield_every == 0 then
            coroutine.yield(total)
        end
    end
    return total
end

local function coroutine_scheduler(scale)
    local coroutines = {}
    for i = 1, scale.coroutines do
        coroutines[i] = coroutine.create(function()
            return scheduler_task({
                id = i,
                seed = i * 3,
                steps = scale.steps,
                yield_every = scale.yield_every,
            })
        end)
    end

    local total = 0
    local alive = true
    while alive do
        alive = false
        for i = 1, #coroutines do
            local co = coroutines[i]
            if coroutine.status(co) ~= "dead" then
                alive = true
                local ok, value = coroutine.resume(co)
                assert(ok, value)
                if value then
                    total = total + value
                end
            end
        end
    end
    return total
end

local function encode_value(value, out)
    local t = type(value)
    if t == "number" or t == "boolean" then
        out[#out + 1] = tostring(value)
    elseif t == "string" then
        out[#out + 1] = value
    elseif t == "table" then
        out[#out + 1] = "{"
        for k, v in pairs(value) do
            encode_value(k, out)
            out[#out + 1] = ":"
            encode_value(v, out)
            out[#out + 1] = ";"
        end
        out[#out + 1] = "}"
    end
end

local function make_packet(i)
    return {
        id = i,
        op = "move",
        position = { x = i * 2, y = i * 3, z = i % 5 },
        stats = { hp = 100 + i % 29, mp = 40 + i % 11 },
        tags = { "entity", "zone", tostring(i % 17) },
    }
end

local function serialization_like(scale)
    local total = 0
    for i = 1, scale.packets do
        local out = {}
        encode_value(make_packet(i), out)
        total = total + #table.concat(out, "|")
    end
    return total
end

local function churn_alloc_batch(round, width)
    local batch = {}
    for i = 1, width do
        batch[i] = {
            id = round * width + i,
            payload = string.rep("x", (i % 23) + 8),
            values = { i, i * 2, i * 3 },
        }
    end
    return batch
end

local function memory_churn(scale)
    local retained = {}
    local total = 0
    for round = 1, scale.rounds do
        local batch = churn_alloc_batch(round, scale.width)
        total = total + #batch
        if round % 3 == 0 then
            retained[#retained + 1] = batch[1]
            batch = nil
            collectgarbage("collect")
        end
    end
    return total + #retained
end

M.profiles = {
    smoke = {
        tick_loop = { ticks = 8, entities = 40 },
        entity_component_update = { ticks = 5, entities = 35 },
        table_hot_path = { operations = 900, buckets = 16, keyspace = 211 },
        coroutine_scheduler = { coroutines = 12, steps = 30, yield_every = 5 },
        serialization_like = { packets = 120 },
        memory_churn = { rounds = 12, width = 40 },
    },
    extended = {
        tick_loop = { ticks = 40, entities = 180 },
        entity_component_update = { ticks = 25, entities = 160 },
        table_hot_path = { operations = 8000, buckets = 64, keyspace = 1201 },
        coroutine_scheduler = { coroutines = 80, steps = 100, yield_every = 7 },
        serialization_like = { packets = 1200 },
        memory_churn = { rounds = 80, width = 180 },
    },
}

M.workloads = {
    {
        name = "tick_loop",
        description = "Server tick loop over entities with position and AI updates.",
        expected_bottleneck = {
            pattern = "tick_loop_step",
            metric = "cpu_cost_raw(ns)",
            top_n = 8,
            reason = "The per-tick entity loop dominates repeated update work.",
        },
        run = workload_tick_loop,
    },
    {
        name = "entity_component_update",
        description = "Entity/component update across transform, combat, and inventory components.",
        expected_bottleneck = {
            pattern = "component_inventory",
            metric = "call_count",
            top_n = 12,
            reason = "Inventory component work is called once per entity per tick and has inner slot iteration.",
        },
        run = entity_component_update,
    },
    {
        name = "table_hot_path",
        description = "Hash table lookup/update hot path with repeated bucket access.",
        expected_bottleneck = {
            pattern = "table_hot_path_lookup",
            metric = "call_count",
            top_n = 8,
            reason = "Lookup function is called for every operation.",
        },
        run = table_hot_path,
    },
    {
        name = "coroutine_scheduler",
        description = "Cooperative scheduler resuming many yielding coroutine tasks.",
        expected_bottleneck = {
            pattern = "yield",
            metric = "call_count",
            top_n = 12,
            reason = "The scheduler workload is dominated by repeated coroutine yield/resume operations.",
        },
        run = coroutine_scheduler,
    },
    {
        name = "serialization_like",
        description = "Serialization-like recursive traversal and string assembly.",
        expected_bottleneck = {
            pattern = "encode_value",
            metric = "call_count",
            top_n = 8,
            reason = "Recursive encoder visits every packet field.",
        },
        run = serialization_like,
    },
    {
        name = "memory_churn",
        description = "Allocation churn with short-lived batches, retained objects, and forced GC.",
        expected_bottleneck = {
            pattern = "churn_alloc_batch",
            metric = "alloc_bytes",
            top_n = 8,
            reason = "Batch allocation creates the majority of short-lived objects.",
        },
        run = memory_churn,
    },
}

return M
