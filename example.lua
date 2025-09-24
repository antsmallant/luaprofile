root="./"
package.path = package.path .. ";" .. root .. "?.lua"
package.cpath = package.cpath .. ";" .. root .. "?.so"

local profile = require "profile"
local json = require "json"

local function test3()
    local s = 0
    for i = 1, 10000 do
        s = s + i
    end
end

local function test2()
    for i = 1, 10 do
        test3()
    end    
end

local function test1()
    profile.start()
    tonumber("123")    
    print("111")
    tonumber("234")
    print("222")
    test2()
    local result = profile.stop()
    local strResult = json.encode(result)
    print(strResult)
end

test1()