local dbg=debug
local mt = {}
setmetatable(_G, mt)
mt.__newindex = function (t, n, v)
    if dbg.getinfo(2) then
        local w = dbg.getinfo(2, "S").what
        if w ~= "main" and w ~= "C" then
            error("Script attempted to create global variable '" .. tostring(n) .. "'", 2)
        end
    end
    rawset(t, n, v)
end
mt.__index = function (t, n)
    if dbg.getinfo(2) and dbg.getinfo(2, "S").what ~= "C" then
        error("Script attempted to access nonexistent global variable '" .. tostring(n) .. "'", 2);
    end
    return rawget(t, n)
end
debug = nil
