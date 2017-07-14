lua-rados
=========

lua-rados is Lua bindings for the RADOS object store client library.

This project is a fork of [lua-rados], but with all features removed apart
from those to allow interaction from a read-only client.

Please report bugs and make feature requests using the github tracker.

Example
-------

### NGINX config - Connecting to RADOS
```
init_by_lua_block
{
  rados = require("rados")
}

init_worker_by_lua_block
{
  -- Create a persistent connection to the object store
  cluster = rados.create()
  cluster:conf_read_file()
  cluster:connect()
  if cluster:is_connected() then
    ioctx = cluster:open_ioctx("rbd")
  end
}
```

### NGINX config - Simple.
```
server
{
  location /rbd
  {
    content_by_lua_block
    {
      local key = string.match(ngx.var.uri, "/rbd/(.+)")
      if ioctx and key then
        local size = ioctx:stat(nil, key)
        if size then
          ngx.print(ioctx:read(nil, key, size, 0))
        end
      end
    }
  }
}
```

### NGINX config - Complex.
```
server
{
  location /rbd
  {
    content_by_lua_block
    {
      local loc, key = string.match(ngx.var.uri, "/rbd/(.+)/(.+)")
      -- We are connected and have matched a key
      if ioctx and loc and key then
        local size, mtime = ioctx:stat(loc, key)
        -- Key exists in object store
        if size and mtime then
          -- We have an object to send, check client headers
          local ims = ngx.var.http_if_modified_since
          local range = ngx.var.http_range
          local data = nil

          -- Set response headers
          local hmtime = ngx.http_time(tonumber(mtime))
          ngx.header["Last-Modified"] = hmtime

          if ims == hmtime then
            -- Not modified since last access
            ngx.status = 304
            ngx.send_headers()
          else
            local from, to = string.match(range or "", "bytes=([0-9]+)-([0-9]+)")
            -- Send all or part of the object to the client
            if from and to then
              local offset = tonumber(from)
              local rsize = tonumber(to) - offset + 1
              if rsize < 1 then
                -- Requested range not satisfiable
                ngx.header["Content-Range"] = "bytes */" .. size
                ngx.status = 416
              else
                data = ioctx:read(loc, key, offset, rsize)
                -- Return partial content to client
                if data then
                  ngx.header["Content-Range"] = "bytes " .. from .. "-" .. to .. "/" .. size
                  ngx.status = 206
                  size = rsize
                end
              end
            else
              data = ioctx:read(loc, key, size, 0)
            end

            -- Fetched data without incident
            if data then
              ngx.header["Content-Length"] = size
              ngx.print(data)
            end
          end
        end
      end
      if ngx.status == 0 then
        ngx.status = 404
      end
      ngx.exit(ngx.status)
    }
  }
}
```

[lua-rados]: https://github.com/noahdesu/lua-rados "noahdesu/lua-rados"
