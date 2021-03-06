/**
  @module lua-rados
*/
#include <errno.h>
#include <stdlib.h>

#define LUA_COMPAT_MODULE

#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>

#include <rados/librados.h>

#define LRAD_TRADOS_T "Rados.RadosT"
#define LRAD_TIOCTX_T "Rados.IoctxT"
#define LRAD_TCOMPLETION_T "Rados.CompletionT"
#define LRAD_BUFFER_T "Rados.Buffer"

static int active_completions = 0;

static char reg_key_rados_refs;

typedef enum
{
  CLUSTER_CONFIGURING,
  CLUSTER_CONNECTED,
  CLUSTER_SHUTDOWN
} cluster_state_t;

typedef struct lua_rados_t
{
  rados_t cluster;
  cluster_state_t state;
} lua_rados_t;

typedef enum
{
  IOCTX_OPEN,
  IOCTX_CLOSED
} ioctx_state_t;

typedef struct lua_ioctx_t
{
  rados_ioctx_t io;
  ioctx_state_t state;
} lua_ioctx_t;

typedef enum
{
  COMPLETION_STAT,
  COMPLETION_READ
} completion_state_t;

typedef struct lua_completion_t
{
  rados_completion_t completion;
  completion_state_t state;
  struct
  {
    uint64_t size;
    union
    {
      time_t mtime;
      char *buf;
    };
  };
} lua_completion_t;

/* Get rados object at stack index.  */

static inline lua_rados_t *
lua_rados_checkcluster_1 (lua_State *lstate, int pos)
{
  return (lua_rados_t *) luaL_checkudata (lstate, pos, LRAD_TRADOS_T);
}

/* Get non-shutdown rados object at stack index. Error if had shutdown.  */

static inline lua_rados_t *
lua_rados_checkcluster (lua_State *lstate, int pos)
{
  lua_rados_t *rados;

  rados = lua_rados_checkcluster_1 (lstate, pos);
  if (rados->state == CLUSTER_SHUTDOWN)
    luaL_argerror (lstate, pos, "cannot reuse shutdown rados handle");

  return rados;
}

/* Get connected rados object at stack index. Error if not connected.  */

static inline lua_rados_t *
lua_rados_checkcluster_conn (lua_State *lstate, int pos)
{
  lua_rados_t *rados;

  rados = lua_rados_checkcluster (lstate, pos);
  if (rados->state != CLUSTER_CONNECTED)
    luaL_argerror (lstate, pos, "not connected to cluster");

  return rados;
}

/* Get opened rados ioctx object at stack index. Error if had closed.  */

static inline lua_ioctx_t *
lua_rados_checkioctx (lua_State *lstate, int pos)
{
  lua_ioctx_t *ioctx;

  ioctx = (lua_ioctx_t *) luaL_checkudata (lstate, pos, LRAD_TIOCTX_T);
  if (ioctx->state != IOCTX_OPEN)
    luaL_argerror (lstate, pos, "cannot reuse closed ioctx handle");

  return ioctx;
}

/* Get rados completion object at stack index.  */

static inline lua_completion_t *
lua_rados_checkcompletion (lua_State *lstate, int pos)
{
  return (lua_completion_t *) luaL_checkudata (lstate, pos, LRAD_TCOMPLETION_T);
}

/* Push nil-error protocol values.  */

static int
lua_rados_pusherror (lua_State *lstate, int ret)
{
  lua_pushnil (lstate);
  lua_pushfstring (lstate, "%s", strerror (-ret));
  lua_pushinteger (lstate, ret);

  return 3;
}

/* Push return value or error if not ok.  */

static int
lua_rados_pushresult (lua_State *lstate, int ok, int ret)
{
  if (!ok)
    return lua_rados_pusherror (lstate, ret);

  lua_pushinteger (lstate, ret);

  return 1;
}

/* Garbage collect buffers in the heap.  */

static int
lua_rados_buffer_gc (lua_State *lstate)
{
  char **pbuf;

  pbuf = (char **) luaL_checkudata (lstate, 1, LRAD_BUFFER_T);
  if (pbuf && *pbuf)
    {
      free (*pbuf);
      *pbuf = NULL;
    }

  return 0;
}

/* Allocate a new char * in the heap.  */

static char *
lua_rados_newbuffer (lua_State *lstate, int size)
{
  char *buf;
  char **pbuf;

  if (size < 0)
    return NULL;

  if (size == 0)
    size = 1;

  buf = (char *) malloc (size);
  if (buf)
    memset (buf, 0, size);

  pbuf = (char **) lua_newuserdata (lstate, sizeof (*pbuf));
  *pbuf = buf;

  luaL_getmetatable (lstate, LRAD_BUFFER_T);
  lua_setmetatable (lstate, -2);

  return buf;
}

/**
  @type Rados
 */

/**
  Get the version of librados.
  @function version
  @return major version
  @return minor version
  @return extra version
  @usage major, minor, extra = rados.version()
 */
static int
lua_rados_version (lua_State *lstate)
{
  int major, minor, extra;

  rados_version (&major, &minor, &extra);

  lua_pushinteger (lstate, major);
  lua_pushinteger (lstate, minor);
  lua_pushinteger (lstate, extra);

  return 3;
}

/**
  Get the number of open completions.
  @function open_completions
  @return number of open completions.
  @usage num = rados.open_completions()
 */
static int
lua_rados_open_completions (lua_State *lstate)
{
  lua_pushinteger (lstate, __sync_add_and_fetch (&active_completions, 0));
  return 1;
}

/**
  Create handle for communicating with RADOS cluster.
  @function create
  @string id the user to connect as (optional, or nil)
  @return rados handle object on success, nil otherwise
  @return error message and retval if failed
  @usage cluster = rados.create()
  @usage cluster = rados.create('admin')
*/
static int
lua_rados_create (lua_State *lstate)
{
  lua_rados_t *rados;
  const char *id;
  int ret;

  id = luaL_optstring (lstate, 1, NULL);

  rados = (lua_rados_t *) lua_newuserdata (lstate, sizeof (*rados));
  rados->state = CLUSTER_CONFIGURING;

  luaL_getmetatable (lstate, LRAD_TRADOS_T);
  lua_setmetatable (lstate, -2);

  ret = rados_create (&rados->cluster, id);
  if (ret)
    return lua_rados_pusherror (lstate, ret);

  /* return the userdata */
  return 1;
}

/**
  @type Cluster
 */

/**
  Configure the rados handle using a Ceph config file.
  @function conf_read_file
  @string file path to configuration file (optional, or nil)
  @return 0 on success, nil otherwise
  @return error message and retval if failed
  @usage cluster:conf_read_file()
  @usage cluster:conf_read_file('/path/to/ceph.conf')
 */
static int
lua_rados_conf_read_file (lua_State *lstate)
{
  lua_rados_t *rados;
  const char *path;
  int ret;

  rados = lua_rados_checkcluster (lstate, 1);
  path = luaL_optstring (lstate, 2, NULL);

  ret = rados_conf_read_file (rados->cluster, path);

  return lua_rados_pushresult (lstate, (ret == 0), ret);
}

/**
  Connect to the cluster.
  @function connect
  @return 0 on success, nil otherwise
  @return error message and retval if failed
  @usage cluster:connect()
  @usage status, errstr, ret = cluster:connect()
 */
static int
lua_rados_connect (lua_State *lstate)
{
  lua_rados_t *rados;
  int ret;

  rados = lua_rados_checkcluster (lstate, 1);
  if (rados->state == CLUSTER_CONNECTED)
    luaL_argerror (lstate, 1, "already connected to cluster");

  ret = rados_connect (rados->cluster);
  if (!ret)
    rados->state = CLUSTER_CONNECTED;

  return lua_rados_pushresult (lstate, (ret == 0), ret);
}

/**
  Return true if connected to the cluster.
  @function is_connected
  @return true if connected to cluster, false otherwise
  @usage cluster:is_connected()
  @usage ret = cluster:connected()
 */
static int
lua_rados_is_connected (lua_State *lstate)
{
  lua_rados_t *rados;

  rados = lua_rados_checkcluster_1 (lstate, 1);
  lua_pushboolean (lstate, (rados->state == CLUSTER_CONNECTED));

  return 1;
}

/**
  Disconnect from the cluster.
  @function shutdown
  @usage cluster:shutdown()
 */
static int
lua_rados_shutdown (lua_State *lstate)
{
  lua_rados_t *rados;

  rados = lua_rados_checkcluster_conn (lstate, 1);
  if (rados->state != CLUSTER_SHUTDOWN)
    {
      rados_shutdown (rados->cluster);
      rados->cluster = NULL;
    }

  rados->state = CLUSTER_SHUTDOWN;

  return 0;
}

/**
  Create an I/O context.
  @function open_ioctx
  @string name of pool
  @return I/O context object or nil on failure
  @return errstr, ret if failed
  @usage ioctx = cluster:open_ioctx('my_pool')
 */
static int
lua_rados_open_ioctx (lua_State *lstate)
{
  lua_rados_t *rados;
  const char *pool_name;
  lua_ioctx_t *ioctx;
  int ret;

  rados = lua_rados_checkcluster_conn (lstate, 1);
  pool_name = luaL_checkstring (lstate, 2);

  ioctx = (lua_ioctx_t *) lua_newuserdata (lstate, sizeof (*ioctx));
  ioctx->state = IOCTX_OPEN;

  luaL_getmetatable (lstate, LRAD_TIOCTX_T);
  lua_setmetatable (lstate, -2);

  ret = rados_ioctx_create (rados->cluster, pool_name, &ioctx->io);
  if (ret)
    return lua_rados_pusherror (lstate, ret);

  /* record rados_ioctx_t -> rados_t reference in weak key table */
  lua_pushlightuserdata (lstate, &reg_key_rados_refs);
  lua_gettable (lstate, LUA_REGISTRYINDEX);

  if (lua_isnil (lstate, -1) || lua_type (lstate, -1) != LUA_TTABLE)
    return lua_rados_pusherror (lstate, 1);

  lua_pushvalue (lstate, -2); /* key = ioctx */
  lua_pushvalue (lstate, 1);  /* value = cluster */
  lua_settable (lstate, -3);
  lua_pop (lstate, 1);

  /* return the userdata */
  return 1;
}

/**
  Register us as a daemon providing a particular service.
 */

static int
lua_rados_service_register (lua_State *lstate)
{
  lua_rados_t *rados;
  const char *service;
  const char *daemon;
  int ret;

  rados = lua_rados_checkcluster_conn (lstate, 1);
  service = luaL_checkstring (lstate, 2);
  daemon = luaL_checkstring (lstate, 3);

  ret = rados_service_register (rados->cluster, service, daemon, "");
  if (ret)
    return lua_rados_pusherror (lstate, ret);

  return 0;
}

/* Garbage collect the rados object, and shutdown only if connected.  */

static int
lua_rados_cluster_gc (lua_State *lstate)
{
  lua_rados_t *rados;

  rados = lua_rados_checkcluster_1 (lstate, 1);
  if (rados->state == CLUSTER_CONNECTED)
    rados_shutdown (rados->cluster);
  else if (rados->cluster != NULL)
    {
      /* Call to rados_connect() was made and failed.  */
      free (rados->cluster);
      rados->cluster = NULL;
    }

  rados->state = CLUSTER_SHUTDOWN;

  return 0;
}

/**
  @type Ioctx
 */

/**
  Close the I/O context.
  @function close
  @usage ioctx:close()
 */
static int
lua_rados_ioctx_close (lua_State *lstate)
{
  lua_ioctx_t *ioctx;

  ioctx = lua_rados_checkioctx (lstate, 1);
  if (ioctx->state == IOCTX_OPEN)
    rados_ioctx_destroy (ioctx->io);

  ioctx->state = IOCTX_CLOSED;

  return 0;
}

/**
  Get object stat info (size/mtime)
  @function stat
  @string loc locator key
  @string oid object name
  @return len, mtime, or nil on failure
  @return errstr and retval if failed
  @usage size, mtime = ioctx:stat(nil, 'obj3')
 */
static int
lua_rados_ioctx_stat (lua_State *lstate)
{
  lua_ioctx_t *ioctx;
  const char *oid, *loc;
  uint64_t size;
  time_t mtime;
  int ret;

  ioctx = lua_rados_checkioctx (lstate, 1);
  loc = luaL_optstring (lstate, 2, NULL);
  oid = luaL_checkstring (lstate, 3);

  rados_ioctx_locator_set_key (ioctx->io, loc);
  ret = rados_stat (ioctx->io, oid, &size, &mtime);
  rados_ioctx_locator_set_key (ioctx->io, NULL);
  if (ret)
    return lua_rados_pusherror (lstate, ret);

  lua_pushinteger (lstate, size);
  lua_pushinteger (lstate, mtime);

  return 2;
}

/**
  Read data from an object.
  @function read
  @string loc locator key
  @string oid object name
  @int length number of bytes read
  @int offset offset in object from which to read
  @return string with at most `length` bytes, or nil on error
  @return errstr and retval if failed
  @usage data = ioctx:read(nil, 'obj3', 1000, 0)
 */
static int
lua_rados_ioctx_read (lua_State *lstate)
{
  lua_ioctx_t *ioctx;
  const char *oid, *loc;
  size_t size;
  uint64_t off;
  char *buf;
  int ret;

  ioctx = lua_rados_checkioctx (lstate, 1);
  loc = luaL_optstring (lstate, 2, NULL);
  oid = luaL_checkstring (lstate, 3);
  size = luaL_checkinteger (lstate, 4);
  off = luaL_checkinteger (lstate, 5);

  buf = lua_rados_newbuffer (lstate, size);
  if (!buf)
    return lua_rados_pusherror (lstate, -ENOMEM);

  rados_ioctx_locator_set_key (ioctx->io, loc);
  ret = rados_read (ioctx->io, oid, buf, size, off);
  rados_ioctx_locator_set_key (ioctx->io, NULL);
  if (ret < 0)
    return lua_rados_pusherror (lstate, ret);

  lua_pushlstring (lstate, buf, ret);

  return 1;
}

/**
  Asynchronously get object stat info (size/mtime)
  @function aio_stat
  @string loc locator key
  @string oid object name
  @return completion object
  @usage completion = ioctx:aio_stat(nil, 'obj3')
 */
static int
lua_rados_ioctx_aio_stat (lua_State *lstate)
{
  lua_ioctx_t *ioctx;
  const char *oid, *loc;
  lua_completion_t *comp;
  int ret;

  ioctx = lua_rados_checkioctx (lstate, 1);
  loc = luaL_optstring (lstate, 2, NULL);
  oid = luaL_checkstring (lstate, 3);

  comp = (lua_completion_t *) lua_newuserdata (lstate, sizeof (*comp));
  comp->completion = NULL;
  comp->state = COMPLETION_STAT;

  luaL_getmetatable (lstate, LRAD_TCOMPLETION_T);
  lua_setmetatable (lstate, -2);

  ret = rados_aio_create_completion (NULL, NULL, NULL, &comp->completion);
  if (ret)
    return lua_rados_pusherror (lstate, ret);

  __sync_add_and_fetch (&active_completions, 1);
  rados_ioctx_locator_set_key (ioctx->io, loc);
  ret = rados_aio_stat (ioctx->io, oid, comp->completion,
			&comp->size, &comp->mtime);
  rados_ioctx_locator_set_key (ioctx->io, NULL);
  if (ret)
    return lua_rados_pusherror (lstate, ret);

  /* return the userdata */
  return 1;
}

/**
  Asynchronously read data from an object.
  @function aio_read
  @string loc locator key
  @string oid object name
  @int length number of bytes read
  @int offset offset in object from which to read
  @return completion object
  @usage completion = ioctx:aio_read(nil, 'obj3', 1000, 0)
 */
static int
lua_rados_ioctx_aio_read (lua_State *lstate)
{
  lua_ioctx_t *ioctx;
  const char *oid, *loc;
  size_t size;
  uint64_t off;
  lua_completion_t *comp;
  int ret;

  ioctx = lua_rados_checkioctx (lstate, 1);
  loc = luaL_optstring (lstate, 2, NULL);
  oid = luaL_checkstring (lstate, 3);
  size = luaL_checkinteger (lstate, 4);
  off = luaL_checkinteger (lstate, 5);

  comp = (lua_completion_t *) lua_newuserdata (lstate, sizeof (*comp));
  comp->completion = NULL;
  comp->state = COMPLETION_READ;
  comp->size = size;
  comp->buf = (char *) malloc (comp->size);
  if (!comp->buf)
    return lua_rados_pusherror (lstate, -ENOMEM);

  luaL_getmetatable (lstate, LRAD_TCOMPLETION_T);
  lua_setmetatable (lstate, -2);

  ret = rados_aio_create_completion (NULL, NULL, NULL, &comp->completion);
  if (ret)
    return lua_rados_pusherror (lstate, ret);

  __sync_add_and_fetch (&active_completions, 1);
  rados_ioctx_locator_set_key (ioctx->io, loc);
  ret = rados_aio_read (ioctx->io, oid, comp->completion,
			comp->buf, comp->size, off);
  rados_ioctx_locator_set_key (ioctx->io, NULL);
  if (ret)
    return lua_rados_pusherror (lstate, ret);

  /* return the userdata */
  return 1;
}

/**
  Has an asynchronous operation completed?
  @function is_complete
  @return whether the operation is complete
  @usage complete = completion:is_complete()
 */
static int
lua_rados_completion_complete (lua_State *lstate)
{
  lua_completion_t *comp;
  int ret;

  comp = lua_rados_checkcompletion (lstate, 1);

  ret = rados_aio_is_complete (comp->completion);

  lua_pushinteger (lstate, ret);

  return 1;
}

/**
  Wait for an asynchronous operation to complete
  @function wait_for_complete
  @usage completion:wait_for_complete()
 */
static int
lua_rados_completion_wait_for_complete (lua_State *lstate)
{
  lua_completion_t *comp;

  comp = lua_rados_checkcompletion (lstate, 1);

  rados_aio_wait_for_complete (comp->completion);

  return 0;
}

/**
  Get the return value of an asynchronous operation
  @function get_return_value
  @return len, mtime, or nil on failure
  @return errstr and retval if failed
  @usage size, mtime = completion:get_return_value()
 */
static int
lua_rados_completion_get_return_value (lua_State *lstate)
{
  lua_completion_t *comp;
  int ret;

  comp = lua_rados_checkcompletion (lstate, 1);

  ret = rados_aio_get_return_value (comp->completion);
  if (ret < 0)
    return lua_rados_pusherror (lstate, ret);

  if (comp->state == COMPLETION_STAT)
    {
      lua_pushinteger (lstate, comp->size);
      lua_pushinteger (lstate, comp->mtime);
      return 2;
    }
  else if (comp->state == COMPLETION_READ)
    {
      lua_pushlstring (lstate, comp->buf, comp->size);
      return 1;
    }

  /* Unhandled completion request.  */
  return lua_rados_pusherror (lstate, -EINVAL);
}

/* Garbage collect the rados completion.  */

static int
lua_rados_completion_gc (lua_State *lstate)
{
  lua_completion_t *comp;

  comp = lua_rados_checkcompletion (lstate, 1);

  if (comp->completion != NULL)
    {
      rados_aio_release (comp->completion);
      comp->completion = NULL;
      __sync_sub_and_fetch (&active_completions, 1);
    }

  if (comp->state == COMPLETION_READ && comp->buf != NULL)
    {
      free (comp->buf);
      comp->buf = NULL;
    }

  return 0;
}


static const luaL_Reg clusterlib_m[] =
{
  { "conf_read_file", lua_rados_conf_read_file },
  { "connect", lua_rados_connect },
  { "is_connected", lua_rados_is_connected },
  { "shutdown", lua_rados_shutdown },
  { "open_ioctx", lua_rados_open_ioctx },
  { "register", lua_rados_service_register },
  { "__gc", lua_rados_cluster_gc },
  { NULL, NULL }
};

static const luaL_Reg ioctxlib_m[] =
{
  { "close", lua_rados_ioctx_close },
  { "read", lua_rados_ioctx_read },
  { "stat", lua_rados_ioctx_stat },
  { "aio_stat", lua_rados_ioctx_aio_stat },
  { "aio_read", lua_rados_ioctx_aio_read },
  { NULL, NULL }
};

static const luaL_Reg completionlib_m[] =
{
  { "is_complete", lua_rados_completion_complete },
  { "wait_for_complete", lua_rados_completion_wait_for_complete },
  { "get_return_value", lua_rados_completion_get_return_value },
  { "__gc", lua_rados_completion_gc },
  { NULL, NULL }
};

static const luaL_Reg radoslib_f[] =
{
  { "version", lua_rados_version },
  { "open_completions", lua_rados_open_completions },
  { "create", lua_rados_create },
  { NULL, NULL }
};

LUALIB_API int
luaopen_rados (lua_State *lstate)
{
  /* setup rados_t userdata type */
  luaL_newmetatable (lstate, LRAD_TRADOS_T);
  lua_pushvalue (lstate, -1);
  lua_setfield (lstate, -2, "__index");
  luaL_register (lstate, NULL, clusterlib_m);
  lua_pop (lstate, 1);

  /* setup rados_ioctx_t userdata type */
  luaL_newmetatable (lstate, LRAD_TIOCTX_T);
  lua_pushvalue (lstate, -1);
  lua_setfield (lstate, -2, "__index");
  luaL_register (lstate, NULL, ioctxlib_m);
  lua_pop (lstate, 1);

  /* setup rados_completion_t userdata type */
  luaL_newmetatable (lstate, LRAD_TCOMPLETION_T);
  lua_pushvalue (lstate, -1);
  lua_setfield (lstate, -2, "__index");
  luaL_register (lstate, NULL, completionlib_m);
  lua_pop (lstate, 1);

  /* setup buffer userdata type */
  luaL_newmetatable (lstate, LRAD_BUFFER_T);
  lua_pushcfunction (lstate, lua_rados_buffer_gc);
  lua_setfield (lstate, -2, "__gc");
  lua_pop (lstate, 1);

  /* weak table to protect rados_ioctx_t -> rados_t refs */
  lua_pushlightuserdata (lstate, &reg_key_rados_refs);
  lua_newtable (lstate);
  lua_pushstring (lstate, "k");
  lua_setfield (lstate, -2, "__mode");
  lua_pushvalue (lstate, -1);
  lua_setmetatable (lstate, -2);
  lua_settable (lstate, LUA_REGISTRYINDEX);

  luaL_register (lstate, "rados", radoslib_f);

  return 0;
}
