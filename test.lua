local rados = require "rados"

describe("version", function()
  local major, minor, extra

  setup(function()
    major, minor, extra = rados.version()
  end)

  it("returns three numeric values", function()
    assert.is.number(major)
    assert.is.number(minor)
    assert.is.number(extra)
  end)

  it("returns a non-zero version part", function()
    assert.is_true(major > 0 or minor > 0 or extra > 0)
  end)
end)

describe("create", function()
  it("excepts no id paraemter", function()
    cluster = rados.create()
    assert.is_userdata(cluster)
  end)

  it("excepts nil id paraemter", function()
    cluster = rados.create(nil)
    assert.is_userdata(cluster)
  end)

  it("excepts string id paraemter", function()
    cluster = rados.create('admin')
    assert.is_userdata(cluster)
  end)

  it("throws error on numeric parameter", function()
    assert.error(function() rados.create(0) end)
  end)

  it("throws error on non string id paraemter", function()
    assert.error(function() rados.create({}) end)
  end)
end)

describe("cluster object", function()
  local cluster_new, cluster_conn, cluster_down

  before_each(function()
    cluster_new = rados.create()

    cluster_conn = rados.create()
    cluster_conn:conf_read_file()
    cluster_conn:connect()

    cluster_down = rados.create()
    cluster_down:conf_read_file()
    cluster_down:connect()
    cluster_down:shutdown()
  end)

  describe("connect method", function()
    it("throws error if connected", function()
      assert.error(function() cluster_conn:connect() end)
    end)

    it("throws error if shutdown", function()
      assert.error(function() cluster_down:connect() end)
    end)
  end)

  describe("shutdown method", function()
    it("succeeds if connected", function()
      cluster_conn:shutdown()
    end)

    it("throws error if not connected", function()
      assert.error(function() cluster_new:shutdown() end)
    end)

    it("throws error if shutdown", function()
      assert.error(function() cluster_down:shutdown() end)
    end)
  end)

  describe("open_ioctx method", function()
    it("returns a userdata object", function()
      assert.is_userdata(cluster_conn:open_ioctx('rbd'))
    end)

    it("throws error without pool name", function()
      assert.error(function() cluster_conn:open_ioctx() end)
    end)

    it("throws error with non-string param", function()
      assert.error(function() cluster_conn:open_ioctx({}) end)
    end)

    it("returns nil for non-existent pools", function()
      assert.is_nil(cluster_conn:open_ioctx(23423432))
      assert.is_nil(cluster_conn:open_ioctx('asdlfk'))
    end)

    it("throws error if not connected", function()
      assert.error(function() cluster_new:open_ioctx('rbd') end)
    end)

    it("throws error if shutdown", function()
      assert.error(function() cluster_down:open_ioctx('rbd') end)
    end)
  end)
end)
