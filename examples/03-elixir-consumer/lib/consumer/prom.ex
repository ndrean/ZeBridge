defmodule Prom do
  def metrics do
    Req.get!("http://127.0.0.1:8080/metrics").body
  end

  def status do
    Req.get!("http://127.0.0.1:8080/status").body
  end
end
