defmodule GRPC.Integration.StubMintTest do
  use GRPC.Integration.TestCase

  defmodule HelloServer do
    use GRPC.Server, service: Helloworld.Greeter.Service

    def say_hello(req, _stream) do
      Helloworld.HelloReply.new(message: "Hello, #{req.name}")
    end
  end

  defmodule SlowServer do
    use GRPC.Server, service: Helloworld.Greeter.Service

    def say_hello(_req, _stream) do
      Process.sleep(1000)
    end
  end

  def port_for(pid) do
    Port.list()
    |> Enum.find(fn port ->
      case Port.info(port, :links) do
        {:links, links} ->
          pid in links

        _ ->
          false
      end
    end)
  end

  test "body larger than 2^14 works" do
    run_server(HelloServer, fn port ->
      {:ok, channel} = GRPC.Stub.connect("localhost:#{port}", interceptors: [GRPC.Logger.Client], adapter: GRPC.Adapter.Mint)
      name = String.duplicate("a", round(:math.pow(2, 2)))
      req = Helloworld.HelloRequest.new(name: name)
      {:ok, reply} = channel |> Helloworld.Greeter.Stub.say_hello(req)
      assert reply.message == "Hello, #{name}"
    end)
  end

end
