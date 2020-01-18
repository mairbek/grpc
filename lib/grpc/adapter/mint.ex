defmodule GRPC.Adapter.Mint do
  alias GRPC.Adapter.Mint.ConnectionProcess

  require Logger

  # 1. Create GenServer
  # 2. Plumb adapter in stub and write a test
  # 3. Implement connect and disconnect
  # 4. Implement non-streaming RPC
  # 4.1 Implement :gun::await alternative
  # !!!! big win here
  # 5. Implement streaming RPC

  # connect to the backend using mint
  def connect(%{host: host, port: port} = channel, opts) do
    {:ok, conn_pid} = ConnectionProcess.start_link({:http, host, port, []})
    {:ok, Map.put(channel, :adapter_payload, %{conn_pid: conn_pid})}
  end

  # disconnect
  def disconnect(%{} = channel) do
  end

  # send request to the server
  def send_request(%{channel: %{adapter_payload: %{conn_pid: conn_pid}}, path: path} = stream, message, opts) do
    IO.puts("Send request")

    headers = GRPC.Transport.HTTP2.client_headers_without_reserved(stream, opts)
    {:ok, data, _} = GRPC.Message.to_data(message, opts)

    {:ok, chan} = ConnectionProcess.request(conn_pid, "POST", path, headers, data)
    GRPC.Client.Stream.put_payload(stream, :stream_ref, chan)
  end

  # send headers to the server
  def send_headers(
    stream,
    opts
  ) do
    IO.puts("Send headers")
  end

  # send data
  def send_data(stream, message, opts) do
    IO.puts("Send data")
  end

  # finish stream
  def end_stream(stream) do
    IO.puts("end stream")
  end

  # cancel stream
  def cancel(%{conn_pid: conn_pid}, %{stream_ref: stream_ref}) do
    IO.puts("cancel")
  end

  # recv headers
  def recv_headers(%{conn_pid: conn_pid}, %{stream_ref: chan}, opts) do
    IO.puts("recv_headers")
    {:ok, status} = case Brex.Channel.pop(chan) do
      {:ok, {:status, status}} ->
        {:ok, status}
      error -> GRPC.RPCError.exception(GRPC.Status.internal(), "#{inspect(error)}")
    end

    if status == 200 do
      {:ok, {:headers, headers}} = Brex.Channel.pop(chan)
      headers = Enum.into(headers, %{})
      IO.inspect(headers)
      # fin or nofin?
      {:ok, headers, :fin}
    else
      {:error,
        GRPC.RPCError.exception(
          "unknown"
        )}
    end
  end

  def recv_data_or_trailers(%{conn_pid: conn_pid}, %{stream_ref: chan}, opts) do
    IO.puts("recv_data_or_trailers")

    case Brex.Channel.pop(chan) do
      {:ok, {:data, data}} ->
        IO.puts("____ 1")
        {:data, data} |> IO.inspect
      {:ok, {:headers, headers}} ->
        IO.puts("____ 2")
        headers = Enum.into(headers, %{})
        # fin or nofin?
        {:ok, {:done, _}} = Brex.Channel.pop(chan)
        {:trailers, headers} |> IO.inspect
    end

  end

end
